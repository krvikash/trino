/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.gpu;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostMemoryBuffer;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class CopyToBlocks
        implements GpuOperation
{
    // The entire GPU Page is materialized to host memory regardless of target page size, so this
    // does not affect peak memory — only output page granularity. Larger pages amortize allocation
    // and per-page overhead, so we use 8 MB instead of the PageBuilder default of 1 MB.
    private static final int MAX_PAGE_SIZE_IN_BYTES = 8 * 1024 * 1024;
    private static final int INITIAL_BATCH_SIZE = 16;
    private static final int MAX_POSITIONS_PER_PAGE = 128 * 1024;
    private static final int CANONICAL_NAN_FLOAT_BITS = Float.floatToIntBits(Float.NaN);
    private static final long CANONICAL_NAN_DOUBLE_BITS = Double.doubleToLongBits(Double.NaN);

    private final GpuOperation source;
    private final List<Type> types;

    public CopyToBlocks(GpuOperation source, List<Type> types)
    {
        this.source = requireNonNull(source, "source is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    @Override
    public @Move Result execute()
    {
        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Finished finished -> finished;
            case Yielded yielded -> yielded;
            case Data(GpuPage page) -> {
                try (page) {
                    yield new Data(processPage(page));
                }
            }
        };
    }

    private @Move GpuPage processPage(@Borrow GpuPage inputPage)
    {
        try {
            checkArgument(inputPage.columnCount() == types.size(), "Page has wrong column count");
            int[] columnIndexToCopierIndex = new int[inputPage.columnCount()];
            @Own List<ColumnCopier> copiers = newArrayListWithExpectedSize(inputPage.columnCount());
            @Own Column[] newColumns = new Column[inputPage.columnCount()];
            try {
                Optional<List<Integer>> desiredBlockPositions = IntStream.range(0, inputPage.columnCount())
                        .mapToObj(columnIndex -> switch (inputPage.column(columnIndex)) {
                            case Blocks blocks -> Optional.of(blocks);
                            case DeviceMemory _ -> Optional.<Blocks>empty();
                        })
                        .flatMap(Optional::stream)
                        .map(blocks -> blocks.blocks().stream()
                                .map(Block::getPositionCount)
                                .collect(toImmutableList()))
                        .distinct()
                        .collect(toOptional());
                // TODO (https://starburstdata.atlassian.net/browse/ENG-9808) if there are any pre-existing blocks, we need to honor their alignment or rewrite them
                checkState(desiredBlockPositions.isEmpty(), "Pre-existing blocks");

                int positionCount = inputPage.positionCount();
                for (int columnIndex = 0; columnIndex < inputPage.columnCount(); columnIndex++) {
                    switch (inputPage.column(columnIndex)) {
                        case Blocks _ -> {}
                        case DeviceMemory deviceMemory -> {
                            columnIndexToCopierIndex[columnIndex] = copiers.size();
                            Type type = types.get(columnIndex);
                            copiers.add(createColumnCopier(deviceMemory.columnVector(), type));
                        }
                    }
                }

                List<Page> copiedPages = new ArrayList<>();
                int targetBatchSize = INITIAL_BATCH_SIZE;
                int offset = 0;
                double maxAvgBytesPerPosition = 0;

                // Growth heuristic mirrors PreSizedPageBuilder: start small, double until
                // avg observed bytes/position × batchSize approaches MAX_PAGE_SIZE_IN_BYTES.
                while (offset < positionCount) {
                    int batchSize = min(targetBatchSize, positionCount - offset);

                    Block[] blocks = new Block[copiers.size()];
                    for (int i = 0; i < copiers.size(); i++) {
                        blocks[i] = copiers.get(i).buildBlock(offset, batchSize);
                    }
                    Page page = new Page(batchSize, blocks);
                    copiedPages.add(page);

                    double avg = (double) page.getSizeInBytes() / batchSize;
                    maxAvgBytesPerPosition = max(maxAvgBytesPerPosition, avg);
                    long byteBudget = (long) (MAX_PAGE_SIZE_IN_BYTES / maxAvgBytesPerPosition);
                    targetBatchSize = Math.clamp(Math.min(2L * batchSize, byteBudget), INITIAL_BATCH_SIZE, MAX_POSITIONS_PER_PAGE);
                    offset += batchSize;
                }

                for (int columnIndex = 0; columnIndex < inputPage.columnCount(); columnIndex++) {
                    newColumns[columnIndex] = switch (inputPage.column(columnIndex)) {
                        case Blocks blocks -> blocks;
                        case DeviceMemory _ -> {
                            int copiedPagesColumnIndex = columnIndexToCopierIndex[columnIndex];
                            yield new Blocks(
                                    copiedPages.stream()
                                            .map(page -> page.getBlock(copiedPagesColumnIndex))
                                            .collect(toImmutableList()));
                        }
                    };
                }
                return new GpuPage(positionCount, newColumns);
            }
            finally {
                try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
                    for (Column column : newColumns) {
                        if (column != null) {
                            // Technically, no close is needed because newColumns is fully heap stuff
                            closer.register(column);
                        }
                    }
                    copiers.forEach(closer::register);
                }
            }
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private @Move ColumnCopier createColumnCopier(@Borrow ColumnVector columnVector, Type type)
    {
        if (type == BOOLEAN || type == TINYINT) {
            return new ByteColumnCopier(columnVector);
        }
        if (type == SMALLINT) {
            return new ShortColumnCopier(columnVector);
        }
        if (type == INTEGER || type == DATE) {
            return new IntColumnCopier(columnVector);
        }
        if (type == BIGINT) {
            return new LongColumnCopier(columnVector);
        }
        if (type instanceof DecimalType decimalType) {
            return decimalType.isShort()
                    ? new LongColumnCopier(columnVector)
                    : new Int128ColumnCopier(columnVector);
        }
        if (type instanceof TimestampType timestampType) {
            return switch (timestampType.getPrecision()) {
                case 0 -> new RescaledLongColumnCopier(columnVector, 1_000_000L);
                case 3 -> new RescaledLongColumnCopier(columnVector, 1_000L);
                case 6 -> new LongColumnCopier(columnVector);
                default -> throw new UnsupportedOperationException("Unsupported type: " + type);
            };
        }
        if (type == REAL) {
            return new RealColumnCopier(columnVector);
        }
        if (type == DOUBLE) {
            return new DoubleColumnCopier(columnVector);
        }
        if (type instanceof VarcharType) {
            return new VarcharColumnCopier(columnVector);
        }
        if (type instanceof VarbinaryType) {
            return new VarbinaryColumnCopier(columnVector);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    @Override
    public void close()
    {
        source.close();
    }

    /**
     * Unpack an Arrow-style least-significant-bit-first validity bitmask (bit 0 of each byte
     * corresponds to the first row within that byte) covering positions
     * {@code [position, position + count)} into a per-row {@code boolean[]}
     * where {@code true} means null, matching Trino block nulls convention.
     */
    private static Optional<boolean[]> validityToNulls(@Nullable @Borrow HostMemoryBuffer validityBuf, int position, int count)
    {
        if (validityBuf == null) {
            return Optional.empty();
        }
        int startByte = position >> 3;
        int endByte = (position + count - 1) >> 3;
        int byteCount = endByte - startByte + 1;
        byte[] validityBytes = new byte[byteCount];
        validityBuf.getBytes(validityBytes, 0, startByte, byteCount);

        boolean[] valueIsNull = new boolean[count];
        for (int i = 0; i < count; i++) {
            int bitIndex = position + i;
            valueIsNull[i] = (validityBytes[(bitIndex >> 3) - startByte] & (1 << (bitIndex & 7))) == 0;
        }
        return Optional.of(valueIsNull);
    }

    private static class ByteColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public ByteColumnCopier(ColumnVector columnVector)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9841) use ColumnVector.copyToHostAsync(stream) to get parallel transfers for all columns being copied
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            byte[] values = new byte[count];
            hostColumnVector.getData().getBytes(values, 0, position, count);
            return new ByteArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class ShortColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public ShortColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            short[] values = new short[count];
            ByteBuffer byteBuffer = hostColumnVector.getData().asByteBuffer((long) position * Short.BYTES, count * Short.BYTES);
            byteBuffer.asShortBuffer().get(values);
            return new ShortArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class IntColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public IntColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            int[] values = new int[count];
            hostColumnVector.getData().getInts(values, 0, (long) position * Integer.BYTES, count);
            return new IntArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class LongColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public LongColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] values = new long[count];
            hostColumnVector.getData().getLongs(values, 0, (long) position * Long.BYTES, count);
            return new LongArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class RescaledLongColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;
        private final long multiplier;

        public RescaledLongColumnCopier(ColumnVector columnVector, long multiplier)
        {
            this.hostColumnVector = columnVector.copyToHost();
            this.multiplier = multiplier;
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] values = new long[count];
            hostColumnVector.getData().getLongs(values, 0, (long) position * Long.BYTES, count);
            for (int i = 0; i < count; i++) {
                values[i] = Math.multiplyExact(values[i], multiplier);
            }
            return new LongArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class RealColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public RealColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            int[] values = new int[count];
            hostColumnVector.getData().getInts(values, 0, (long) position * Float.BYTES, count);
            // cuDF preserves raw NaN bits; Trino expects the canonical NaN
            for (int i = 0; i < count; i++) {
                if (Float.isNaN(intBitsToFloat(values[i]))) {
                    values[i] = CANONICAL_NAN_FLOAT_BITS;
                }
            }
            return new IntArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class DoubleColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public DoubleColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] values = new long[count];
            hostColumnVector.getData().getLongs(values, 0, (long) position * Double.BYTES, count);
            // cuDF preserves raw NaN bits; Trino expects the canonical NaN
            for (int i = 0; i < count; i++) {
                if (Double.isNaN(longBitsToDouble(values[i]))) {
                    values[i] = CANONICAL_NAN_DOUBLE_BITS;
                }
            }
            return new LongArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    /**
     * cuDF DECIMAL128 stores 16 bytes per position in little-endian order: bytes 0-7 are the low
     * 64 bits, bytes 8-15 are the high 64 bits. {@link Int128ArrayBlock} stores values as
     * {@code (high, low)} long pairs, so we swap each pair on the way back.
     */
    private static class Int128ColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public Int128ColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] cudfLowHighPairs = new long[count * 2];
            hostColumnVector.getData().getLongs(cudfLowHighPairs, 0, (long) position * Int128ArrayBlock.INT128_BYTES, count * 2);
            long[] trinoHighLowPairs = new long[count * 2];
            for (int i = 0; i < count; i++) {
                trinoHighLowPairs[2 * i] = cudfLowHighPairs[2 * i + 1];
                trinoHighLowPairs[2 * i + 1] = cudfLowHighPairs[2 * i];
            }
            return new Int128ArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), trinoHighLowPairs);
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class VarcharColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public VarcharColumnCopier(ColumnVector columnVector)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9841) use ColumnVector.copyToHostAsync(stream) to get parallel transfers for all columns being copied
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            int[] offsets = new int[count + 1];
            hostColumnVector.getOffsets().getInts(offsets, 0, (long) position * Integer.BYTES, count + 1);
            int dataStart = offsets[0];
            offsets[0] = 0;
            for (int i = 1; i <= count; i++) {
                offsets[i] -= dataStart;
            }
            int dataLength = offsets[count];

            byte[] bytes = new byte[dataLength];
            if (dataLength > 0) {
                hostColumnVector.getData().getBytes(bytes, 0, dataStart, dataLength);
            }

            return new VariableWidthBlock(
                    count,
                    wrappedBuffer(bytes),
                    offsets,
                    validityToNulls(hostColumnVector.getValidity(), position, count));
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    /**
     * cuDF VARBINARY columns are LIST<INT8>: the parent carries offsets and validity, while the
     * child INT8 column carries the byte data. {@code copyToHost} preserves this structure, so
     * we reach the data buffer through {@link HostColumnVector#getChildColumnView(int)}.
     *
     * <p>Null list elements have offsets[i+1] == offsets[i] in cuDF, so the per-row length is 0
     * and no bytes are read for nulls; the validity bitmap is the source of truth.
     */
    private static class VarbinaryColumnCopier
            implements ColumnCopier
    {
        private final @Own HostColumnVector hostColumnVector;

        public VarbinaryColumnCopier(ColumnVector columnVector)
        {
            this.hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            int[] offsets = new int[count + 1];
            hostColumnVector.getOffsets().getInts(offsets, 0, (long) position * Integer.BYTES, count + 1);
            int dataStart = offsets[0];
            offsets[0] = 0;
            for (int i = 1; i <= count; i++) {
                offsets[i] -= dataStart;
            }
            int dataLength = offsets[count];

            byte[] bytes = new byte[dataLength];
            if (dataLength > 0) {
                hostColumnVector.getChildColumnView(0).getData().getBytes(bytes, 0, dataStart, dataLength);
            }

            return new VariableWidthBlock(
                    count,
                    wrappedBuffer(bytes),
                    offsets,
                    validityToNulls(hostColumnVector.getValidity(), position, count));
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private interface ColumnCopier
            extends RuntimeCloseable
    {
        Block buildBlock(int position, int count);
    }
}
