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

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostMemoryBuffer;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
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
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (memory; page) {
                    yield new Data(AllocatedMemory.untracked(), processPage(page));
                }
            }
        };
    }

    private @Move GpuPage processPage(@Borrow GpuPage inputPage)
    {
        try {
            checkArgument(inputPage.columnCount() == types.size(), "Page has wrong column count");
            List<ColumnCopier> copiers = newArrayListWithExpectedSize(inputPage.columnCount());
            @Own Column[] newColumns = new Column[inputPage.columnCount()];
            @Own List<HostColumnVector> hostColumnVectors = new ArrayList<>();
            boolean syncFailed = false;
            try {
                int positionCount = inputPage.positionCount();
                // Issue all device→host transfers, then synchronize once so host-side allocation
                // and bookkeeping overlap with in-flight DMA.
                try {
                    for (int columnIndex = 0; columnIndex < inputPage.columnCount(); columnIndex++) {
                        if (inputPage.column(columnIndex) instanceof DeviceMemory deviceMemory) {
                            hostColumnVectors.add(deviceMemory.columnVector().copyToHostAsync(Cuda.DEFAULT_STREAM));
                        }
                    }
                    Cuda.DEFAULT_STREAM.sync();
                }
                catch (RuntimeException e) {
                    // Sync before the finally-close to prevent freeing pinned host buffers that
                    // in-flight DMAs may still be writing into.
                    try {
                        Cuda.DEFAULT_STREAM.sync();
                    }
                    catch (RuntimeException syncException) {
                        syncFailed = true;
                        e.addSuppressed(syncException);
                    }
                    throw e;
                }

                int hostColumnVectorIndex = 0;
                for (int columnIndex = 0; columnIndex < inputPage.columnCount(); columnIndex++) {
                    Type type = types.get(columnIndex);
                    switch (inputPage.column(columnIndex)) {
                        case Blocks blocks -> copiers.add(createBlockCopier(blocks, type));
                        case DeviceMemory _ -> {
                            HostColumnVector hostColumnVector = hostColumnVectors.get(hostColumnVectorIndex++);
                            copiers.add(createColumnCopier(hostColumnVector, type));
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
                    int index = columnIndex;
                    newColumns[columnIndex] = new Blocks(copiedPages.stream()
                            .map(page -> page.getBlock(index))
                            .collect(toImmutableList()));
                }
                return new GpuPage(positionCount, newColumns);
            }
            finally {
                try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
                    // Technically, no close is needed because newColumns is fully heap stuff
                    closer.register(() -> closeColumns(newColumns));

                    // When the stream sync failed, host buffers may still be targets of in-flight
                    // DMAs and must be leaked rather than freed.
                    if (!syncFailed) {
                        hostColumnVectors.forEach(closer::register);
                    }
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

    private static ColumnCopier createBlockCopier(Blocks blocks, Type type)
    {
        return new ColumnCopier()
        {
            private final Iterator<Block> input = blocks.blocks().iterator();
            private int pastPositions;
            private Block currentBlock;
            private int currentBlockOffset;

            @Override
            public Block buildBlock(int position, int count)
            {
                checkArgument(pastPositions == position, "Unexpected position, expected %s, got %s", pastPositions, position);
                discardExhaustedInputBlock();

                if (count <= currentBlock.getPositionCount() - currentBlockOffset) {
                    Block region = currentBlock.getRegion(currentBlockOffset, count);
                    pastPositions += count;
                    currentBlockOffset += count;
                    return region;
                }

                BlockBuilder builder = type.createBlockBuilder(null, count);
                int remaining = count;
                while (remaining > 0) {
                    discardExhaustedInputBlock();
                    int batch = Math.min(remaining, currentBlock.getPositionCount() - currentBlockOffset);
                    builder.appendBlockRange(currentBlock, currentBlockOffset, batch);
                    remaining -= batch;
                    pastPositions += batch;
                    currentBlockOffset += batch;
                }
                return builder.build();
            }

            private void discardExhaustedInputBlock()
            {
                while (currentBlock == null || currentBlockOffset == currentBlock.getPositionCount()) {
                    currentBlock = input.next();
                    currentBlockOffset = 0;
                }
            }
        };
    }

    public static Block copyToBlock(@Borrow HostColumnVector hostColumnVector, Type type)
    {
        return createColumnCopier(hostColumnVector, type)
                .buildBlock(0, toIntExact(hostColumnVector.getRowCount()));
    }

    private static ColumnCopier createColumnCopier(@Borrow HostColumnVector hostColumnVector, Type type)
    {
        checkType(hostColumnVector.getType(), toDType(type).orElseThrow());
        return switch (type) {
            case BooleanType _ -> new ByteColumnCopier(hostColumnVector);
            case TinyintType _ -> new ByteColumnCopier(hostColumnVector);
            case SmallintType _ -> new ShortColumnCopier(hostColumnVector);
            case IntegerType _ -> new IntColumnCopier(hostColumnVector);
            case BigintType _ -> new LongColumnCopier(hostColumnVector);
            case RealType _ -> new RealColumnCopier(hostColumnVector);
            case DoubleType _ -> new DoubleColumnCopier(hostColumnVector);
            case DecimalType decimalType when decimalType.isShort() -> new LongColumnCopier(hostColumnVector);
            case DecimalType decimalType when !decimalType.isShort() -> new Int128ColumnCopier(hostColumnVector);
            case CharType _, VarcharType _ -> new VariableWidthBlockColumnCopier(hostColumnVector);
            case VarbinaryType _ -> {
                checkType(hostColumnVector.getType(), DType.LIST);
                checkArgument(hostColumnVector.getNumChildren() == 1, "Unexpected number of child vectors: %s", hostColumnVector.getNumChildren());
                checkType(hostColumnVector.getChildColumnView(0).getType(), DType.UINT8);
                yield new VarbinaryColumnCopier(hostColumnVector);
            }
            case DateType _ -> new IntColumnCopier(hostColumnVector);
            case TimestampType timestampType when timestampType.getPrecision() == 0 -> new RescaledLongColumnCopier(hostColumnVector, 1_000_000L);
            case TimestampType timestampType when timestampType.getPrecision() <= 3 -> new RescaledLongColumnCopier(hostColumnVector, 1_000L);
            case TimestampType timestampType when timestampType.getPrecision() <= 6 -> new LongColumnCopier(hostColumnVector);
            case TimestampType timestampType when timestampType.getPrecision() <= 9 -> new TimestampNanosCopier(hostColumnVector);
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
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
        private final @Borrow HostColumnVector hostColumnVector;

        private ByteColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            byte[] values = new byte[count];
            hostColumnVector.getData().getBytes(values, 0, position, count);
            return new ByteArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }
    }

    private static class ShortColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private ShortColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            short[] values = new short[count];
            ByteBuffer byteBuffer = hostColumnVector.getData().asByteBuffer((long) position * Short.BYTES, count * Short.BYTES);
            byteBuffer.asShortBuffer().get(values);
            return new ShortArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }
    }

    private static class IntColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private IntColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            int[] values = new int[count];
            hostColumnVector.getData().getInts(values, 0, (long) position * Integer.BYTES, count);
            return new IntArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }
    }

    private static class LongColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private LongColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] values = new long[count];
            hostColumnVector.getData().getLongs(values, 0, (long) position * Long.BYTES, count);
            return new LongArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }
    }

    private static class RescaledLongColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;
        private final long multiplier;

        private RescaledLongColumnCopier(@Borrow HostColumnVector hostColumnVector, long multiplier)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
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
    }

    private static class RealColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private RealColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            int[] values = new int[count];
            hostColumnVector.getData().getInts(values, 0, (long) position * Float.BYTES, count);
            return new IntArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }
    }

    private static class DoubleColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private DoubleColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] values = new long[count];
            hostColumnVector.getData().getLongs(values, 0, (long) position * Double.BYTES, count);
            return new LongArrayBlock(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
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
        private final @Borrow HostColumnVector hostColumnVector;

        private Int128ColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
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
    }

    private static class VariableWidthBlockColumnCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private VariableWidthBlockColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
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
        private final @Borrow HostColumnVector hostColumnVector;

        private VarbinaryColumnCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
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
    }

    private static class TimestampNanosCopier
            implements ColumnCopier
    {
        private final @Borrow HostColumnVector hostColumnVector;

        private TimestampNanosCopier(@Borrow HostColumnVector hostColumnVector)
        {
            this.hostColumnVector = requireNonNull(hostColumnVector, "hostColumnVector is null");
        }

        @Override
        public Block buildBlock(int position, int count)
        {
            long[] nanos = new long[count];
            hostColumnVector.getData().getLongs(nanos, 0, (long) position * Long.BYTES, count);
            int[] values = new int[count * 3];
            for (int i = 0; i < count; i++) {
                long timestampNanos = nanos[i];
                long epochMicros = floorDiv(timestampNanos, NANOSECONDS_PER_MICROSECOND);
                int picosOfMicro = floorMod(timestampNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                Fixed12Block.encodeFixed12(epochMicros, picosOfMicro, values, i);
            }
            return new Fixed12Block(count, validityToNulls(hostColumnVector.getValidity(), position, count), values);
        }
    }

    private interface ColumnCopier
    {
        Block buildBlock(int position, int count);
    }

    private static void checkType(DType type, DType expected)
    {
        checkArgument(expected.equals(type), "Unexpected DType: expected %s, got %s", expected, type);
    }
}
