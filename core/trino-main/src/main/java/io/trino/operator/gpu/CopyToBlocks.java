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
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.Column.Blocks;
import io.trino.operator.gpu.Column.DeviceMemory;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

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
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class CopyToBlocks
        implements GpuOperation
{
    private static final int BATCH_SIZE = 16;

    private final GpuOperation source;
    private final List<Type> types;
    private final int columnCount;

    public CopyToBlocks(GpuOperation source, List<Type> types)
    {
        this.source = requireNonNull(source, "source is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columnCount = types.size();
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
            checkArgument(inputPage.columnCount() == columnCount, "Page has wrong column count");
            int[] columnIndexToAppenderIndex = new int[inputPage.columnCount()];
            @Own List<BlockBuilderAppender> appenders = newArrayListWithExpectedSize(inputPage.columnCount());
            List<Type> copiedTypes = newArrayListWithExpectedSize(inputPage.columnCount());
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

                for (int columnIndex = 0; columnIndex < inputPage.columnCount(); columnIndex++) {
                    switch (inputPage.column(columnIndex)) {
                        case Blocks _ -> {
                            // Nothing to do here
                        }
                        case DeviceMemory deviceMemory -> {
                            columnIndexToAppenderIndex[columnIndex] = appenders.size();
                            Type type = types.get(columnIndex);
                            appenders.add(copyToBlocks(deviceMemory.columnVector(), type));
                            copiedTypes.add(type);
                        }
                    }
                }

                List<Page> copiedPages = new ArrayList<>();
                PageBuilder pageBuilder = new PageBuilder(inputPage.positionCount(), copiedTypes);

                // TODO (https://starburstdata.atlassian.net/browse/ENG-9841) the batch size probably should depend on width of the rows
                int positionOffset = 0;
                for (int batchNumber = 0; batchNumber < inputPage.positionCount() / BATCH_SIZE; batchNumber++) {
                    pageBuilder.declarePositions(BATCH_SIZE);
                    for (int i = 0; i < appenders.size(); i++) {
                        appenders.get(i).appendBatch(pageBuilder.getBlockBuilder(i), positionOffset);
                    }
                    positionOffset += BATCH_SIZE;
                    if (pageBuilder.isFull()) {
                        copiedPages.add(pageBuilder.build());
                        pageBuilder.reset();
                    }
                }
                while (positionOffset < inputPage.positionCount()) {
                    pageBuilder.declarePosition();
                    for (int i = 0; i < appenders.size(); i++) {
                        appenders.get(i).append(pageBuilder.getBlockBuilder(i), positionOffset);
                    }
                    positionOffset++;
                }
                if (!pageBuilder.isEmpty()) {
                    copiedPages.add(pageBuilder.build());
                }
                pageBuilder.reset();

                for (int columnIndex = 0; columnIndex < inputPage.columnCount(); columnIndex++) {
                    newColumns[columnIndex] = switch (inputPage.column(columnIndex)) {
                        case Blocks blocks -> blocks;
                        case DeviceMemory _ -> {
                            int copiedPagesColumnIndex = columnIndexToAppenderIndex[columnIndex];
                            yield new Blocks(
                                    copiedPages.stream()
                                            .map(page -> page.getBlock(copiedPagesColumnIndex))
                                            .collect(toImmutableList()));
                        }
                    };
                }
                return new GpuPage(inputPage.positionCount(), newColumns);
            }
            finally {
                try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
                    for (Column column : newColumns) {
                        if (column != null) {
                            // Technically, no close is needed because newColumns is fully heap stuff
                            closer.register(column);
                        }
                    }
                    appenders.forEach(closer::register);
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

    private @Move BlockBuilderAppender copyToBlocks(@Borrow ColumnVector columnVector, Type type)
    {
        if (type == BOOLEAN) {
            return new BooleanAppender(columnVector);
        }
        if (type == TINYINT) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == SMALLINT) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == INTEGER) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == BIGINT) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == REAL) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == DOUBLE) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof DecimalType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == NUMBER) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof CharType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof VarcharType varcharType) {
            return new VarcharAppender(columnVector, varcharType);
        }
        if (type instanceof VarbinaryType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == DATE) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimeType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimeWithTimeZoneType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimestampType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    @Override
    public void close()
    {
        source.close();
    }

    private static class BooleanAppender
            implements BlockBuilderAppender
    {
        private final @Own HostColumnVector hostColumnVector;

        public BooleanAppender(ColumnVector columnVector)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9841) use ColumnVector.copyToHostAsync(stream) to get parallel transfers for all columns being copied
            hostColumnVector = columnVector.copyToHost();
        }

        @Override
        public void appendBatch(BlockBuilder blockBuilder, int positionOffset)
        {
            for (int i = 0; i < BATCH_SIZE; i++) {
                append(blockBuilder, positionOffset + i);
            }
        }

        @Override
        public void append(BlockBuilder blockBuilder, int position)
        {
            if (hostColumnVector.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(blockBuilder, hostColumnVector.getBoolean(position));
            }
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private static class VarcharAppender
            implements BlockBuilderAppender
    {
        private final @Own HostColumnVector hostColumnVector;
        private final VarcharType varcharType;

        public VarcharAppender(ColumnVector columnVector, VarcharType varcharType)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9841) use ColumnVector.copyToHostAsync(stream) to get parallel transfers for all columns being copied
            this.hostColumnVector = columnVector.copyToHost();
            this.varcharType = requireNonNull(varcharType, "varcharType is null");
        }

        @Override
        public void appendBatch(BlockBuilder blockBuilder, int positionOffset)
        {
            for (int i = 0; i < BATCH_SIZE; i++) {
                append(blockBuilder, positionOffset + i);
            }
        }

        @Override
        public void append(BlockBuilder blockBuilder, int position)
        {
            if (hostColumnVector.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                // TODO (https://starburstdata.atlassian.net/browse/ENG-9841) avoid intermediate byte[]
                //  See how getUTF8 is implemented. We could maybe hostColumnVector.getData().getBytes(...) directly into a pre-resized builder byte[] array
                byte[] utf8 = hostColumnVector.getUTF8(position);
                varcharType.writeSlice(blockBuilder, wrappedBuffer(utf8));
            }
        }

        @Override
        public void close()
        {
            hostColumnVector.close();
        }
    }

    private interface BlockBuilderAppender
            extends RuntimeCloseable
    {
        /**
         * Append {@link #BATCH_SIZE} entries
         */
        void appendBatch(BlockBuilder blockBuilder, int positionOffset);

        /**
         * Append one entry.
         */
        void append(BlockBuilder blockBuilder, int position);
    }
}
