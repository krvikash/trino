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
package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInternalBlockEncodingSerde
{
    private final TestingTypeManager testingTypeManager = new TestingTypeManager();
    private final Map<String, BlockEncoding> blockEncodings = ImmutableMap.of(VariableWidthBlockEncoding.NAME, new VariableWidthBlockEncoding());
    private final Map<Class<? extends Block>, BlockEncoding> blockNames = ImmutableMap.of(VariableWidthBlock.class, new VariableWidthBlockEncoding());
    private final BlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodings::get, (block, _) -> blockNames.get(block), testingTypeManager::getType);

    @Test
    public void blockRoundTrip()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 2);
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("hello"));
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("world"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, blockBuilder.build());
        Block copy = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertThat(VARCHAR.getSlice(copy, 0).toStringUtf8()).isEqualTo("hello");
        assertThat(VARCHAR.getSlice(copy, 1).toStringUtf8()).isEqualTo("world");
    }

    @Test
    public void testTypeRoundTrip()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeType(sliceOutput, BOOLEAN);
        Type actualType = blockEncodingSerde.readType(sliceOutput.slice().getInput());
        assertThat(actualType).isEqualTo(BOOLEAN);
    }

    private static class TestBlockEncodingBase
            implements BlockEncoding
    {
        private final String name;
        private final long writtenValue;

        private TestBlockEncodingBase(String name, long writtenValue)
        {
            this.name = name;
            this.writtenValue = writtenValue;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public Class<? extends Block> getBlockClass()
        {
            return LongArrayBlock.class;
        }

        @Override
        public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
        {
            return new LongArrayBlockEncoding().readBlock(blockEncodingSerde, input);
        }

        @Override
        public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
        {
            // ignore value from parameter and alway write single element block with writtenValue we get from constructor
            LongArrayBlock writtenBlock = new LongArrayBlock(
                    1,
                    Optional.empty(),
                    new long[] {writtenValue});
            new LongArrayBlockEncoding().writeBlock(blockEncodingSerde, sliceOutput, writtenBlock);
        }
    }

    private static class XEncoding
            extends TestBlockEncodingBase
    {
        private XEncoding()
        {
            super("X", 1);
        }
    }

    private static class YEncoding
            extends TestBlockEncodingBase
    {
        private YEncoding()
        {
            super("Y", 2);
        }
    }

    private static class ZEncoding
            extends TestBlockEncodingBase
    {
        private ZEncoding()
        {
            super("Z", 3);
        }
    }

    @Test
    void testPerTypeBlockEncoding()
    {
        InternalBlockEncodingSerde serde = new InternalBlockEncodingSerde(
                name -> switch (name) {
                    case "X" -> new XEncoding();
                    case "Y" -> new YEncoding();
                    case "Z" -> new ZEncoding();
                    default -> throw new IllegalArgumentException("Unknown encoding: " + name);
                },
                (blockClass, type) -> {
                    if (blockClass == LongArrayBlock.class && type.isPresent() && type.get().equals(BIGINT)) {
                        return new XEncoding();
                    }
                    if (blockClass == LongArrayBlock.class && type.isPresent() && type.get().equals(DOUBLE)) {
                        return new YEncoding();
                    }
                    if (blockClass == LongArrayBlock.class) {
                        return new ZEncoding();
                    }
                    throw new IllegalArgumentException("No entry for " + blockClass + " and " + type);
                },
                testingTypeManager::getType);

        LongArrayBlock dummyBlock = new LongArrayBlock(1, Optional.empty(), new long[] {1});

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);

        serde.writeBlock(sliceOutput, dummyBlock, Optional.of(BIGINT));
        LongArrayBlock readBlock = (LongArrayBlock) serde.readBlock(sliceOutput.slice().getInput());
        assertThat(readBlock.getPositionCount()).isEqualTo(1);
        assertThat(readBlock.getLong(0)).isEqualTo(1L);
        sliceOutput.reset();

        serde.writeBlock(sliceOutput, dummyBlock, Optional.of(DOUBLE));
        readBlock = (LongArrayBlock) serde.readBlock(sliceOutput.slice().getInput());
        assertThat(readBlock.getPositionCount()).isEqualTo(1);
        assertThat(readBlock.getLong(0)).isEqualTo(2L);
        sliceOutput.reset();

        serde.writeBlock(sliceOutput, dummyBlock, Optional.of(DATE));
        readBlock = (LongArrayBlock) serde.readBlock(sliceOutput.slice().getInput());
        assertThat(readBlock.getPositionCount()).isEqualTo(1);
        assertThat(readBlock.getLong(0)).isEqualTo(3L);
        sliceOutput.reset();

        serde.writeBlock(sliceOutput, dummyBlock, Optional.empty());
        readBlock = (LongArrayBlock) serde.readBlock(sliceOutput.slice().getInput());
        assertThat(readBlock.getPositionCount()).isEqualTo(1);
        assertThat(readBlock.getLong(0)).isEqualTo(3L);
        sliceOutput.reset();
    }
}
