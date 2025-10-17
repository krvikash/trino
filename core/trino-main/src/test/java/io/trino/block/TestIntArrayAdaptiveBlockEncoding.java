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
package io.trino.block;

import io.airlift.slice.DynamicSliceOutput;
import io.trino.FeaturesConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.block.BaseBlockEncodingTest;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestIntArrayAdaptiveBlockEncoding
        extends BaseBlockEncodingTest<Integer>
{
    @Override
    protected BlockEncodingSerde createBlockEncodingSerde()
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(new FeaturesConfig());
        assertThat(blockEncodingManager.getBlockEncodingByBlockClass(IntArrayBlock.class))
                .isInstanceOf(IntArrayAdaptiveBlockEncoding.class);
        return new InternalBlockEncodingSerde(blockEncodingManager, TESTING_TYPE_MANAGER);
    }

    @Override
    protected Type getType()
    {
        return INTEGER;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Integer value)
    {
        INTEGER.writeInt(blockBuilder, value);
    }

    @Override
    protected Integer randomValue(Random random)
    {
        return random.nextInt();
    }

    @Test
    public void testNegativeAscending()
    {
        Object[] values = new Integer[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = Integer.MIN_VALUE + i;
        }
        Block block = createBlockWithOffset(values);

        int adaptiveSize = roundTripAndGetSize(block);
        // Negative values are 32 bits wide, but since the array is sorted,
        // we can use delta encoding combined with bitpacking.
        int rawSize = encodeAndGetSize(createRawSerde(), block);
        assertThat(adaptiveSize).isLessThan(rawSize);
    }

    @Test
    public void testNegativeRandom()
    {
        Object[] values = generateValuesWithBitWidth(1000, 32);
        Block block = createBlockWithOffset(values);

        int adaptiveSize = roundTripAndGetSize(block);
        // This is the worst case, as all values have full bit width, so raw encoding is selected.
        int rawSize = encodeAndGetSize(createRawSerde(), block);
        assertThat(adaptiveSize).isEqualTo(rawSize - 2); // -2 to account for the difference in encoding names
    }

    @Test
    public void testPositiveRandom()
    {
        for (int width = 0; width <= 31; width++) {
            Object[] values = generateValuesWithBitWidth(1000, width);
            Block block = createBlockWithOffset(values);

            int adaptiveSize = roundTripAndGetSize(block);
            int rawSize = encodeAndGetSize(createRawSerde(), block);
            assertThat(adaptiveSize).isLessThan(rawSize);
        }
    }

    @Test
    public void testRle()
    {
        Random random = new Random(123);
        int runCount = 100;
        int maxRunLength = 256;
        Object[] values = IntStream.range(0, runCount)
                .mapToObj(runIndex -> {
                    if (runIndex == 0) {
                        return Integer.MAX_VALUE;
                    }
                    if (runIndex == runCount - 1) {
                        return Integer.MIN_VALUE;
                    }
                    return random.nextInt();
                })
                .flatMap(runValue -> Collections.nCopies(1 + random.nextInt(maxRunLength), runValue).stream())
                .toArray();
        Block block = createBlockWithOffset(values);
        int adaptiveSize = roundTripAndGetSize(block);
        int rawSize = encodeAndGetSize(createRawSerde(), block);
        assertThat(adaptiveSize).isLessThan(rawSize);
    }

    @Test
    public void testVByte()
    {
        Object[] values = generateValuesWithBitWidth(127, 8);
        Block block = createBlockWithOffset(values);

        int adaptiveSize = roundTripAndGetSize(block);
        int adaptiveWithoutVByteSize = encodeAndGetSize(createAdaptiveWithoutVByteSerde(), block);
        assertThat(adaptiveSize).isLessThan(adaptiveWithoutVByteSize);
    }

    private static Object[] generateValuesWithBitWidth(int length, int bitWidth)
    {
        checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be in range 0..32");

        Random random = new Random(3571);
        Object[] values = new Integer[length];

        for (int i = 0; i < values.length; i++) {
            if (bitWidth == 0) {
                values[i] = 0;
            }
            else if (bitWidth == 1) {
                values[i] = 1;
            }
            else {
                int maxValue = (1 << (bitWidth - 1)) - 1;
                int mostSignificantBit = 1 << (bitWidth - 1);
                int value = random.nextInt(maxValue);
                values[i] = mostSignificantBit | value;
            }
        }

        return values;
    }

    private int roundTripAndGetSize(Block block)
    {
        BlockEncodingSerde serde = createBlockEncodingSerde();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
        serde.writeBlock(sliceOutput, block);
        Block actualBlock = serde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(getType(), actualBlock, block);
        return sliceOutput.size();
    }

    private int encodeAndGetSize(BlockEncodingSerde serde, Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
        serde.writeBlock(sliceOutput, block);
        return sliceOutput.size();
    }

    private Block createBlockWithOffset(Object... values)
    {
        BlockBuilder blockBuilder = getType().createBlockBuilder(null, values.length);

        int offset = 3; // this is to verify that offset is respected
        blockBuilder.appendRepeated(new IntArrayBlock(1, Optional.empty(), new int[] {0}), 0, offset);

        for (Object value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                write(blockBuilder, (Integer) value);
            }
        }

        return blockBuilder.build().getRegion(offset, values.length);
    }

    private static BlockEncodingSerde createRawSerde()
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(
                new FeaturesConfig()
                        .setExchangeVbyteBlockEncodingEnabled(false)
                        .setExchangeAdaptiveBlockEncodingEnabled(false));
        return new InternalBlockEncodingSerde(blockEncodingManager, TESTING_TYPE_MANAGER);
    }

    private static BlockEncodingSerde createAdaptiveWithoutVByteSerde()
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(
                new FeaturesConfig()
                        .setExchangeVbyteBlockEncodingEnabled(false)
                        .setExchangeAdaptiveBlockEncodingEnabled(true));
        return new InternalBlockEncodingSerde(blockEncodingManager, TESTING_TYPE_MANAGER);
    }
}
