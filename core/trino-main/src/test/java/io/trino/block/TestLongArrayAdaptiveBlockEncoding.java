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
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestLongArrayAdaptiveBlockEncoding
        extends BaseBlockEncodingTest<Long>
{
    @Override
    protected BlockEncodingSerde createBlockEncodingSerde()
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(new FeaturesConfig());
        assertThat(blockEncodingManager.getBlockEncodingByBlockClass(LongArrayBlock.class))
                .isInstanceOf(LongArrayAdaptiveBlockEncoding.class);
        return new InternalBlockEncodingSerde(blockEncodingManager, TESTING_TYPE_MANAGER);
    }

    @Override
    protected Type getType()
    {
        return BIGINT;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Long value)
    {
        BIGINT.writeLong(blockBuilder, value);
    }

    @Override
    protected Long randomValue(Random random)
    {
        return random.nextLong();
    }

    @Test
    public void testNegativeAscending()
    {
        Object[] values = new Long[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = Long.MIN_VALUE + i;
        }
        Block block = createBlockWithOffset(values);

        int adaptiveSize = roundTripAndGetSize(block);
        // Negative values are 64 bits wide, but since the array is sorted,
        // we can use delta encoding combined with bitpacking.
        int rawSize = encodeAndGetSize(createRawSerde(), block);
        assertThat(adaptiveSize).isLessThan(rawSize);
    }

    @Test
    public void testNegativeRandom()
    {
        Object[] values = generateValuesWithBitWidth(1000, 64);
        Block block = createBlockWithOffset(values);

        int adaptiveSize = roundTripAndGetSize(block);
        // This is the worst case, as all values have full bit width, so raw encoding is selected.
        int rawSize = encodeAndGetSize(createRawSerde(), block);
        assertThat(adaptiveSize).isEqualTo(rawSize - 2); // -2 to account for the difference in encoding names
    }

    @Test
    public void testPositiveRandom()
    {
        for (int width = 0; width <= 63; width++) {
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
                        return Long.MAX_VALUE;
                    }
                    if (runIndex == runCount - 1) {
                        return Long.MIN_VALUE;
                    }
                    return random.nextLong();
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
        checkArgument(bitWidth >= 0 && bitWidth <= 64, "bitWidth must be in range 0..64");

        Random random = new Random(3571);
        Object[] values = new Long[length];

        for (int i = 0; i < values.length; i++) {
            if (bitWidth == 0) {
                values[i] = 0L;
            }
            else if (bitWidth == 1) {
                values[i] = 1L;
            }
            else {
                long maxValue = (1L << (bitWidth - 1)) - 1;
                long mostSignificantBit = 1L << (bitWidth - 1);
                long value = random.nextLong(maxValue);
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
        blockBuilder.appendRepeated(new LongArrayBlock(1, Optional.empty(), new long[] {0}), 0, offset);

        for (Object value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                write(blockBuilder, (Long) value);
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
