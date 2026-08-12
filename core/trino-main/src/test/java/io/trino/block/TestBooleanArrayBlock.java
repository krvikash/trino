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

import io.trino.spi.block.Block;
import io.trino.spi.block.BooleanArrayBlock;
import io.trino.spi.block.ShortArrayBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBooleanArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Boolean[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
    }

    @Test
    public void testCopyPositions()
    {
        Boolean[] expectedValues = createTestValue(17);
        Block block = createBlock(expectedValues);
        assertBlockFilteredPositions(expectedValues, block, 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Block block = createBlock(createTestValue(100));
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(Byte.BYTES);
        }

        assertThat(new ShortArrayBlockBuilder(null, 22).appendNull().build().getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    @Test
    public void testCompactBlock()
    {
        boolean[] booleanArray = {false, true, false, false, false, false};
        long[] packedValues = packBooleans(booleanArray);

        testCompactBlock(new BooleanArrayBlock(0, Optional.empty(), new long[0]));
        testCompactBlock(new BooleanArrayBlock(booleanArray.length, Optional.empty(), packedValues));
        // one word more than the position count requires
        testNotCompactBlock(new BooleanArrayBlock(booleanArray.length, Optional.empty(), Arrays.copyOf(packedValues, packedValues.length + 1)));
    }

    private void assertFixedWithValues(Boolean[] expectedValues)
    {
        assertBlock(createBlock(expectedValues), expectedValues);
    }

    private static Boolean[] createTestValue(int positionCount)
    {
        Boolean[] expectedValues = new Boolean[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = random.nextBoolean();
        }
        return expectedValues;
    }

    private static BooleanArrayBlock createBlock(Boolean[] values)
    {
        boolean[] booleanValues = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            booleanValues[i] = values[i];
        }
        return new BooleanArrayBlock(booleanValues.length, Optional.empty(), packBooleans(booleanValues));
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        assertThat(block.isNull(position)).isFalse();
        assertThat(((BooleanArrayBlock) block).getBoolean(position)).isEqualTo(expectedValue);
    }

    private static long[] packBooleans(boolean[] values)
    {
        long[] rawWords = new long[wordsForBits(values.length)];
        for (int position = 0; position < values.length; position++) {
            if (values[position]) {
                set(rawWords, 0, position);
            }
        }
        return rawWords;
    }
}
