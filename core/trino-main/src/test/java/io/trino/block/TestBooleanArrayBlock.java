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

import java.util.Random;

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

        testCompactBlock(new BooleanArrayBlock(0, 0, new boolean[0]));
        testCompactBlock(new BooleanArrayBlock(0, booleanArray.length, booleanArray));
        testNotCompactBlock(new BooleanArrayBlock(0, booleanArray.length - 1, booleanArray));
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
        return new BooleanArrayBlock(0, values.length, booleanValues);
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
}
