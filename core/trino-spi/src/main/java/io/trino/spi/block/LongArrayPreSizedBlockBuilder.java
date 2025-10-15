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
package io.trino.spi.block;

import static io.trino.spi.block.PreSizedBlockBuilder.checkArgument;

public class LongArrayPreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private static final Block NULL_VALUE_BLOCK = new LongArrayBlock(0, 1, new boolean[] {true}, new long[1]);

    private final int expectedEntries;
    private final boolean[] isNull;
    private final long[] values;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    public LongArrayPreSizedBlockBuilder(int expectedEntries)
    {
        checkArgument(expectedEntries >= 0, "expectedEntries %s must be positive", expectedEntries);
        this.expectedEntries = expectedEntries;
        this.isNull = new boolean[expectedEntries];
        this.values = new long[expectedEntries];
    }

    @Override
    public void appendNull()
    {
        isNull[positionCount++] = true;
        hasNullValue = true;
    }

    public void writeLong(long value)
    {
        values[positionCount++] = value;
        hasNonNullValue = true;
    }

    @Override
    public Block build()
    {
        checkArgument(positionCount == expectedEntries, "Expected %s entries, but wrote %s", expectedEntries, positionCount);
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return new LongArrayBlock(0, positionCount, hasNullValue ? isNull : null, values);
    }
}
