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

import static io.trino.spi.block.Fixed12Block.encodeFixed12;
import static io.trino.spi.block.PreSizedBlockBuilder.checkArgument;

public class Fixed12PreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private static final Block NULL_VALUE_BLOCK = new Fixed12Block(0, 1, new boolean[] {true}, new int[3]);

    private final boolean[] isNull;
    private final int[] values;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    public Fixed12PreSizedBlockBuilder(int expectedEntries)
    {
        checkArgument(expectedEntries >= 0, "expectedEntries %s must be positive", expectedEntries);
        this.isNull = new boolean[expectedEntries];
        this.values = new int[expectedEntries * 3];
    }

    @Override
    public void appendNull()
    {
        isNull[positionCount++] = true;
        hasNullValue = true;
    }

    public void writeFixed12(long first, int second)
    {
        encodeFixed12(first, second, values, positionCount);

        hasNonNullValue = true;
        positionCount++;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        Fixed12Block fixed12Block = (Fixed12Block) block;
        if (fixed12Block.isNull(position)) {
            isNull[positionCount] = true;
            hasNullValue = true;
        }
        else {
            int[] rawValues = fixed12Block.getRawValues();
            int rawValuePosition = (fixed12Block.getRawOffset() + position) * 3;

            int positionIndex = positionCount * 3;
            values[positionIndex] = rawValues[rawValuePosition];
            values[positionIndex + 1] = rawValues[rawValuePosition + 1];
            values[positionIndex + 2] = rawValues[rawValuePosition + 2];
            hasNonNullValue = true;
        }
        positionCount++;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return new Fixed12Block(0, positionCount, hasNullValue ? isNull : null, values);
    }
}
