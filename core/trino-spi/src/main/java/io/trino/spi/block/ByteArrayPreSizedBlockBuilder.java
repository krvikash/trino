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

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Preconditions.checkArgument;

public class ByteArrayPreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private static final Block NULL_VALUE_BLOCK = new ByteArrayBlock(0, 1, new long[] {0}, new byte[1]);

    private final long[] valueIsValid;
    private final byte[] values;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    public ByteArrayPreSizedBlockBuilder(int expectedEntries)
    {
        checkArgument(expectedEntries >= 0, "expectedEntries %s must be positive", expectedEntries);
        this.valueIsValid = allocateWords(expectedEntries, true);
        this.values = new byte[expectedEntries];
    }

    @Override
    public void appendNull()
    {
        clear(valueIsValid, 0, positionCount++);
        hasNullValue = true;
    }

    public void writeByte(byte value)
    {
        values[positionCount++] = value;
        hasNonNullValue = true;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        ByteArrayBlock byteArrayBlock = (ByteArrayBlock) block;
        if (byteArrayBlock.isNull(position)) {
            clear(valueIsValid, 0, positionCount);
            hasNullValue = true;
        }
        else {
            values[positionCount] = byteArrayBlock.getByte(position);
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
        return new ByteArrayBlock(0, positionCount, hasNullValue ? valueIsValid : null, values);
    }

    @Override
    public PreSizedBlockBuilder newBlockBuilderLike(int expectedEntries)
    {
        return new ByteArrayPreSizedBlockBuilder(expectedEntries);
    }
}
