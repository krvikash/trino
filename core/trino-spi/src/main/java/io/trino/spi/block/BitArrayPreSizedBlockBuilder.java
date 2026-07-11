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
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Preconditions.checkArgument;

public class BitArrayPreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private static final Block NULL_VALUE_BLOCK = new BitArrayBlock(0, 1, new long[] {0}, new long[1]);

    // The bitmaps are allocated in whole 64-bit words, so unlike the array-backed builders the
    // backing storage does not fail fast when a caller writes past expectedEntries
    private final int capacity;
    // all bits set on allocation; values are all clear and positions are only ever written once
    private final long[] valueIsValid;
    private final long[] values;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    public BitArrayPreSizedBlockBuilder(int expectedEntries)
    {
        checkArgument(expectedEntries >= 0, "expectedEntries %s must be positive", expectedEntries);
        this.capacity = expectedEntries;
        this.valueIsValid = allocateWords(expectedEntries, true);
        this.values = allocateWords(expectedEntries, false);
    }

    @Override
    public void appendNull()
    {
        checkNotFull();
        clear(valueIsValid, 0, positionCount++);
        hasNullValue = true;
    }

    public void writeBoolean(boolean value)
    {
        checkNotFull();
        if (value) {
            set(values, 0, positionCount);
        }
        hasNonNullValue = true;
        positionCount++;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        checkNotFull();
        BitArrayBlock bitArrayBlock = (BitArrayBlock) block;
        if (bitArrayBlock.isNull(position)) {
            clear(valueIsValid, 0, positionCount);
            hasNullValue = true;
        }
        else {
            if (bitArrayBlock.getBoolean(position)) {
                set(values, 0, positionCount);
            }
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
        return new BitArrayBlock(0, positionCount, hasNullValue ? valueIsValid : null, values);
    }

    @Override
    public PreSizedBlockBuilder newBlockBuilderLike(int expectedEntries)
    {
        return new BitArrayPreSizedBlockBuilder(expectedEntries);
    }

    private void checkNotFull()
    {
        if (positionCount >= capacity) {
            throw new IllegalStateException("Block builder is full with " + capacity + " positions; cannot append more");
        }
    }
}
