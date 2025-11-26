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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.block.PreSizedBlockBuilder.checkArgument;
import static java.lang.Math.min;

public class VariableWidthPreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true});

    private final boolean[] isNull;
    private final int[] offsets;

    private byte[] bytes;
    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    public VariableWidthPreSizedBlockBuilder(int expectedEntries)
    {
        this(expectedEntries, expectedEntries * 8);
    }

    private VariableWidthPreSizedBlockBuilder(int expectedEntries, int expectedBytes)
    {
        checkArgument(expectedEntries >= 0, "expectedEntries %s must be positive", expectedEntries);
        checkArgument(expectedBytes >= 0, "expectedBytes %s must be positive", expectedBytes);
        this.isNull = new boolean[expectedEntries];
        this.offsets = new int[expectedEntries + 1];
        this.bytes = new byte[expectedBytes];
    }

    @Override
    public void appendNull()
    {
        isNull[positionCount] = true;
        offsets[positionCount + 1] = offsets[positionCount];
        positionCount++;
        hasNullValue = true;
    }

    public void writeEntry(Slice source)
    {
        int bytesWritten = source.length();
        ensureFreeSpace(bytesWritten);

        source.getBytes(0, bytes, offsets[positionCount], bytesWritten);
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;

        positionCount++;
        hasNonNullValue = true;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int bytesWritten = 0;
        if (variableWidthBlock.isNull(position)) {
            isNull[positionCount] = true;
            hasNullValue = true;
        }
        else {
            int rawArrayBase = variableWidthBlock.getRawArrayBase();
            int[] rawOffsets = variableWidthBlock.getRawOffsets();
            int startValueOffset = rawOffsets[rawArrayBase + position];
            int endValueOffset = rawOffsets[rawArrayBase + position + 1];
            int length = endValueOffset - startValueOffset;
            ensureFreeSpace(length);

            Slice rawSlice = variableWidthBlock.getRawSlice();
            byte[] rawByteArray = rawSlice.byteArray();
            int byteArrayOffset = rawSlice.byteArrayOffset();

            System.arraycopy(rawByteArray, byteArrayOffset + startValueOffset, bytes, offsets[positionCount], length);
            bytesWritten = length;
            hasNonNullValue = true;
        }
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;
        positionCount++;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return new VariableWidthBlock(0, positionCount, Slices.wrappedBuffer(bytes, 0, offsets[positionCount]), offsets, hasNullValue ? isNull : null);
    }

    @Override
    public PreSizedBlockBuilder newBlockBuilderLike(int expectedEntries)
    {
        if (positionCount == 0) {
            return new VariableWidthPreSizedBlockBuilder(expectedEntries);
        }
        double bytesPerEntry = (double) offsets[positionCount] / positionCount;
        int expectedBytes = (int) min(bytesPerEntry * expectedEntries, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        return new VariableWidthPreSizedBlockBuilder(expectedEntries, expectedBytes);
    }

    private void ensureFreeSpace(int extraBytesCapacity)
    {
        int requiredSize = offsets[positionCount] + extraBytesCapacity;
        if (bytes.length >= requiredSize) {
            return;
        }

        int newSize = calculateNewArraySize(requiredSize, bytes.length);
        bytes = Arrays.copyOf(bytes, newSize);
    }
}
