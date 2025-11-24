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

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.block.PreSizedBlockBuilder.checkArgument;
import static java.lang.Math.addExact;

public class VariableWidthPreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true});

    private final boolean[] isNull;
    private final int[] offsets;
    private final List<Slice> values;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    public VariableWidthPreSizedBlockBuilder(int expectedEntries)
    {
        checkArgument(expectedEntries >= 0, "expectedEntries %s must be positive", expectedEntries);
        this.isNull = new boolean[expectedEntries];
        this.offsets = new int[expectedEntries + 1];
        this.values = new ArrayList<>(expectedEntries);
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
        addNonNullEntry(source, source.length());
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        if (variableWidthBlock.isNull(position)) {
            isNull[positionCount] = true;
            offsets[positionCount + 1] = offsets[positionCount];
            positionCount++;
            hasNullValue = true;
        }
        else {
            Slice value = variableWidthBlock.getSlice(position);
            addNonNullEntry(value, value.length());
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return new VariableWidthBlock(0, positionCount, asSlice(), offsets, hasNullValue ? isNull : null);
    }

    private void addNonNullEntry(Slice value, int bytesWritten)
    {
        values.add(value);
        offsets[positionCount + 1] = addExact(offsets[positionCount], bytesWritten);

        positionCount++;
        hasNonNullValue = true;
    }

    private Slice asSlice()
    {
        int totalSize = offsets[positionCount];
        Slice slice = Slices.allocate(totalSize);
        int offset = 0;
        for (Slice value : values) {
            slice.setBytes(offset, value);
            offset += value.length();
        }
        return slice;
    }
}
