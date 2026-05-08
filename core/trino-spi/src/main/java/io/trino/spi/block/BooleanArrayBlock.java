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

import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.ensureCapacity;

public final class BooleanArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(BooleanArrayBlock.class);
    public static final int SIZE_IN_BYTES_PER_POSITION = Byte.BYTES + Byte.BYTES;

    private final int arrayOffset;
    private final int positionCount;
    private final boolean[] values;

    private final long retainedSizeInBytes;

    public BooleanArrayBlock(int arrayOffset, int positionCount, boolean[] values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        retainedSizeInBytes = (INSTANCE_SIZE + sizeOf(values));
    }

    /**
     * Gets the raw byte array that keeps the actual data values.
     */
    public boolean[] getRawValues()
    {
        return values;
    }

    /**
     * Gets the offset into raw byte array where the data values start.
     */
    public int getRawValuesOffset()
    {
        return arrayOffset;
    }

    @Override
    public long getSizeInBytes()
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Byte.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(values, sizeOf(values));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    public boolean getBoolean(int position)
    {
        checkReadablePosition(this, position);
        return values[position + arrayOffset];
    }

    @Override
    public boolean mayHaveNull()
    {
        return false;
    }

    @Override
    public boolean hasNull()
    {
        return false;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return false;
    }

    @Override
    public boolean isNullUnchecked(int position)
    {
        return false;
    }

    @Override
    public BooleanArrayBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);
        return new BooleanArrayBlock(
                0,
                1,
                new boolean[] {values[position + arrayOffset]});
    }

    @Override
    public BooleanArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValues = new boolean[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(this, position);
            newValues[i] = values[position + arrayOffset];
        }
        return new BooleanArrayBlock(0, length, newValues);
    }

    @Override
    public BooleanArrayBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new BooleanArrayBlock(positionOffset + arrayOffset, length, values);
    }

    @Override
    public BooleanArrayBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;
        boolean[] newValues = compactArray(values, positionOffset, length);

        if (newValues == values) {
            return this;
        }
        return new BooleanArrayBlock(0, length, newValues);
    }

    @Override
    public BooleanArrayBlock copyWithAppendedNull()
    {
        boolean[] newValues = ensureCapacity(values, arrayOffset + positionCount + 1);

        return new BooleanArrayBlock(arrayOffset, positionCount + 1, newValues);
    }

    @Override
    public BooleanArrayBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "BooleanArrayBlock{positionCount=" + getPositionCount() + '}';
    }

    @Override
    public Optional<BooleanArrayBlock> getNulls()
    {
        return Optional.empty();
    }
}
