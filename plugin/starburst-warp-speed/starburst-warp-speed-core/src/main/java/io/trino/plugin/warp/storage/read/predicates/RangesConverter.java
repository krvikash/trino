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
package io.trino.plugin.warp.storage.read.predicates;

import io.airlift.slice.Slice;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.plugin.warp.util.StringPredicateData;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.predicate.SortedRangeSet;

import java.nio.ByteBuffer;
import java.util.function.Function;

class RangesConverter
{
    static final byte INCLUSIVE = (byte) 1;
    static final byte EXCLUSIVE = (byte) 0;
    // Unbounded markers
    static final int INT_LOWER_UNBOUNDED = Integer.MIN_VALUE;
    static final int INT_UPPER_UNBOUNDED = Integer.MAX_VALUE;
    static final long LONG_LOWER_UNBOUNDED = Long.MIN_VALUE;
    static final long LONG_UPPER_UNBOUNDED = Long.MAX_VALUE;
    static final short SHORT_LOWER_UNBOUNDED = Short.MIN_VALUE;
    static final short SHORT_UPPER_UNBOUNDED = Short.MAX_VALUE;
    static final byte BYTE_LOWER_UNBOUNDED = Byte.MIN_VALUE;
    static final byte BYTE_UPPER_UNBOUNDED = Byte.MAX_VALUE;
    static final float FLOAT_LOWER_UNBOUNDED = Float.NEGATIVE_INFINITY;
    static final float FLOAT_UPPER_UNBOUNDED = Float.POSITIVE_INFINITY;
    static final double DOUBLE_LOWER_UNBOUNDED = Double.NEGATIVE_INFINITY;
    static final double DOUBLE_UPPER_UNBOUNDED = Double.POSITIVE_INFINITY;
    static final long LONG_DECIMAL_LOWER_UNBOUNDED_MSB = Long.MIN_VALUE;
    static final long LONG_DECIMAL_LOWER_UNBOUNDED_LSB = 0L;
    static final long LONG_DECIMAL_UPPER_UNBOUNDED_MSB = Long.MAX_VALUE;
    static final long LONG_DECIMAL_UPPER_UNBOUNDED_LSB = 0xFFFFFFFFFFFFFFFFL;

    RangesConverter() {}

    void setBooleanRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        ByteArrayBlock valueBlock = (ByteArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.put((byte) 0);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(1)));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(i)));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(i + 1)));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2)));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.put((byte) 1);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setIntRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        IntArrayBlock valueBlock = (IntArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putInt(INT_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(1)));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(i)));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(i + 1)));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2)));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putInt(INT_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setRealRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        IntArrayBlock valueBlock = (IntArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putFloat(FLOAT_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(1)));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(i)));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(i + 1)));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putInt(valueBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2)));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putFloat(FLOAT_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setLongRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        LongArrayBlock valueBlock = (LongArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putLong(LONG_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putLong(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(1)));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putLong(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(i)));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putLong(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(i + 1)));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putLong(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2)));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putLong(LONG_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setLongDecimalRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        Int128ArrayBlock valueBlock = (Int128ArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putLong(LONG_DECIMAL_LOWER_UNBOUNDED_MSB);
            lowBuf.putLong(LONG_DECIMAL_LOWER_UNBOUNDED_LSB);
            lowBuf.put(INCLUSIVE); // inclusive

            int position = sortedRangesBlock.getUnderlyingValuePosition(1);
            highBuf.putLong(valueBlock.getInt128High(position));
            highBuf.putLong(valueBlock.getInt128Low(position));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            int lowPosition = sortedRangesBlock.getUnderlyingValuePosition(i);
            lowBuf.putLong(valueBlock.getInt128High(lowPosition));
            lowBuf.putLong(valueBlock.getInt128Low(lowPosition));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            int highPosition = sortedRangesBlock.getUnderlyingValuePosition(i + 1);
            highBuf.putLong(valueBlock.getInt128High(highPosition));
            highBuf.putLong(valueBlock.getInt128Low(highPosition));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            int position = sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2);
            lowBuf.putLong(valueBlock.getInt128High(position));
            lowBuf.putLong(valueBlock.getInt128Low(position));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putLong(LONG_DECIMAL_UPPER_UNBOUNDED_MSB);
            highBuf.putLong(LONG_DECIMAL_UPPER_UNBOUNDED_LSB);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setDoubleRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        LongArrayBlock valueBlock = (LongArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putDouble(DOUBLE_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putDouble(Double.longBitsToDouble(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(1))));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putDouble(Double.longBitsToDouble(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(i))));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putDouble(Double.longBitsToDouble(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(i + 1))));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putDouble(Double.longBitsToDouble(valueBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2))));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putDouble(DOUBLE_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setTinyintRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        ByteArrayBlock valueBlock = (ByteArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.put(BYTE_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(1)));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(i)));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(i + 1)));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.put(valueBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2)));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.put(BYTE_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setSmallIntRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        ShortArrayBlock valueBlock = (ShortArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putShort(SHORT_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putShort(valueBlock.getShort(sortedRangesBlock.getUnderlyingValuePosition(1)));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putShort(valueBlock.getShort(sortedRangesBlock.getUnderlyingValuePosition(i)));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putShort(valueBlock.getShort(sortedRangesBlock.getUnderlyingValuePosition(i + 1)));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putShort(valueBlock.getShort(sortedRangesBlock.getUnderlyingValuePosition(positionCount - 2)));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putShort(SHORT_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setStringRanges(
            SortedRangeSet sortedRangeSet,
            ByteBuffer lowBuf,
            ByteBuffer highBuf,
            int recLength,
            Function<Slice, Slice> sliceConverter)
    {
        boolean[] inclusive = sortedRangeSet.getInclusive();
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        VariableWidthBlock valueBlock = (VariableWidthBlock) sortedRangesBlock.getUnderlyingValueBlock();
        final int positionCount = sortedRangesBlock.getPositionCount();
        SliceUtils.StringPredicateDataFactory stringPredicateDataFactory = new SliceUtils.StringPredicateDataFactory();
        StringPredicateData stringPredicateData;
        int posIx = 0;

        if (isUnbounded(sortedRangesBlock, posIx)) {
            lowBuf.putLong(LONG_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            posIx++;

            Slice highSlice = valueBlock.getSlice(sortedRangesBlock.getUnderlyingValuePosition(posIx));
            Slice convertedHighSlice = sliceConverter.apply(highSlice);
            stringPredicateData = stringPredicateDataFactory.create(convertedHighSlice, recLength, false, highSlice);
            highBuf.putLong(stringPredicateData.comperationValue());
            highBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
            posIx++;
        }

        final int endPosIx = isUnbounded(sortedRangesBlock, positionCount - 1) ? positionCount - 2 : positionCount;
        while (posIx < endPosIx) {
            Slice lowSlice = valueBlock.getSlice(sortedRangesBlock.getUnderlyingValuePosition(posIx));
            Slice convertedLowSlice = sliceConverter.apply(lowSlice);
            stringPredicateData = stringPredicateDataFactory.create(convertedLowSlice, recLength, false, lowSlice);
            lowBuf.putLong(stringPredicateData.comperationValue());
            lowBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE);
            posIx++;

            Slice highSlice = valueBlock.getSlice(sortedRangesBlock.getUnderlyingValuePosition(posIx));
            Slice convertedHighSlice = sliceConverter.apply(highSlice);
            stringPredicateData = stringPredicateDataFactory.create(convertedHighSlice, recLength, false, highSlice);
            highBuf.putLong(stringPredicateData.comperationValue());
            highBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE);
            posIx++;
        }

        if (posIx < positionCount) {
            Slice lowSlice = valueBlock.getSlice(sortedRangesBlock.getUnderlyingValuePosition(posIx));
            Slice convertedLowSlice = sliceConverter.apply(lowSlice);
            stringPredicateData = stringPredicateDataFactory.create(convertedLowSlice, recLength, false, lowSlice);
            lowBuf.putLong(stringPredicateData.comperationValue());
            lowBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putLong(LONG_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    private boolean isUnbounded(Block sortedRangesBlock, int position)
    {
        return sortedRangesBlock.isNull(position);
    }
}
