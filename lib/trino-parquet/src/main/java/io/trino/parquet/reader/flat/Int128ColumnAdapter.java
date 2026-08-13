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
package io.trino.parquet.reader.flat;

import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getBits;

public class Int128ColumnAdapter
        implements ColumnAdapter<long[]>
{
    public static final Int128ColumnAdapter INT128_ADAPTER = new Int128ColumnAdapter();

    @Override
    public long[] createBuffer(int size)
    {
        return new long[size * 2];
    }

    @Override
    public Block createNonNullBlock(long[] values)
    {
        return new Int128ArrayBlock(values.length / 2, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(long[] valueIsValid, long[] values)
    {
        return new Int128ArrayBlock(values.length / 2, Optional.of(valueIsValid), values);
    }

    @Override
    public void unpackNullValues(long[] source, long[] destination, long[] valueIsValid, int destOffset, int nonNullCount, int totalValuesCount)
    {
        // Benchmarking shows scalar copying is faster once a validity word contains this many short runs.
        int scalarCopyTransitionThreshold = 12;
        int srcOffset = 0;
        int endOffset = destOffset + totalValuesCount;
        while (srcOffset < nonNullCount && destOffset < endOffset) {
            int bitsInWord = Math.min(Long.SIZE, endOffset - destOffset);
            long validBits = getBits(valueIsValid, 0, destOffset, bitsInWord);
            if (validBits == 0) {
                destOffset += bitsInWord;
                continue;
            }
            if (Long.bitCount(validBits) == bitsInWord) {
                copyValues(source, srcOffset, destination, destOffset, bitsInWord);
                srcOffset += bitsInWord;
                destOffset += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= scalarCopyTransitionThreshold) {
                int endOffsetInWord = destOffset + bitsInWord;
                while (destOffset < endOffsetInWord) {
                    copyValue(source, srcOffset, destination, destOffset);
                    srcOffset += (int) (validBits & 1);
                    destOffset++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = Math.min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                destOffset += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = Math.min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                if (validCount == 1) {
                    // Avoid copyValues overhead for singleton valid runs.
                    copyValue(source, srcOffset, destination, destOffset);
                }
                else {
                    copyValues(source, srcOffset, destination, destOffset, validCount);
                }
                srcOffset += validCount;
                destOffset += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
    }

    @Override
    public void copyValues(long[] source, int sourceIndex, long[] destination, int destinationIndex, int length)
    {
        System.arraycopy(source, sourceIndex * 2, destination, destinationIndex * 2, length * 2);
    }

    @Override
    public void decodeDictionaryIds(long[] values, int offset, int length, int[] ids, long[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            int id = 2 * ids[i];
            int destinationIndex = 2 * (offset + i);
            values[destinationIndex] = dictionary[id];
            values[destinationIndex + 1] = dictionary[id + 1];
        }
    }

    @Override
    public long getSizeInBytes(long[] values)
    {
        return sizeOf(values);
    }

    @Override
    public long[] merge(List<long[]> buffers)
    {
        return LongColumnAdapter.concatLongArrays(buffers);
    }

    private void copyValue(long[] source, int sourceIndex, long[] destination, int destinationIndex)
    {
        destination[destinationIndex * 2] = source[sourceIndex * 2];
        destination[(destinationIndex * 2) + 1] = source[(sourceIndex * 2) + 1];
    }
}
