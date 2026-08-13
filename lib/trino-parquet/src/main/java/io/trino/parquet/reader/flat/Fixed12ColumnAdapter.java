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
import io.trino.spi.block.Fixed12Block;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getBits;

public class Fixed12ColumnAdapter
        implements ColumnAdapter<int[]>
{
    public static final Fixed12ColumnAdapter FIXED12_ADAPTER = new Fixed12ColumnAdapter();

    @Override
    public int[] createBuffer(int size)
    {
        return new int[size * 3];
    }

    @Override
    public Block createNonNullBlock(int[] values)
    {
        return new Fixed12Block(values.length / 3, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(long[] valueIsValid, int[] values)
    {
        return new Fixed12Block(values.length / 3, Optional.of(valueIsValid), values);
    }

    /// Expands packed non-null values into a destination shaped by a validity bitmap using the
    /// [io.trino.spi.block.Bitmap] encoding.
    @Override
    public void unpackNullValues(int[] source, int[] destination, long[] valueIsValid, int destOffset, int nonNullCount, int totalValuesCount)
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
    public void copyValues(int[] source, int sourceIndex, int[] destination, int destinationIndex, int length)
    {
        System.arraycopy(source, sourceIndex * 3, destination, destinationIndex * 3, length * 3);
    }

    @Override
    public void decodeDictionaryIds(int[] values, int offset, int length, int[] ids, int[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            int id = 3 * ids[i];
            int destinationIndex = 3 * (offset + i);
            values[destinationIndex] = dictionary[id];
            values[destinationIndex + 1] = dictionary[id + 1];
            values[destinationIndex + 2] = dictionary[id + 2];
        }
    }

    @Override
    public long getSizeInBytes(int[] values)
    {
        return sizeOf(values);
    }

    @Override
    public int[] merge(List<int[]> buffers)
    {
        return IntColumnAdapter.concatIntArrays(buffers);
    }

    private void copyValue(int[] source, int sourceIndex, int[] destination, int destinationIndex)
    {
        destination[destinationIndex * 3] = source[sourceIndex * 3];
        destination[(destinationIndex * 3) + 1] = source[(sourceIndex * 3) + 1];
        destination[(destinationIndex * 3) + 2] = source[(sourceIndex * 3) + 2];
    }
}
