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
import io.trino.spi.block.IntArrayBlock;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getBits;
import static java.lang.Math.toIntExact;

public class IntColumnAdapter
        implements ColumnAdapter<int[]>
{
    public static final IntColumnAdapter INT_ADAPTER = new IntColumnAdapter();

    @Override
    public int[] createBuffer(int size)
    {
        return new int[size];
    }

    @Override
    public Block createNonNullBlock(int[] values)
    {
        return new IntArrayBlock(values.length, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(long[] valueIsValid, int[] values)
    {
        return new IntArrayBlock(values.length, Optional.of(valueIsValid), values);
    }

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
                    destination[destOffset] = source[srcOffset];
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
                    destination[destOffset] = source[srcOffset];
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
        System.arraycopy(source, sourceIndex, destination, destinationIndex, length);
    }

    @Override
    public void decodeDictionaryIds(int[] values, int offset, int length, int[] ids, int[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            values[offset + i] = dictionary[ids[i]];
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
        return concatIntArrays(buffers);
    }

    static int[] concatIntArrays(List<int[]> buffers)
    {
        long resultSize = 0;
        for (int[] buffer : buffers) {
            resultSize += buffer.length;
        }
        int[] result = new int[toIntExact(resultSize)];
        int offset = 0;
        for (int[] buffer : buffers) {
            System.arraycopy(buffer, 0, result, offset, buffer.length);
            offset += buffer.length;
        }
        return result;
    }
}
