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
package io.trino.operator.torin.expression;

import com.google.common.collect.ImmutableList;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/// Helpers for testing expressions that take the four 64-bit sums of the 32-bit chunks [GpuExtractInt32Chunk]
/// extracts from DECIMAL128 values.
final class SumChunksTestUtils
{
    static final long MAX_ROWS = Integer.MAX_VALUE;
    static final long MAX_UNSIGNED_CHUNK = 0xFFFF_FFFFL;
    static final BigInteger MAX_38_DIGITS = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);

    private SumChunksTestUtils() {}

    // the chunk sums as columns 0..3
    static List<GpuExpression> chunkSumColumns()
    {
        return ImmutableList.of(new GetColumn(0), new GetColumn(1), new GetColumn(2), new GetColumn(3));
    }

    // chunk sums of a single-row 160-bit sum, not limited to 128 bits
    static ChunkSums sumOf(BigInteger sum)
    {
        return new ChunkSums(unsignedChunk(sum, 0), unsignedChunk(sum, 1), unsignedChunk(sum, 2), sum.shiftRight(96).longValueExact());
    }

    // chunk sums of `rows` rows of the same DECIMAL128 value, as the frame sums of its 32-bit chunks produce
    static ChunkSums sumOfRepeated(BigInteger value, long rows)
    {
        return new ChunkSums(
                unsignedChunk(value, 0) * rows,
                unsignedChunk(value, 1) * rows,
                unsignedChunk(value, 2) * rows,
                value.shiftRight(96).intValueExact() * rows);
    }

    static ChunkSums sumOfValues(long... values)
    {
        return sumOfValues(Arrays.stream(values).mapToObj(BigInteger::valueOf).toList());
    }

    // chunk sums of a frame holding the DECIMAL128 values, as the frame sums of their 32-bit chunks produce
    static ChunkSums sumOfValues(List<BigInteger> values)
    {
        long[] chunks = new long[4];
        for (BigInteger value : values) {
            chunks[0] += unsignedChunk(value, 0);
            chunks[1] += unsignedChunk(value, 1);
            chunks[2] += unsignedChunk(value, 2);
            chunks[3] += value.shiftRight(96).intValueExact();
        }
        return new ChunkSums(chunks[0], chunks[1], chunks[2], chunks[3]);
    }

    private static long unsignedChunk(BigInteger value, int chunkIndex)
    {
        // BigInteger shifts and masks act on the two's complement representation, also for negative values
        return value.shiftRight(32 * chunkIndex).and(BigInteger.valueOf(MAX_UNSIGNED_CHUNK)).longValueExact();
    }

    record ChunkSums(long chunk0, long chunk1, long chunk2, long chunk3)
    {
        long chunk(int index)
        {
            return switch (index) {
                case 0 -> chunk0;
                case 1 -> chunk1;
                case 2 -> chunk2;
                case 3 -> chunk3;
                default -> throw new IllegalArgumentException("Invalid chunk index: " + index);
            };
        }

        BigInteger sum()
        {
            return BigInteger.valueOf(chunk3).shiftLeft(96)
                    .add(BigInteger.valueOf(chunk2).shiftLeft(64))
                    .add(BigInteger.valueOf(chunk1).shiftLeft(32))
                    .add(BigInteger.valueOf(chunk0));
        }
    }
}
