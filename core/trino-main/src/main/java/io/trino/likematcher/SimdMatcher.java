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
package io.trino.likematcher;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Implements the "Generic SIMD" algorithm from Wojciech Muła:
 * <a href="http://0x80.pl/notesen/2016-11-28-simd-strfind.html">SIMD-friendly algorithms for substring searching</a>
 */
final class SimdMatcher
        implements Matcher
{
    private static final VectorSpecies<Byte> SPECIES;
    private static final int VECTOR_BIT_SIZE;

    private final boolean exact;
    private final List<CompiledPattern> compiledPatterns = new ArrayList<>();

    static {
        int preferredSize = VectorShape.preferredShape().vectorBitSize();
        if (preferredSize > 512) {
            // VectorMask.toLong() supports at most 64 lanes (i.e., 512 bits).
            SPECIES = ByteVector.SPECIES_512;
        }
        else {
            SPECIES = ByteVector.SPECIES_PREFERRED;
        }
        VECTOR_BIT_SIZE = SPECIES.vectorBitSize();
    }

    private record CompiledPattern(byte[] bytes, int lastDistinctByteOffset)
    {
        public CompiledPattern
        {
            requireNonNull(bytes, "bytes is null");
        }
    }

    public SimdMatcher(List<Pattern> pattern, int start, int end, boolean exact)
    {
        this.exact = exact;

        for (int i = start; i <= end; i++) {
            Pattern element = pattern.get(i);

            switch (element) {
                case Pattern.Literal literal -> {
                    checkArgument(i == 0 || !(pattern.get(i - 1) instanceof Pattern.Literal), "Multiple consecutive literals found");

                    byte[] bytes = literal.value().getBytes(StandardCharsets.UTF_8);
                    int lastDistinctByteOffset = findLastDistinctByteOffset(bytes);
                    compiledPatterns.add(new CompiledPattern(bytes, lastDistinctByteOffset));
                }
                case Pattern.Any _ -> throw new IllegalArgumentException("'any' pattern not supported");
                case null, default -> {}
            }
        }
    }

    /**
     * Find the last position where a byte differs from the first byte.
     * Returns the last position if all bytes are the same.
     */
    private static int findLastDistinctByteOffset(byte[] pattern)
    {
        byte firstByte = pattern[0];

        for (int i = pattern.length - 1; i > 0; i--) {
            if (pattern[i] != firstByte) {
                return i;
            }
        }

        return pattern.length - 1;
    }

    public static boolean isSupported()
    {
        // Performance has not been evaluated below 128 bits.
        return VECTOR_BIT_SIZE >= 128;
    }

    @Override
    public boolean match(byte[] input, int offset, int length)
    {
        int start = offset;
        int remaining = length;

        for (CompiledPattern compiled : compiledPatterns) {
            if (remaining == 0) {
                return false;
            }

            byte[] pattern = compiled.bytes();

            int position = find(input, start, remaining, pattern, compiled.lastDistinctByteOffset());
            if (position == -1) {
                return false;
            }

            position += pattern.length;
            remaining -= position - start;
            start = position;
        }

        return !exact || remaining == 0;
    }

    private static int find(byte[] input, int offset, int length, byte[] pattern, int lastDistinctByteOffset)
    {
        if (pattern.length > length || pattern.length == 0) {
            return -1;
        }

        // Maximum offset from start where the pattern still fits
        int vectorizedLimit = length - pattern.length;
        int firstByteIndex = offset;

        byte firstByte = pattern[0];
        byte lastDistinctByte = pattern[lastDistinctByteOffset];
        ByteVector firstByteVector = ByteVector.broadcast(SPECIES, firstByte);
        ByteVector lastDistinctByteVector = ByteVector.broadcast(SPECIES, lastDistinctByte);

        for (; firstByteIndex < SPECIES.loopBound(vectorizedLimit) + offset; firstByteIndex += SPECIES.length()) {
            ByteVector firstVector = ByteVector.fromArray(SPECIES, input, firstByteIndex);
            ByteVector lastDistinctVector = ByteVector.fromArray(SPECIES, input, firstByteIndex + lastDistinctByteOffset);

            VectorMask<Byte> firstMask = firstVector.eq(firstByteVector);
            VectorMask<Byte> lastDistinctMask = lastDistinctVector.eq(lastDistinctByteVector);

            VectorMask<Byte> candidates = firstMask.and(lastDistinctMask);

            if (candidates.anyTrue()) {
                long bits = candidates.toLong();
                while (bits != 0) {
                    int candidateIndex = firstByteIndex + Long.numberOfTrailingZeros(bits);

                    if (verifyCandidate(input, candidateIndex, pattern)) {
                        return candidateIndex;
                    }

                    bits &= (bits - 1); // Clear bit for candidateIndex
                }
            }
        }

        // Scalar tail: check remaining positions
        for (; firstByteIndex <= vectorizedLimit + offset; firstByteIndex++) {
            if (input[firstByteIndex] == firstByte && input[firstByteIndex + lastDistinctByteOffset] == lastDistinctByte) {
                if (verifyCandidate(input, firstByteIndex, pattern)) {
                    return firstByteIndex;
                }
            }
        }

        return -1;
    }

    private static boolean verifyCandidate(byte[] input, int offset, byte[] pattern)
    {
        // Both bytes already verified by find() comparing firstByte and lastDistinctByte
        if (pattern.length <= 2) {
            return true;
        }

        // Position 0 was already checked, so we skip it
        return Arrays.equals(
                input,
                offset + 1,
                offset + pattern.length,
                pattern,
                1,
                pattern.length);
    }
}
