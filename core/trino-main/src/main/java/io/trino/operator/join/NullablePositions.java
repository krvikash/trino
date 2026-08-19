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
package io.trino.operator.join;

import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.sql.gen.columnar.ColumnarFilterUtils.filterSetBitsRange;
import static java.lang.Math.min;

public final class NullablePositions
{
    private NullablePositions() {}

    /**
     * Returns the indices of non-null positions across {@code nullableBlocks}, or an empty {@link Optional} when
     * every position is non-null. The empty result lets callers that don't need an explicit position list skip the
     * identity-array allocation that would otherwise occur on the all-non-null fast path.
     */
    public static Optional<int[]> getNonNullPositions(Block[] nullableBlocks, int nullableBlocksCount, int positionCount)
    {
        if (nullableBlocksCount == 0) {
            return Optional.empty();
        }
        if (nullableBlocksCount == 1) {
            // Special case for a single nullable block to avoid the need for an explicit validity accumulator
            int[] outputPositions = new int[positionCount];
            int outputPositionsCount = getNonNullPositions(nullableBlocks[0], positionCount, outputPositions);
            if (outputPositionsCount == positionCount) {
                return Optional.empty();
            }
            return Optional.of(Arrays.copyOf(outputPositions, outputPositionsCount));
        }

        long[] valueIsValid = allocateWords(positionCount, true);
        for (int i = 0; i < nullableBlocksCount; i++) {
            retainNonNullPositions(nullableBlocks[i], valueIsValid, positionCount);
        }

        int[] outputPositions = new int[positionCount];
        int outputPositionsCount = filterSetBitsRange(valueIsValid, 0, 0, positionCount, outputPositions);
        if (outputPositionsCount == positionCount) {
            return Optional.empty();
        }
        return Optional.of(Arrays.copyOf(outputPositions, outputPositionsCount));
    }

    private static int getNonNullPositions(Block nullableBlock, int positionCount, int[] outputPositions)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    return 0; // all positions are null
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<Bitmap> dictionaryValidity = dictionary.getValidityBitmap();
                if (dictionaryValidity.isPresent()) {
                    int outputPositionCount = 0;
                    long[] dictionaryIsValid = dictionaryValidity.get().getRawWords();
                    int validityOffset = dictionaryValidity.get().getRawBitOffset();
                    for (int position = 0; position < positionCount; position++) {
                        boolean isNull = !isSet(dictionaryIsValid, validityOffset, dictionaryBlock.getId(position));
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    Bitmap bitmap = validity.get();
                    return filterSetBitsRange(bitmap.getRawWords(), bitmap.getRawBitOffset(), 0, positionCount, outputPositions);
                }
            }
        }
        for (int position = 0; position < positionCount; position++) {
            outputPositions[position] = position;
        }
        return positionCount;
    }

    /**
     * Clears the accumulator bits of positions that are null in {@code nullableBlock}, a word at a time.
     */
    private static void retainNonNullPositions(Block nullableBlock, long[] valueIsValid, int positionCount)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    Arrays.fill(valueIsValid, 0);
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                Optional<Bitmap> dictionaryValidity = dictionaryBlock.getDictionary().getValidityBitmap();
                if (dictionaryValidity.isPresent()) {
                    long[] dictionaryIsValid = dictionaryValidity.get().getRawWords();
                    int validityOffset = dictionaryValidity.get().getRawBitOffset();
                    for (int position = 0; position < positionCount; position++) {
                        if (!isSet(dictionaryIsValid, validityOffset, dictionaryBlock.getId(position))) {
                            clear(valueIsValid, 0, position);
                        }
                    }
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    Bitmap bitmap = validity.get();
                    long[] blockIsValid = bitmap.getRawWords();
                    int rawBitOffset = bitmap.getRawBitOffset();
                    for (int position = 0, word = 0; position < positionCount; position += Long.SIZE, word++) {
                        valueIsValid[word] &= getBits(blockIsValid, rawBitOffset, position, min(Long.SIZE, positionCount - position));
                    }
                }
            }
        }
    }
}
