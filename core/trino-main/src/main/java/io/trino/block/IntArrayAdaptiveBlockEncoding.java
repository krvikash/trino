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
package io.trino.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.IntArrayBlock;

import java.util.Optional;

import static io.trino.spi.block.BlockShim.getRawValueIsValid;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static java.lang.System.arraycopy;

/**
 * Adaptive encoding for int array blocks, selecting the most efficient encoding
 * method based on the characteristics of the data.
 *
 * <p>Layout of an encoded block:
 * <pre>
 * +-------------------+----------------------+--------------------+-----------+
 * | 4 bytes           | n bytes (optional)   | 4 bytes (optional) | 1 byte    |
 * | Position count    | Null bits (if any)   | Non-null count     | Method ID |
 * +-------------------+----------------------+--------------------+-----------+
 * | x bytes: values (method-dependent size)                                   |
 * +---------------------------------------------------------------------------+
 * </pre>
 */
public class IntArrayAdaptiveBlockEncoding
        implements BlockEncoding
{
    @Override
    public String getName()
    {
        return "INT_AD";
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return IntArrayBlock.class;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        int positionCount = input.readInt();

        long[] validityAsLongs = decodeValidityAsLongs(input, positionCount);
        int[] values = new int[positionCount];

        if (validityAsLongs == null) {
            AdaptiveIntEncoding.decode(input, values, positionCount);
            return new IntArrayBlock(positionCount, Optional.empty(), values);
        }

        int nonNullPositionCount = input.readInt();

        AdaptiveIntEncoding.decode(input, values, nonNullPositionCount);
        restoreNullPositions(positionCount, nonNullPositionCount, values, validityAsLongs);

        return new IntArrayBlock(positionCount, Optional.of(validityAsLongs), values);
    }

    private static void restoreNullPositions(int positionCount, int nonNullPositionCount, int[] values, long[] validityWords)
    {
        int position = nonNullPositionCount - 1;
        int tailStart = positionCount & ~63;

        // Handle last (positionCount % 64) positions
        for (int i = positionCount - 1; i >= tailStart && position >= 0; i--) {
            values[i] = values[position];
            if (Bitmap.isSet(validityWords, 0, i)) { // non-null
                position--;
            }
        }

        // Handle 64 positions at a time
        for (int wordIndex = (tailStart >>> 6) - 1; wordIndex >= 0 && position >= 0; wordIndex--) {
            long validity = validityWords[wordIndex]; // set = non-null
            int base = wordIndex << 6;
            if (validity == -1L) { // all non-null
                arraycopy(values, position - 63, values, base, 64);
                position -= 64;
            }
            else if (validity != 0L) { // mixed
                for (int j = base + 63; j >= base && position >= 0; j--) {
                    values[j] = values[position];
                    if (Bitmap.isSet(validityWords, 0, j)) {
                        position--;
                    }
                }
            }
            // all null: do nothing
        }
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput output, Block block)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        int positionCount = intArrayBlock.getPositionCount();
        output.appendInt(positionCount);

        encodeValidityAsLongs(output, getRawValueIsValid(intArrayBlock), intArrayBlock.getRawValuesOffset(), positionCount);

        if (!intArrayBlock.mayHaveNull()) {
            AdaptiveIntEncoding.encode(output, intArrayBlock.getRawValues(), intArrayBlock.getRawValuesOffset(), positionCount);
        }
        else {
            int[] valuesWithoutNull = new int[positionCount];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                valuesWithoutNull[nonNullPositionCount] = intArrayBlock.getInt(i);
                if (!intArrayBlock.isNull(i)) {
                    nonNullPositionCount++;
                }
            }

            output.writeInt(nonNullPositionCount);

            AdaptiveIntEncoding.encode(output, valuesWithoutNull, 0, nonNullPositionCount);
        }
    }
}
