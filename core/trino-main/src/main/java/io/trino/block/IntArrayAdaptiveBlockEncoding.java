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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.IntArrayBlock;

import java.util.Optional;

import static io.trino.spi.block.BlockShim.getRawValueIsNull;
import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
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
    private final AdaptiveIntEncoding adaptiveIntEncoding;

    public IntArrayAdaptiveBlockEncoding(boolean vByteEncodingEnabled)
    {
        this.adaptiveIntEncoding = new AdaptiveIntEncoding(vByteEncodingEnabled);
    }

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

        byte[] valueIsNullPacked = retrieveNullBits(input, positionCount);
        int[] values = new int[positionCount];

        if (valueIsNullPacked == null) {
            adaptiveIntEncoding.decode(input, values, positionCount);
            return new IntArrayBlock(positionCount, Optional.empty(), values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        int nonNullPositionCount = input.readInt();

        adaptiveIntEncoding.decode(input, values, nonNullPositionCount);
        restoreNullPositions(positionCount, nonNullPositionCount, values, valueIsNull, valueIsNullPacked);

        return new IntArrayBlock(positionCount, Optional.of(valueIsNull), values);
    }

    private static void restoreNullPositions(int positionCount, int nonNullPositionCount, int[] values, boolean[] valueIsNull, byte[] valueIsNullPacked)
    {
        int position = nonNullPositionCount - 1;

        // Handle Last (positionCount % 8) values
        for (int i = positionCount - 1; i >= (positionCount & ~0b111) && position >= 0; i--) {
            values[i] = values[position];
            if (!valueIsNull[i]) {
                position--;
            }
        }

        // Handle the remaining positions.
        for (int i = (positionCount & ~0b111) - 8; i >= 0 && position >= 0; i -= 8) {
            byte packed = valueIsNullPacked[i >>> 3];
            if (packed == 0) { // Only values
                arraycopy(values, position - 7, values, i, 8);
                position -= 8;
            }
            else if (packed != -1) { // At least one non-null
                for (int j = i + 7; j >= i && position >= 0; j--) {
                    values[j] = values[position];
                    if (!valueIsNull[j]) {
                        position--;
                    }
                }
            }
            // Do nothing if there are only nulls
        }
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput output, Block block)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        int positionCount = intArrayBlock.getPositionCount();
        output.appendInt(positionCount);

        encodeNullsAsBits(output, getRawValueIsNull(intArrayBlock), intArrayBlock.getRawValuesOffset(), positionCount);

        if (!intArrayBlock.mayHaveNull()) {
            adaptiveIntEncoding.encode(output, intArrayBlock.getRawValues(), intArrayBlock.getRawValuesOffset(), positionCount);
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

            adaptiveIntEncoding.encode(output, valuesWithoutNull, 0, nonNullPositionCount);
        }
    }
}
