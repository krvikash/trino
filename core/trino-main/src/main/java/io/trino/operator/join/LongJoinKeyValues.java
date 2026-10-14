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

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.trino.operator.join.PagesHash.getHashPosition;

public final class LongJoinKeyValues
        implements JoinKeyValues
{
    private static final int INSTANCE_SIZE = instanceSize(LongJoinKeyValues.class);

    private final long[] values;

    public LongJoinKeyValues(int positionCount)
    {
        values = new long[positionCount];
    }

    @Override
    public long get(int position)
    {
        return values[position];
    }

    @Override
    public void copyFrom(Block block, int offset, int positionCount, int mask, int[] hashPositions)
    {
        switch (block) {
            case RunLengthEncodedBlock rleBlock -> {
                long value = ((LongArrayBlock) rleBlock.getValue()).getLong(0);
                Arrays.fill(values, offset, offset + positionCount, value);
                Arrays.fill(hashPositions, 0, positionCount, getHashPosition(value, mask));
            }
            case DictionaryBlock dictionaryBlock -> {
                LongArrayBlock dictionary = (LongArrayBlock) dictionaryBlock.getDictionary();
                long[] rawValues = dictionary.getRawValues();
                int rawOffset = dictionary.getRawValuesOffset();
                int[] rawIds = dictionaryBlock.getRawIds();
                int rawIdsOffset = dictionaryBlock.getRawIdsOffset();
                for (int i = 0; i < positionCount; i++) {
                    long value = rawValues[rawOffset + rawIds[rawIdsOffset + i]];
                    values[offset + i] = value;
                    hashPositions[i] = getHashPosition(value, mask);
                }
            }
            case ValueBlock valueBlock -> {
                LongArrayBlock longArrayBlock = (LongArrayBlock) valueBlock;
                long[] rawValues = longArrayBlock.getRawValues();
                int rawOffset = longArrayBlock.getRawValuesOffset();
                for (int i = 0; i < positionCount; i++) {
                    long value = rawValues[rawOffset + i];
                    values[offset + i] = value;
                    hashPositions[i] = getHashPosition(value, mask);
                }
            }
        }
    }

    @Override
    public void copyFrom(Block block, int offset, int[] positions, int mask, int[] hashPositions)
    {
        switch (block) {
            // A run-length block with nulls is all null, so it has no positions to copy
            case RunLengthEncodedBlock _ -> {}
            case DictionaryBlock dictionaryBlock -> {
                LongArrayBlock dictionary = (LongArrayBlock) dictionaryBlock.getDictionary();
                long[] rawValues = dictionary.getRawValues();
                int rawOffset = dictionary.getRawValuesOffset();
                int[] rawIds = dictionaryBlock.getRawIds();
                int rawIdsOffset = dictionaryBlock.getRawIdsOffset();
                for (int position : positions) {
                    long value = rawValues[rawOffset + rawIds[rawIdsOffset + position]];
                    values[offset + position] = value;
                    hashPositions[position] = getHashPosition(value, mask);
                }
            }
            case ValueBlock valueBlock -> {
                LongArrayBlock longArrayBlock = (LongArrayBlock) valueBlock;
                long[] rawValues = longArrayBlock.getRawValues();
                int rawOffset = longArrayBlock.getRawValuesOffset();
                for (int position : positions) {
                    long value = rawValues[rawOffset + position];
                    values[offset + position] = value;
                    hashPositions[position] = getHashPosition(value, mask);
                }
            }
        }
    }

    @Override
    public long readKey(ValueBlock block, int position)
    {
        return ((LongArrayBlock) block).getLong(position);
    }

    @Override
    public void readKeys(Block block, int[] positions, int positionCount, int mask, long[] target, int[] hashPositions)
    {
        switch (block) {
            case RunLengthEncodedBlock rleBlock -> {
                long value = ((LongArrayBlock) rleBlock.getValue()).getLong(0);
                Arrays.fill(target, 0, positionCount, value);
                Arrays.fill(hashPositions, 0, positionCount, getHashPosition(value, mask));
            }
            case DictionaryBlock dictionaryBlock -> {
                LongArrayBlock dictionary = (LongArrayBlock) dictionaryBlock.getDictionary();
                long[] rawValues = dictionary.getRawValues();
                int rawOffset = dictionary.getRawValuesOffset();
                int[] rawIds = dictionaryBlock.getRawIds();
                int rawIdsOffset = dictionaryBlock.getRawIdsOffset();
                for (int i = 0; i < positionCount; i++) {
                    long value = rawValues[rawOffset + rawIds[rawIdsOffset + positions[i]]];
                    target[i] = value;
                    hashPositions[i] = getHashPosition(value, mask);
                }
            }
            case ValueBlock valueBlock -> {
                LongArrayBlock longArrayBlock = (LongArrayBlock) valueBlock;
                long[] rawValues = longArrayBlock.getRawValues();
                int rawOffset = longArrayBlock.getRawValuesOffset();
                for (int i = 0; i < positionCount; i++) {
                    long value = rawValues[rawOffset + positions[i]];
                    target[i] = value;
                    hashPositions[i] = getHashPosition(value, mask);
                }
            }
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values);
    }

    public static long getEstimatedRetainedSizeInBytes(int positionCount)
    {
        return INSTANCE_SIZE + sizeOfLongArray(positionCount);
    }
}
