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
package io.trino.plugin.warp.storage.read.fill;

import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.type.SmallintType;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Optional;

public class ShortBlockFiller
        extends BlockFiller<Long>
{
    public ShortBlockFiller()
    {
        super(SmallintType.SMALLINT, BlockFillerType.SHORT);
    }

    @Override
    protected Long getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        return (long) buff.get();
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        short[] mappingValues = new short[2]; // including null
        long[] isValueValid = Bitmap.allocateWords(2, true);
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        mappingValues[0] = buff.get();
        Bitmap.clear(isValueValid, 0, 1);
        Block dictionaryBlock = new ShortArrayBlock(2, Optional.of(isValueValid), mappingValues);

        return wrapSingleWithNulls(juffersWE, rowsToFill, dictionaryBlock, 1);
    }

    @Override
    protected Block fillRawBlock(
            ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
    {
        Optional<long[]> valueIsValidOptional;
        short[] values = new short[rowsToFill];
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        if (collectNulls) {
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            long[] valueIsValid = Bitmap.allocateWords(rowsToFill, true);
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, i)) {
                    Bitmap.clear(valueIsValid, 0, i);
                }
            }
            valueIsValidOptional = Optional.of(valueIsValid);
        }
        else {
            valueIsValidOptional = Optional.empty();
        }
        buff.get(values);
        return new ShortArrayBlock(rowsToFill, valueIsValidOptional, values);
    }

    @Override
    protected Block createSingleBlockWithMapping(
            ReadJuffersWarmUpElement juffersWE,
            int mapKey,
            int rowsToFill,
            Block mapBlock,
            RecTypeCode recTypeCode,
            boolean collectNulls)
    {
        short singleVal = ((ShortArrayBlock) mapBlock).getShort(mapKey);
        Block retBlock;

        if (!collectNulls) {
            retBlock = createSingleValueBlock(spiBuilderType, (long) singleVal, rowsToFill);
        }
        else {
            Block singleMapBlock = createSingleMappingBlock(singleVal);
            retBlock = wrapSingleWithNulls(juffersWE, rowsToFill, singleMapBlock, 1);
        }

        return retBlock;
    }

    private Block createSingleMappingBlock(short singleValue)
    {
        short[] mappingValues = new short[2]; // including null
        long[] isValueValid = Bitmap.allocateWords(2, true);
        mappingValues[0] = singleValue;
        Bitmap.clear(isValueValid, 0, 1);
        return new ShortArrayBlock(2, Optional.of(isValueValid), mappingValues);
    }
}
