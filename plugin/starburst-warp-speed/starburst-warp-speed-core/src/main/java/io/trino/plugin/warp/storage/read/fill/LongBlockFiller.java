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
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.BigintType;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Optional;

public class LongBlockFiller
        extends BlockFiller<Long>
{
    public LongBlockFiller()
    {
        super(BigintType.BIGINT, BlockFillerType.LONG);
    }

    @Override
    protected Long getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        LongBuffer buff = (LongBuffer) juffersWE.getRecordBuffer();
        return buff.get();
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        long singleValue = getSingleValue(juffersWE, recLength, 0);
        Block mappingBlock = createSingleMappingBlock(singleValue);

        return wrapSingleWithNulls(juffersWE, rowsToFill, mappingBlock, 1);
    }

    @Override
    protected Block fillRawBlock(
            ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
    {
        LongBuffer buff = (LongBuffer) juffersWE.getRecordBuffer();
        long[] values = new long[rowsToFill];
        Optional<boolean[]> valueIsNullOptional;
        if (collectNulls) {
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            boolean[] valueIsNull = new boolean[rowsToFill];
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, i)) {
                    valueIsNull[i] = true;
                }
            }
            valueIsNullOptional = Optional.of(valueIsNull);
        }
        else {
            valueIsNullOptional = Optional.empty();
        }
        buff.get(values);
        return new LongArrayBlock(rowsToFill, valueIsNullOptional, values);
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
        long singleVal = ((LongArrayBlock) mapBlock).getLong(mapKey);
        Block retBlock;

        if (!collectNulls) {
            retBlock = createSingleValueBlock(spiBuilderType, singleVal, rowsToFill);
        }
        else {
            Block singleMapBlock = createSingleMappingBlock(singleVal);
            retBlock = wrapSingleWithNulls(juffersWE, rowsToFill, singleMapBlock, 1);
        }

        return retBlock;
    }

    private Block createSingleMappingBlock(long singleValue)
    {
        long[] mappingValues = new long[2]; // including null
        boolean[] nulls = new boolean[2];
        mappingValues[0] = singleValue;
        nulls[1] = true;
        return new LongArrayBlock(2, Optional.of(nulls), mappingValues);
    }
}
