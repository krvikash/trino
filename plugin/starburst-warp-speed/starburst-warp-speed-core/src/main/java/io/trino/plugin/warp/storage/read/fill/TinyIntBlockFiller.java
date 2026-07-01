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
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.TinyintType;

import java.nio.ByteBuffer;
import java.util.Optional;

public class TinyIntBlockFiller
        extends BlockFiller<Long>
{
    public TinyIntBlockFiller()
    {
        super(TinyintType.TINYINT, BlockFillerType.TINY_INT);
    }

    @Override
    protected Long getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        return (long) buff.get();
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        byte[] mappingValues = new byte[2]; // including null
        long[] valueIsValid = Bitmap.allocateWords(2, true);
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        mappingValues[0] = buff.get();
        Bitmap.clear(valueIsValid, 0, 1);
        Block dictionaryBlock = new ByteArrayBlock(2, Optional.of(valueIsValid), mappingValues);

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
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        byte[] values = new byte[rowsToFill];
        Optional<long[]> valueIsValidOptional;
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
        return new ByteArrayBlock(rowsToFill, valueIsValidOptional, values);
    }
}
