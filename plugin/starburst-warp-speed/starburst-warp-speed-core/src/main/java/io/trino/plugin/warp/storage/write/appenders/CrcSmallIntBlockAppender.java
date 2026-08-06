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
package io.trino.plugin.warp.storage.write.appenders;

import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;

import static io.trino.spi.type.SmallintType.SMALLINT;

public class CrcSmallIntBlockAppender
        extends CrcBlockAppender
{
    public CrcSmallIntBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendValues(
            int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        return switch (blockPos.getBlock()) {
            case RunLengthEncodedBlock rleBlock -> appendRepeatedValue((ShortArrayBlock) rleBlock.getValue(), jufferPos, blockPos, warmupElementStatsBuilder);
            case DictionaryBlock dictionaryBlock -> appendDictionaryBlock(dictionaryBlock, jufferPos, blockPos, warmupElementStatsBuilder);
            case ValueBlock valueBlock -> appendValueBlock((ShortArrayBlock) valueBlock, jufferPos, blockPos, warmupElementStatsBuilder);
        };
    }

    private AppendResult appendValueBlock(ShortArrayBlock valueBlock, int jufferPos, BlockPosHolder blockPos, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        int nullsCount = 0;
        if (valueBlock.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                int position = blockPos.getBlockPosition();
                if (valueBlock.isNull(position)) {
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                    short val = valueBlock.getShort(position);
                    warmupElementStatsBuilder.updateMinMax(val);
                    writeValue(jufferPos, blockPos, val);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                short val = valueBlock.getShort(blockPos.getBlockPosition());
                warmupElementStatsBuilder.updateMinMax(val);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendDictionaryBlock(DictionaryBlock dictionaryBlock, int jufferPos, BlockPosHolder blockPos, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        ShortArrayBlock dictionary = (ShortArrayBlock) dictionaryBlock.getDictionary();
        int nullsCount = 0;
        if (dictionaryBlock.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                int position = dictionaryBlock.getId(blockPos.getBlockPosition());
                if (dictionary.isNull(position)) {
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                    short val = dictionary.getShort(position);
                    warmupElementStatsBuilder.updateMinMax(val);
                    writeValue(jufferPos, blockPos, val);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                short val = dictionary.getShort(dictionaryBlock.getId(blockPos.getBlockPosition()));
                warmupElementStatsBuilder.updateMinMax(val);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendRepeatedValue(ShortArrayBlock valueBlock, int jufferPos, BlockPosHolder blockPos, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        int nullsCount = 0;
        if (valueBlock.isNull(0)) {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
        }
        else {
            short val = valueBlock.getShort(0);
            warmupElementStatsBuilder.updateMinMax(val);
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    @Override
    protected AppendResult appendFromMapBlock(
            BlockPosHolder blockPos,
            int jufferPos,
            Object key)
    {
        int nullsCount = 0;
        for (; blockPos.inRange(); blockPos.advance()) {
            SqlMap elementBlock = (SqlMap) blockPos.getObject();
            int pos = elementBlock.seekKey(key);
            if (pos == -1 || elementBlock.getRawValueBlock().isNull(elementBlock.getRawOffset() + pos)) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                short val = SMALLINT.getShort(elementBlock.getRawValueBlock(), elementBlock.getRawOffset() + pos);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(int jufferPos, BlockPosHolder blockPos, short val)
    {
        crcJuffers.put(val, jufferPos, blockPos);
        juffersWE.updateRecordBufferProps(val);
    }
}
