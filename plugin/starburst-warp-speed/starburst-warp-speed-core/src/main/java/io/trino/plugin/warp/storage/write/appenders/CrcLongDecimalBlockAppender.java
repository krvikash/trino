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
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Int128;

public class CrcLongDecimalBlockAppender
        extends CrcBlockAppender
{
    public CrcLongDecimalBlockAppender(WriteJuffersWarmUpElement juffersWE)
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
            case RunLengthEncodedBlock rleBlock -> appendRepeatedValue((Int128ArrayBlock) rleBlock.getValue(), jufferPos, blockPos);
            case DictionaryBlock dictionaryBlock -> appendDictionaryBlock(dictionaryBlock, jufferPos, blockPos);
            case ValueBlock valueBlock -> appendValueBlock((Int128ArrayBlock) valueBlock, jufferPos, blockPos);
        };
    }

    private AppendResult appendValueBlock(Int128ArrayBlock valueBlock, int jufferPos, BlockPosHolder blockPos)
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
                    writeValue(valueBlock.getInt128(position), blockPos, jufferPos);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(valueBlock.getInt128(blockPos.getBlockPosition()), blockPos, jufferPos);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendDictionaryBlock(DictionaryBlock dictionaryBlock, int jufferPos, BlockPosHolder blockPos)
    {
        Int128ArrayBlock dictionary = (Int128ArrayBlock) dictionaryBlock.getDictionary();
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
                    writeValue(dictionary.getInt128(position), blockPos, jufferPos);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(dictionary.getInt128(dictionaryBlock.getId(blockPos.getBlockPosition())), blockPos, jufferPos);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendRepeatedValue(Int128ArrayBlock valueBlock, int jufferPos, BlockPosHolder blockPos)
    {
        int nullsCount = 0;
        if (valueBlock.isNull(0)) {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
        }
        else {
            Int128 value = valueBlock.getInt128(0);
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(value, blockPos, jufferPos);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(Int128 value, BlockPosHolder blockPos, int jufferPos)
    {
        int crcJufferOffset = crcJuffers.put(value, jufferPos, blockPos);
        juffersWE.updateRecordBufferProps(
                value.getHigh(), // MSB used for min-max
                value,
                crcJufferOffset);      // position of the value in case of single value
    }
}
