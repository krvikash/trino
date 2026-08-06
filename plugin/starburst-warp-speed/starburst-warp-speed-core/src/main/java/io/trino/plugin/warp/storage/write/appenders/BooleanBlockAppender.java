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
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.nio.ByteBuffer;

public class BooleanBlockAppender
        extends DataBlockAppender
{
    public BooleanBlockAppender(WriteJuffersWarmUpElement juffersWE)
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
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        return switch (blockPos.getBlock()) {
            case RunLengthEncodedBlock rleBlock -> appendRepeatedValue((ByteArrayBlock) rleBlock.getValue(), blockPos, buff);
            case DictionaryBlock dictionaryBlock -> appendDictionaryBlock(dictionaryBlock, blockPos, buff);
            case ValueBlock valueBlock -> appendValueBlock((ByteArrayBlock) valueBlock, blockPos, buff);
        };
    }

    private AppendResult appendValueBlock(ByteArrayBlock valueBlock, BlockPosHolder blockPos, ByteBuffer buff)
    {
        int nullsCount = 0;
        if (valueBlock.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                int position = blockPos.getBlockPosition();
                if (valueBlock.isNull(position)) {
                    buff.put(ZERO_BYTE_SIGNAL);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                    writeValue(valueBlock.getByte(position), buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(valueBlock.getByte(blockPos.getBlockPosition()), buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendDictionaryBlock(DictionaryBlock dictionaryBlock, BlockPosHolder blockPos, ByteBuffer buff)
    {
        ByteArrayBlock dictionary = (ByteArrayBlock) dictionaryBlock.getDictionary();
        int nullsCount = 0;
        if (dictionaryBlock.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                int position = dictionaryBlock.getId(blockPos.getBlockPosition());
                if (dictionary.isNull(position)) {
                    buff.put(ZERO_BYTE_SIGNAL);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                    writeValue(dictionary.getByte(position), buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(dictionary.getByte(dictionaryBlock.getId(blockPos.getBlockPosition())), buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendRepeatedValue(ByteArrayBlock valueBlock, BlockPosHolder blockPos, ByteBuffer buff)
    {
        int nullsCount = 0;
        if (valueBlock.isNull(0)) {
            for (; blockPos.inRange(); blockPos.advance()) {
                buff.put(ZERO_BYTE_SIGNAL);
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
        }
        else {
            byte val = valueBlock.getByte(0);
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(val, buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    // normalizes any nonzero byte to 1, matching the encoding the predicate fill writes for booleans
    private void writeValue(byte val, ByteBuffer buff)
    {
        if (val != 0) {
            val = 1;
        }
        buff.put(val);
    }
}
