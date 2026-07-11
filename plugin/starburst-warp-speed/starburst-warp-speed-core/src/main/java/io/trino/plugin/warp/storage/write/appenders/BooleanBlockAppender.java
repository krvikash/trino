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
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.nio.ByteBuffer;

public class BooleanBlockAppender
        extends DataBlockAppender
{
    private static final byte TRUE_VALUE = 1;
    private static final byte FALSE_VALUE = 0;

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
            case RunLengthEncodedBlock rleBlock -> appendRepeatedValue((BitArrayBlock) rleBlock.getValue(), blockPos, buff);
            case DictionaryBlock dictionaryBlock -> appendDictionaryBlock(dictionaryBlock, blockPos, buff);
            case ValueBlock valueBlock -> appendValueBlock((BitArrayBlock) valueBlock, blockPos, buff);
        };
    }

    private AppendResult appendValueBlock(BitArrayBlock valueBlock, BlockPosHolder blockPos, ByteBuffer buff)
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
                    writeValue(valueBlock.getBoolean(position), buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(valueBlock.getBoolean(blockPos.getBlockPosition()), buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendDictionaryBlock(DictionaryBlock dictionaryBlock, BlockPosHolder blockPos, ByteBuffer buff)
    {
        BitArrayBlock dictionary = (BitArrayBlock) dictionaryBlock.getDictionary();
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
                    writeValue(dictionary.getBoolean(position), buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(dictionary.getBoolean(dictionaryBlock.getId(blockPos.getBlockPosition())), buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendRepeatedValue(BitArrayBlock valueBlock, BlockPosHolder blockPos, ByteBuffer buff)
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
            boolean value = valueBlock.getBoolean(0);
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                writeValue(value, buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    // the storage engine stores one byte per boolean, matching the encoding the predicate fill writes for booleans
    private static void writeValue(boolean value, ByteBuffer buff)
    {
        buff.put(value ? TRUE_VALUE : FALSE_VALUE);
    }
}
