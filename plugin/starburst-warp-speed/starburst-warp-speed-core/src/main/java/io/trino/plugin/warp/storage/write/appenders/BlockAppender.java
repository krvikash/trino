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

import io.trino.plugin.warp.dictionary.DictionaryException;
import io.trino.plugin.warp.dictionary.WriteDictionary;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.storage.juffers.CrcJuffer;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;
import io.trino.plugin.warp.warmup.exceptions.WarmupException;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Optional;

public abstract class BlockAppender
{
    protected static final byte NULL_VALUE_BYTE_SIGNAL = -1;
    protected static final byte NON_NULL_VALUE_BYTE_SIGNAL = 0;
    protected static final byte ZERO_BYTE_SIGNAL = 0;
    private static final byte[] padding = new byte[8192];   // PageSize
    protected final WriteJuffersWarmUpElement juffersWE;
    protected final ByteBuffer nullBuff;
    protected final CrcJuffer crcJuffers;

    public BlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        this.juffersWE = juffersWE;
        this.nullBuff = juffersWE.getNullBuffer();
        this.crcJuffers = juffersWE.getCrcJuffer();
    }

    public final AppendResult append(
            int jufferPos,
            BlockPosHolder blockPos,
            Optional<WriteDictionary> writeDictionary,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        AppendResult result;
        if (writeDictionary.isEmpty()) {
            result = appendWithoutDictionary(jufferPos, blockPos, warmUpElement, warmupElementStatsBuilder);
        }
        else {
            result = tryAppendWithDictionary(blockPos, writeDictionary.get(), warmupElementStatsBuilder);
        }
        warmupElementStatsBuilder.incNullCount(result.nullsCount());
        return result;
    }

    abstract AppendResult appendWithoutDictionary(
            int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder);

    protected void padBuffer(ByteBuffer buff, int len)
    {
        if (len > padding.length) {
            while (len > 0) {
                int size = Math.min(len, padding.length);
                buff.put(padding, 0, size);
                len -= size;
            }
        }
        else {
            buff.put(padding, 0, len);
        }
    }

    final AppendResult tryAppendWithDictionary(BlockPosHolder blockPos, WriteDictionary writeDictionary, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        try {
            return appendWithDictionary(blockPos, writeDictionary, warmupElementStatsBuilder);
        }
        catch (DictionaryException de) {
            throw de;
        }
        catch (Exception e) {
            throw new DictionaryException("failed to append with dictionary", WarmUpElementState.State.FAILED_TEMPORARILY, writeDictionary.getDictionaryKey(), DictionaryState.DICTIONARY_REJECTED);
        }
    }

    AppendResult appendWithDictionary(BlockPosHolder blockPos, WriteDictionary writeDictionary, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        throw new UnsupportedOperationException();
    }

    void writeValue(short key, ShortBuffer buffer)
    {
        juffersWE.updateRecordBufferProps(key);
        buffer.put(key);
    }

    protected boolean commitWEIfNeeded(BlockPosHolder blockPos, ByteBuffer buff, int jufferPos, int addedNv, int recBuffSize)
    {
        boolean committed = false;
        if (buff.position() >= recBuffSize) {
            if (buff.position() > recBuffSize) {
                throw new WarmupException(
                        "appendStringBlock reached " + buff.position() + " beyond recBuffSize " + recBuffSize,
                        WarmUpElementState.State.FAILED_PERMANENTLY);
            }
            juffersWE.commitAndResetWE(jufferPos + blockPos.getPos(), addedNv, buff.position(), 0);
            committed = true;
        }
        return committed;
    }

    protected AppendResult appendFromMapBlock(BlockPosHolder blockPos, int jufferPos, Object key)
    {
        throw new UnsupportedOperationException();
    }
}
