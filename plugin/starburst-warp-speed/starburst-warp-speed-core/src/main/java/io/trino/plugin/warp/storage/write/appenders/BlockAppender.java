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
import io.trino.plugin.warp.storage.juffers.CrcJuffer;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;

import java.nio.ByteBuffer;

public abstract class BlockAppender
{
    protected static final byte NULL_VALUE_BYTE_SIGNAL = -1;
    protected static final byte NON_NULL_VALUE_BYTE_SIGNAL = 0;
    protected static final byte ZERO_BYTE_SIGNAL = 0;
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
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        AppendResult result = appendValues(jufferPos, blockPos, warmUpElement, warmupElementStatsBuilder);
        warmupElementStatsBuilder.incNullCount(result.nullsCount());
        return result;
    }

    abstract AppendResult appendValues(
            int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder);

    protected AppendResult appendFromMapBlock(BlockPosHolder blockPos, int jufferPos, Object key)
    {
        throw new UnsupportedOperationException();
    }
}
