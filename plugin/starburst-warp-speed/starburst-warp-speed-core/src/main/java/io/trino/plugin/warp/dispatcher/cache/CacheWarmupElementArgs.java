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
package io.trino.plugin.warp.dispatcher.cache;

import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.spi.block.Block;

public class CacheWarmupElementArgs
{
    private final WarmupElementWriteMetadata warmupElementWriteMetadata;
    private final WarmupElementBlocks warmupElementBlocks;
    private WarmingCandidate warmingCandidate;

    public CacheWarmupElementArgs(
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            WarmupElementBlocks warmupElementBlocks)
    {
        this.warmupElementWriteMetadata = warmupElementWriteMetadata;
        this.warmupElementBlocks = warmupElementBlocks;
    }

    public void setWarmingCandidate(WarmingCandidate warmingCandidate)
    {
        this.warmingCandidate = warmingCandidate;
    }

    public WarmupElementWriteMetadata getWarmupElementWriteMetadata()
    {
        return warmupElementWriteMetadata;
    }

    public void addBlock(Block block)
    {
        warmupElementBlocks.add(block);
    }

    public long getWarmupBlocksRetainedSizeInBytes()
    {
        return warmupElementBlocks.getRetainedSizeInBytes();
    }

    public boolean isEmpty()
    {
        return warmupElementBlocks.isEmpty();
    }

    public boolean isReady()
    {
        return warmupElementBlocks.isReady();
    }

    public WarmingCandidate getWarmupCandidate()
    {
        return warmingCandidate;
    }

    public WarmupElementBlocks getWarmupElementBlocks()
    {
        return warmupElementBlocks;
    }

    public int getConnectorBlockIndex()
    {
        return warmupElementWriteMetadata.connectorBlockIndex();
    }

    @Override
    public String toString()
    {
        return "CacheWarmupElementArgs{" +
                "warmupElementWriteMetadata=" + warmupElementWriteMetadata +
                ", warmupElementBlocks=" + warmupElementBlocks +
                ", warmingCandidate=" + warmingCandidate +
                '}';
    }
}
