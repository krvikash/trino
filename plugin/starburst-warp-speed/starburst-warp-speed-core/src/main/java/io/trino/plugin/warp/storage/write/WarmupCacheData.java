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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.dispatcher.cache.WarmupElementBlocks;
import io.trino.spi.block.Block;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;

public class WarmupCacheData
{
    private static final int INSTANCE_SIZE = instanceSize(WarmupCacheData.class);

    private final List<WarmupElementBlocks> warmupElementBlocksList;

    public WarmupCacheData(List<WarmupElementBlocks> warmupElementBlocksList)
    {
        this.warmupElementBlocksList = warmupElementBlocksList;
    }

    public WarmupElementBlocks getWarmupElementBlock(int index)
    {
        return warmupElementBlocksList.get(index);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getWarmupBlocksRetainedSizeInBytes();
    }

    private long getWarmupBlocksRetainedSizeInBytes()
    {
        long totalRetainedSizeInBytes = 0;
        for (WarmupElementBlocks warmupElementBlocks : warmupElementBlocksList) {
            if (warmupElementBlocks != null) {
                totalRetainedSizeInBytes += Integer.SIZE + warmupElementBlocks.getRetainedSizeInBytes();
            }
        }
        return totalRetainedSizeInBytes;
    }

    public void clear()
    {
        warmupElementBlocksList.clear();
    }

    public int size()
    {
        return warmupElementBlocksList.size();
    }

    public boolean notAllDataFlushed()
    {
        return warmupElementBlocksList.stream().anyMatch(x -> !x.isEmpty());
    }

    public boolean addBlock(Block block, int blockIndex)
    {
        return warmupElementBlocksList.get(blockIndex).add(block);
    }

    @Override
    public String toString()
    {
        return "WarmupCacheData{" +
                "warmupElementBlocksList=" + warmupElementBlocksList +
                '}';
    }
}
