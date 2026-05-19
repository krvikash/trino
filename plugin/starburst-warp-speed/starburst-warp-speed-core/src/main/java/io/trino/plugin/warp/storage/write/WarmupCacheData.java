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

import io.trino.plugin.warp.dispatcher.cache.CacheWarmupElementArgs;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.block.Block;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class WarmupCacheData
{
    private static final int INSTANCE_SIZE = instanceSize(WarmupCacheData.class);
    private final ShapingLogger shapingLogger;

    private Map<Integer, List<CacheWarmupElementArgs>> connectorIndexToWarmColumns;

    public WarmupCacheData(
            Map<Integer, List<CacheWarmupElementArgs>> connectorIndexToWarmColumns,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.connectorIndexToWarmColumns = connectorIndexToWarmColumns;
        shapingLogger = requireNonNull(shapingLoggerFactory).getInstance(WarmupCacheData.class);
    }

    public List<CacheWarmupElementArgs> getCacheWarmupElementArgsList()
    {
        return isNull() ? emptyList() : connectorIndexToWarmColumns.values().stream().flatMap(Collection::stream).toList();
    }

    public List<CacheWarmupElementArgs> getCacheWarmupElementArgsList(int connectorBlockIndex)
    {
        if (isNull()) {
            throw new IllegalStateException("connectorIndexToWarmColumns is null because of revoke event");
        }
        return connectorIndexToWarmColumns.get(connectorBlockIndex);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getWarmupBlocksRetainedSizeInBytes();
    }

    private long getWarmupBlocksRetainedSizeInBytes()
    {
        if (isNull()) {
            return 0;
        }
        return connectorIndexToWarmColumns
                .values()
                .stream()
                .mapToLong(list -> list.stream()
                        .map(CacheWarmupElementArgs::getWarmupBlocksRetainedSizeInBytes)
                        .max(Long::compare) // All elements hold the same block instances, take the maximum because some may had processed more blocks than others
                        .orElse(0L))
                .sum();
    }

    public void clear()
    {
        connectorIndexToWarmColumns = null;
    }

    public Set<Integer> getConnectorColumnIndexes()
    {
        return isNull() ? emptySet() : connectorIndexToWarmColumns.keySet();
    }

    public boolean notAllDataFlushed()
    {
        return isNull() ||
                connectorIndexToWarmColumns
                        .values()
                        .stream()
                        .flatMap(Collection::stream)
                        .anyMatch(x -> !x.isEmpty());
    }

    /**
     * Add a {@code block} to every {@code CacheWarmupElementArgs} with the given {@code connectorBlockIndex}
     *
     * @param block Block to add to all warmup elements with the same connectorBlockIndex
     * @param connectorBlockIndex column index in page
     * @return if any warmup element is ready
     */
    public boolean addBlock(Block block, int connectorBlockIndex)
    {
        if (isNull() || !connectorIndexToWarmColumns.containsKey(connectorBlockIndex)) {
            return false;
        }
        for (CacheWarmupElementArgs cacheWarmupElementArgs : connectorIndexToWarmColumns.get(connectorBlockIndex)) {
            cacheWarmupElementArgs.addBlock(block);
        }
        return connectorIndexToWarmColumns.get(connectorBlockIndex).stream().anyMatch(CacheWarmupElementArgs::isReady);
    }

    public List<WarmingCandidate> getWarmingCandidates()
    {
        return isNull() ? emptyList() : connectorIndexToWarmColumns.values()
                                        .stream()
                                        .flatMap(Collection::stream)
                                        .map(CacheWarmupElementArgs::getWarmupCandidate)
                                        .filter(Objects::nonNull) // warming candidate can be null when CacheWarmer:initCandidate fails
                                        .collect(Collectors.toList());
    }

    private boolean isNull()
    {
        if (connectorIndexToWarmColumns == null) {
            shapingLogger.info("connectorIndexToWarmColumns is null because of revoke but requested to use it");
            return true;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "WarmupCacheData{" +
                "connectorIndexToWarmColumns=" + connectorIndexToWarmColumns +
                '}';
    }
}
