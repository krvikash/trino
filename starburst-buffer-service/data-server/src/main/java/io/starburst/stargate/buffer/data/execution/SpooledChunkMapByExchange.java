/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.errorprone.annotations.ThreadSafe;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class SpooledChunkMapByExchange
{
    // exchangeId -> chunkId -> spooledChunk
    private final Map<String, Map<Long, SpooledChunk>> spooledChunkMapByExchange = new ConcurrentHashMap<>();

    public Optional<SpooledChunk> getSpooledChunk(String exchangeId, long chunkId)
    {
        Map<Long, SpooledChunk> spooledChunkMap = spooledChunkMapByExchange.get(exchangeId);
        if (spooledChunkMap != null) {
            return Optional.ofNullable(spooledChunkMap.get(chunkId));
        }
        return Optional.empty();
    }

    public void update(String exchangeId, Map<Long, SpooledChunk> spooledChunkMap)
    {
        spooledChunkMapByExchange.computeIfAbsent(exchangeId, ignored -> new ConcurrentHashMap<>()).putAll(spooledChunkMap);
    }

    public void removeExchange(String exchangeId)
    {
        spooledChunkMapByExchange.remove(exchangeId);
    }

    public int getSpooledChunks()
    {
        return spooledChunkMapByExchange.values().stream().mapToInt(Map::size).sum();
    }
}
