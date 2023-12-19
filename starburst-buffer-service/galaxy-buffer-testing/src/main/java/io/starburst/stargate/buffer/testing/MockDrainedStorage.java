/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import io.starburst.stargate.buffer.data.client.DataPage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MockDrainedStorage
{
    private final Map<String, Map<ChunkKey, List<DataPage>>> drainedChunks = new HashMap<>();

    public synchronized void flush()
    {
        drainedChunks.clear();
    }

    public synchronized void flushExchange(String exchangeId)
    {
        drainedChunks.remove(exchangeId);
    }

    public synchronized void addChunk(String exchangeId, int partitionId, long chunkId, long bufferNodeId, List<DataPage> data)
    {
        drainedChunks.computeIfAbsent(exchangeId, ignored -> new HashMap<>()).put(new ChunkKey(bufferNodeId, partitionId, chunkId), data);
    }

    public synchronized Optional<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        Map<ChunkKey, List<DataPage>> exchangeChunks = drainedChunks.get(exchangeId);
        if (exchangeChunks == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(exchangeChunks.get(new ChunkKey(bufferNodeId, partitionId, chunkId)));
    }
}
