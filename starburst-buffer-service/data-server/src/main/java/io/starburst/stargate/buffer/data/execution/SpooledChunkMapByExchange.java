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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;

import java.nio.charset.StandardCharsets;
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

    public Slice encodeMetadataSlice()
    {
        int metadataFileSize = 0;
        for (Map.Entry<String, Map<Long, SpooledChunk>> entry : spooledChunkMapByExchange.entrySet()) {
            Map<Long, SpooledChunk> spooledChunkMap = entry.getValue();
            for (Map.Entry<Long, SpooledChunk> secondaryEntry : spooledChunkMap.entrySet()) {
                metadataFileSize += Long.BYTES;
                SpooledChunk spooledChunk = secondaryEntry.getValue();
                metadataFileSize += Integer.BYTES;
                metadataFileSize += spooledChunk.location().length();
                metadataFileSize += Long.BYTES;
                metadataFileSize += Integer.BYTES;
            }
        }
        Slice slice = Slices.allocate(metadataFileSize);
        SliceOutput sliceOutput = slice.getOutput();
        for (Map.Entry<String, Map<Long, SpooledChunk>> entry : spooledChunkMapByExchange.entrySet()) {
            Map<Long, SpooledChunk> spooledChunkMap = entry.getValue();
            for (Map.Entry<Long, SpooledChunk> secondaryEntry : spooledChunkMap.entrySet()) {
                Long chunkId = secondaryEntry.getKey();
                sliceOutput.writeLong(chunkId);
                SpooledChunk spooledChunk = secondaryEntry.getValue();
                sliceOutput.writeInt(spooledChunk.location().length());
                sliceOutput.writeBytes(spooledChunk.location().getBytes(StandardCharsets.UTF_8));
                sliceOutput.writeLong(spooledChunk.offset());
                sliceOutput.writeInt(spooledChunk.length());
            }
        }
        return slice;
    }
}
