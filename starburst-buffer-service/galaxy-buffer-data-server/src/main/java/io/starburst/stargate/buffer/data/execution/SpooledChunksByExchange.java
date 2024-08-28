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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class SpooledChunksByExchange
{
    // exchangeId -> chunkId -> spooledChunk
    private final Map<String, Map<Long, SpooledChunk>> mapping = new ConcurrentHashMap<>();
    private final AtomicBoolean frozen = new AtomicBoolean();
    private final ReadWriteLock freezeLock = new ReentrantReadWriteLock();

    public Optional<SpooledChunk> getSpooledChunk(String exchangeId, long chunkId)
    {
        Map<Long, SpooledChunk> spooledChunkMap = mapping.get(exchangeId);
        if (spooledChunkMap != null) {
            return Optional.ofNullable(spooledChunkMap.get(chunkId));
        }
        return Optional.empty();
    }

    public Map<String, Map<Long, SpooledChunk>> freeze()
    {
        Lock lock = freezeLock.writeLock();
        lock.lock();
        try {
            boolean success = frozen.compareAndSet(false, true);
            if (!success) {
                throw new AlreadyFrozenException("Spooled chunks map already frozen");
            }
            ImmutableMap.Builder<String, Map<Long, SpooledChunk>> frozenMapping = ImmutableMap.builder();

            mapping.forEach((k, v) -> {
                frozenMapping.put(k, ImmutableMap.copyOf(v));
            });
            return frozenMapping.buildOrThrow();
        }
        finally {
            lock.unlock();
        }
    }

    public void update(String exchangeId, Map<Long, SpooledChunk> spooledChunkMap)
    {
        Lock lock = freezeLock.readLock();
        lock.lock();
        try {
            if (frozen.get()) {
                throw new AlreadyFrozenException("Spooled chunks map already frozen");
            }
            mapping.computeIfAbsent(exchangeId, ignored -> new ConcurrentHashMap<>()).putAll(spooledChunkMap);
        }
        finally {
            lock.unlock();
        }
    }

    public void removeExchange(String exchangeId)
    {
        Lock lock = freezeLock.readLock();
        lock.lock();
        try {
            if (frozen.get()) {
                throw new AlreadyFrozenException("Spooled chunks map already frozen");
            }
            mapping.remove(exchangeId);
        }
        finally {
            lock.unlock();
        }
    }

    public int getSpooledChunksCount()
    {
        // no synchronization needed approximate value
        return mapping.values().stream().mapToInt(Map::size).sum();
    }

    public static Slice encodeMetadataSlice(Map<String, Map<Long, SpooledChunk>> mapping)
    {
        int metadataFileSize = 0;
        for (Map.Entry<String, Map<Long, SpooledChunk>> entry : mapping.entrySet()) {
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
        for (Map.Entry<String, Map<Long, SpooledChunk>> entry : mapping.entrySet()) {
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

    public static Map<Long, SpooledChunk> decodeMetadataSlice(Slice metadataSlice)
    {
        SliceInput sliceInput = metadataSlice.getInput();
        Map<Long, SpooledChunk> spooledChunkMap = new ConcurrentHashMap<>();
        while (sliceInput.isReadable()) {
            long chunkId = sliceInput.readLong();
            int locationLength = sliceInput.readInt();
            String location = sliceInput.readSlice(locationLength).toStringAscii();
            long offset = sliceInput.readLong();
            int length = sliceInput.readInt();

            spooledChunkMap.put(chunkId, new SpooledChunk(location, offset, length));
        }

        return spooledChunkMap;
    }

    public int size()
    {
        return mapping.size();
    }

    @VisibleForTesting
    void clear()
    {
        mapping.clear();
    }

    public static class AlreadyFrozenException
            extends RuntimeException
    {
        public AlreadyFrozenException(String message)
        {
            super(message);
        }
    }
}
