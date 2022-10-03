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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Verify.verify;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_FINISHED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class Exchange
{
    private final long bufferNodeId;
    private final String exchangeId;
    private final MemoryAllocator memoryAllocator;
    private final int chunkSizeInBytes;
    private final ChunkIdGenerator chunkIdGenerator;

    // partitionId -> partition
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private OptionalLong nextPagingId = OptionalLong.of(0);
    @GuardedBy("this")
    private ChunkList lastChunkList;
    @GuardedBy("this")
    private long lastPagingId = -1;
    @GuardedBy("this")
    private final LongSet consumedChunks = new LongArraySet();
    @GuardedBy("this")
    private boolean finished;
    private volatile long lastUpdateTime;

    public Exchange(
            long bufferNodeId,
            String exchangeId,
            MemoryAllocator memoryAllocator,
            int chunkSizeInBytes,
            ChunkIdGenerator chunkIdGenerator,
            long currentTime)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");

        this.lastUpdateTime = currentTime;
    }

    public void addDataPages(int partitionId, int taskId, int attemptId, long dataPagesId, List<Slice> pages)
    {
        Partition partition;
        synchronized (this) {
            if (finished) {
                throw new DataServerException(EXCHANGE_FINISHED, "exchange %s already finished".formatted(exchangeId));
            }
            partition = partitions.computeIfAbsent(partitionId, ignored -> new Partition(bufferNodeId, exchangeId, partitionId, memoryAllocator, chunkSizeInBytes, chunkIdGenerator));
        }

        partition.addDataPages(taskId, attemptId, dataPagesId, pages);
    }

    public List<DataPage> getChunkData(int partitionId, long chunkId)
    {
        Partition partition = partitions.get(partitionId);
        if (partition == null) {
            throw new DataServerException(CHUNK_NOT_FOUND, "partition %d not found for exchange %s".formatted(partitionId, exchangeId));
        }
        return partition.getChunkData(chunkId);
    }

    public synchronized ChunkList listClosedChunks(OptionalLong pagingIdOptional)
    {
        long pagingId = pagingIdOptional.orElse(0);
        Optional<ChunkList> cachedChunkList = getCachedChunkList(pagingId);
        if (cachedChunkList.isPresent()) {
            return cachedChunkList.get();
        }

        verify(nextPagingId.isPresent(), "Cannot generate next list of closed chunks when nextPagingId is not present");
        verify(pagingId == nextPagingId.getAsLong(), "Expected pagingId to equal next pagingId");

        ImmutableList.Builder<ChunkHandle> newlyClosedChunkHandles = ImmutableList.builder();
        for (Partition partition : partitions.values()) {
            partition.addNewlyClosedChunkHandles(newlyClosedChunkHandles, consumedChunks);
        }

        if (finished) {
            nextPagingId = OptionalLong.empty();
        }
        else {
            nextPagingId = OptionalLong.of(pagingId + 1);
        }
        ChunkList chunkList = new ChunkList(newlyClosedChunkHandles.build(), nextPagingId);

        // cache the chunk list
        lastPagingId = pagingId;
        lastChunkList = chunkList;

        return chunkList;
    }

    public synchronized void finish()
    {
        if (finished) {
            return;
        }

        for (Partition partition : partitions.values()) {
            partition.finish();
        }

        finished = true;
    }

    public int getOpenChunksCount()
    {
        return (int) partitions.values().stream().filter(Partition::hasOpenChunk).count();
    }

    public int getClosedChunksCount()
    {
        return partitions.values().stream().mapToInt(Partition::getClosedChunks).sum();
    }

    public void releaseChunks()
    {
        partitions.values().forEach(Partition::releaseChunks);
    }

    public long getLastUpdateTime()
    {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime)
    {
        this.lastUpdateTime = lastUpdateTime;
    }

    private Optional<ChunkList> getCachedChunkList(long pagingId)
    {
        // is this the first request
        if (lastChunkList == null) {
            return Optional.empty();
        }

        // is this a repeated request for the last chunk list
        if (pagingId == lastPagingId) {
            return Optional.of(lastChunkList);
        }

        // if this is a chunk list before the lastChunkList, the data is gone
        if (pagingId < lastPagingId) {
            throw new DataApiException(USER_ERROR, "Provided pagingId %d but lastPagingId is %d".formatted(pagingId, lastPagingId));
        }

        // if this is a request for a chunk list after the end of the stream, return not found
        if (nextPagingId.isEmpty()) {
            throw new DataApiException(USER_ERROR,
                    "Unexpected request pagingId %d after exchange %s finished and all chunk handles got acknowledged".formatted(pagingId, exchangeId));
        }

        // if this is not a request for the next chunk list, return not found
        if (pagingId != nextPagingId.getAsLong()) {
            // unknown pagingId
            throw new DataServerException(USER_ERROR, "pagingId %d does not equal nextPagingId %d".formatted(pagingId, nextPagingId.getAsLong()));
        }

        return Optional.empty();
    }
}
