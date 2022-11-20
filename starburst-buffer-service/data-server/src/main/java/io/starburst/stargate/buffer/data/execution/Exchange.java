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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.spooling.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_CORRUPTED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_FINISHED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class Exchange
{
    private final long bufferNodeId;
    private final String exchangeId;
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final int chunkMaxSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final ChunkIdGenerator chunkIdGenerator;
    private final ExecutorService executor;

    // partitionId -> partition
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private OptionalLong nextPagingId = OptionalLong.of(0);
    @GuardedBy("this")
    private ChunkList lastChunkList;
    @GuardedBy("this")
    private long lastPagingId = -1;
    @GuardedBy("this")
    private boolean finished;
    private volatile long lastUpdateTime;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public Exchange(
            long bufferNodeId,
            String exchangeId,
            MemoryAllocator memoryAllocator,
            SpoolingStorage spoolingStorage,
            int chunkMaxSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum,
            ChunkIdGenerator chunkIdGenerator,
            ExecutorService executor,
            long currentTime)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.chunkMaxSizeInBytes = chunkMaxSizeInBytes;
        this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");
        this.executor = requireNonNull(executor, "executor is null");

        this.lastUpdateTime = currentTime;
    }

    public ListenableFuture<Void> addDataPages(int partitionId, int taskId, int attemptId, long dataPagesId, List<Slice> pages)
    {
        throwIfFailed();

        Partition partition;
        synchronized (this) {
            if (finished) {
                throw new DataServerException(EXCHANGE_FINISHED, "exchange %s already finished".formatted(exchangeId));
            }
            partition = partitions.computeIfAbsent(partitionId, ignored -> new Partition(
                    bufferNodeId,
                    exchangeId,
                    partitionId,
                    memoryAllocator,
                    spoolingStorage,
                    chunkMaxSizeInBytes,
                    chunkSliceSizeInBytes,
                    calculateDataPagesChecksum,
                    chunkIdGenerator,
                    executor));
        }

        ListenableFuture<Void> addDataPagesFuture = partition.addDataPages(taskId, attemptId, dataPagesId, pages);
        addExceptionCallback(addDataPagesFuture, throwable -> {
            failure.compareAndSet(null, throwable);
            this.releaseChunks();
        }, executor);
        return addDataPagesFuture;
    }

    public ChunkDataLease getChunkData(int partitionId, long chunkId, long bufferNodeId)
    {
        throwIfFailed();

        Partition partition = partitions.get(partitionId);
        if (partition == null) {
            throw new DataServerException(CHUNK_NOT_FOUND, "partition %d not found for exchange %s".formatted(partitionId, exchangeId));
        }
        return partition.getChunkData(chunkId, bufferNodeId);
    }

    public synchronized ChunkList listClosedChunks(OptionalLong pagingIdOptional)
    {
        throwIfFailed();

        long pagingId = pagingIdOptional.orElse(0);
        Optional<ChunkList> cachedChunkList = getCachedChunkList(pagingId);
        if (cachedChunkList.isPresent()) {
            return cachedChunkList.get();
        }

        verify(nextPagingId.isPresent(), "Cannot generate next list of closed chunks when nextPagingId is not present");
        verify(pagingId == nextPagingId.getAsLong(), "Expected pagingId to equal next pagingId");

        ImmutableList.Builder<ChunkHandle> newlyClosedChunkHandles = ImmutableList.builder();
        for (Partition partition : partitions.values()) {
            partition.getNewlyClosedChunkHandles(newlyClosedChunkHandles);
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
        throwIfFailed();

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

    public synchronized void releaseChunks()
    {
        partitions.values().forEach(Partition::releaseChunks);
        partitions.clear();
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

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throw new DataServerException(EXCHANGE_CORRUPTED, "exchange %s is in inconsistent state".formatted(exchangeId), throwable);
        }
    }
}
