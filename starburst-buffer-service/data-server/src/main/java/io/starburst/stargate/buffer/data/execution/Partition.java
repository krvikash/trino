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
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.spooling.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class Partition
{
    private final long bufferNodeId;
    private final String exchangeId;
    private final int partitionId;
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final int chunkMaxSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final int chunkSpoolConcurrency;
    private final ChunkIdGenerator chunkIdGenerator;
    private final ExecutorService executor;

    private final Map<Long, Chunk> closedChunks = new ConcurrentHashMap<>();
    private final Object spoolLock = new Object();
    @GuardedBy("this")
    private final Map<TaskAttemptId, Long> lastDataPagesIds = new HashMap<>();
    @GuardedBy("this")
    private volatile Chunk openChunk;
    @GuardedBy("this")
    private boolean finished;
    @GuardedBy("this")
    private long lastConsumedChunkId = -1;
    @GuardedBy("this")
    private final Deque<AddDataPagesFuture> addDataPagesFutures = new ArrayDeque<>();
    @GuardedBy("this")
    private boolean released;

    public Partition(
            long bufferNodeId,
            String exchangeId,
            int partitionId,
            MemoryAllocator memoryAllocator,
            SpoolingStorage spoolingStorage,
            int chunkMaxSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum,
            int chunkSpoolConcurrency,
            ChunkIdGenerator chunkIdGenerator,
            ExecutorService executor)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.partitionId = partitionId;
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.chunkMaxSizeInBytes = chunkMaxSizeInBytes;
        this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        this.chunkSpoolConcurrency = chunkSpoolConcurrency;
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");
        this.executor = requireNonNull(executor, "executor is null");

        this.openChunk = createNewOpenChunk();
    }

    public synchronized ListenableFuture<Void> addDataPages(int taskId, int attemptId, long dataPagesId, List<Slice> pages)
    {
        TaskAttemptId taskAttemptId = new TaskAttemptId(taskId, attemptId);
        long lastDataPagesId = lastDataPagesIds.getOrDefault(taskAttemptId, -1L);
        checkArgument(dataPagesId >= lastDataPagesId,
                "dataPagesId should not decrease for the same writer: " +
                        "taskId %d, attemptId %d, dataPagesId %d, lastDataPagesId %d".formatted(taskId, attemptId, dataPagesId, lastDataPagesId));
        if (dataPagesId == lastDataPagesId) {
            return immediateVoidFuture();
        }
        else {
            lastDataPagesIds.put(taskAttemptId, dataPagesId);
        }

        AddDataPagesFuture addDataPagesFuture = new AddDataPagesFuture(taskId, attemptId, pages);
        addDataPagesFutures.add(addDataPagesFuture);
        if (addDataPagesFutures.size() == 1) {
            addDataPagesFuture.process();
        }

        return addDataPagesFuture;
    }

    public ChunkDataLease getChunkData(long chunkId, long bufferNodeId)
    {
        Chunk chunk = closedChunks.get(chunkId);
        if (chunk == null || chunk.getChunkData() == null) {
            // chunk already spooled
            return spoolingStorage.readChunk(exchangeId, chunkId, bufferNodeId);
        }
        // TODO: memory account inaccuracy exists here: getChunkData and spooling can happen concurrently.
        // ChunkDataLease can hold a reference to ChunkData after spooling releases the chunk early.
        return ChunkDataLease.immediate(chunk.getChunkData());
    }

    public synchronized void getNewlyClosedChunkHandles(ImmutableList.Builder<ChunkHandle> newlyClosedChunkHandles)
    {
        for (Chunk chunk : closedChunks.values()) {
            long chunkId = chunk.getChunkId();
            if (chunkId > lastConsumedChunkId) {
                lastConsumedChunkId = chunkId;
                newlyClosedChunkHandles.add(chunk.getHandle());
            }
        }
    }

    public synchronized void finish()
    {
        checkState(!finished, "already finished");
        checkState(addDataPagesFutures.isEmpty(), "finish() called when addDataPages is in progress");
        checkState(openChunk != null, "No open chunk exists for exchange %s partition %d".formatted(exchangeId, partitionId));
        closeChunk(openChunk);
        openChunk = null;
        finished = true;
    }

    public boolean hasOpenChunk()
    {
        return openChunk != null;
    }

    public int getClosedChunks()
    {
        return (int) closedChunks.values().stream().filter(chunk -> chunk.getChunkData() != null).count();
    }

    public void releaseChunks()
    {
        synchronized (this) {
            addDataPagesFutures.forEach(future -> future.cancel(true));
            if (openChunk != null) {
                openChunk.release();
            }
            released = true;
        }
        closedChunks.values().forEach(Chunk::release);
    }

    // TODO: consider concurrent spooling on the exchange level to increase parallelism
    public void spool(Predicate<MemoryAllocator> stopCriteria)
    {
        // at most one spool can be in progress
        synchronized (spoolLock) {
            Iterator<Map.Entry<Long, Chunk>> iterator = closedChunks.entrySet().iterator();
            while (iterator.hasNext()) {
                List<Chunk> chunks = new ArrayList<>();
                ImmutableList.Builder<ListenableFuture<Void>> spoolFutures = ImmutableList.builder();
                while (chunks.size() < chunkSpoolConcurrency && iterator.hasNext()) {
                    Chunk chunk = iterator.next().getValue();
                    ChunkDataHolder chunkData = chunk.getChunkData();
                    if (chunkData == null) {
                        // already spooled
                        continue;
                    }
                    chunks.add(chunk);
                    spoolFutures.add(spoolingStorage.writeChunk(exchangeId, chunk.getChunkId(), bufferNodeId, chunkData));
                }

                getFutureValue(allAsList(spoolFutures.build()));
                chunks.forEach(Chunk::release);

                if (stopCriteria.test(memoryAllocator)) {
                    return;
                }
            }
        }
    }

    public synchronized void closeOpenChunkAndSpool()
    {
        if (!addDataPagesFutures.isEmpty()) {
            // we have pending writes, closing the open chunk will cause loss of data
            return;
        }

        Chunk chunk = openChunk;
        closeChunk(chunk);
        if (chunk.dataSizeInBytes() > 0) {
            getFutureValue(spoolingStorage.writeChunk(exchangeId, chunk.getChunkId(), bufferNodeId, chunk.getChunkData()));
        }
        chunk.release();

        openChunk = createNewOpenChunk();
    }

    @GuardedBy("this")
    private void closeChunk(Chunk chunk)
    {
        chunk.close();
        // ignore empty chunks
        if (chunk.dataSizeInBytes() > 0) {
            closedChunks.put(chunk.getChunkId(), chunk);
        }
    }

    @GuardedBy("this")
    private Chunk createNewOpenChunk()
    {
        checkState(!released, "new chunk creation after release of all chunks");
        long chunkId = chunkIdGenerator.getNextChunkId();
        return new Chunk(
                bufferNodeId,
                partitionId,
                chunkId,
                memoryAllocator,
                executor,
                chunkMaxSizeInBytes,
                chunkSliceSizeInBytes,
                calculateDataPagesChecksum);
    }

    private record TaskAttemptId(
            int taskId,
            int attemptId) {
        public TaskAttemptId {
            checkArgument(taskId <= Short.MAX_VALUE, "taskId %s larger than %s", taskId, Short.MAX_VALUE);
            checkArgument(attemptId <= Byte.MAX_VALUE, "attemptId %s larger than %s", attemptId, Byte.MAX_VALUE);
        }
    }

    private class AddDataPagesFuture
            extends AbstractFuture<Void>
    {
        private final int taskId;
        private final int attemptId;
        private final Iterator<Slice> pages;

        @GuardedBy("Partition.this")
        private ListenableFuture<Void> currentChunkWriteFuture = immediateVoidFuture();

        AddDataPagesFuture(
                int taskId,
                int attemptId,
                List<Slice> pages)
        {
            this.taskId = taskId;
            this.attemptId = attemptId;
            this.pages = requireNonNull(pages, "pages is null").iterator();
            checkArgument(!pages.isEmpty(), "empty pages");
        }

        public void process()
        {
            synchronized (Partition.this) {
                if (currentChunkWriteFuture == null) {
                    checkState(isCancelled(), "PartitionAddDataPagesFuture should be in cancelled state");
                }
                checkState(currentChunkWriteFuture.isDone(), "trying to process next page before previous page is done");

                Slice page = pages.next();
                if (!openChunk.hasEnoughSpace(page)) {
                    // the open chunk doesn't have enough space available, close the chunk and create a new one
                    closeChunk(openChunk);
                    openChunk = createNewOpenChunk();
                }

                currentChunkWriteFuture = openChunk.write(taskId, attemptId, page);
                Futures.addCallback(
                        currentChunkWriteFuture,
                        new FutureCallback<>()
                        {
                            @Override
                            public void onSuccess(Void result)
                            {
                                try {
                                    boolean completeFuture = false;
                                    synchronized (Partition.this) {
                                        if (!pages.hasNext()) {
                                            addDataPagesFutures.removeFirst();
                                            if (!addDataPagesFutures.isEmpty()) {
                                                addDataPagesFutures.peek().process();
                                            }
                                            completeFuture = true;
                                        }
                                        else {
                                            process();
                                        }
                                    }
                                    // complete future outside the lock
                                    if (completeFuture) {
                                        set(null);
                                    }
                                }
                                catch (Exception e) {
                                    onFailure(e);
                                }
                            }

                            @Override
                            public void onFailure(Throwable throwable)
                            {
                                setException(throwable);
                            }
                        },
                        executor);
            }
        }

        @Override
        protected void interruptTask()
        {
            synchronized (Partition.this) {
                if (currentChunkWriteFuture != null) {
                    currentChunkWriteFuture.cancel(true);
                    currentChunkWriteFuture = null;
                }
            }
        }
    }
}
