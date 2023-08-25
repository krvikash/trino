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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class Partition
{
    private static final Logger log = Logger.get(Partition.class);

    private final long bufferNodeId;
    private final String exchangeId;
    private final int partitionId;
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final int chunkTargetSizeInBytes;
    private final int chunkMaxSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final ChunkIdGenerator chunkIdGenerator;
    private final ExecutorService executor;
    private final Consumer<ChunkHandle> closedChunkConsumer;

    private final Map<Long, Chunk> closedChunks = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Map<TaskAttemptId, Long> lastDataPagesIds = new HashMap<>();
    @GuardedBy("this")
    private final Map<TaskAttemptId, ListenableFuture<Void>> currentFutures = new HashMap<>();
    @GuardedBy("this")
    private volatile Chunk openChunk;
    @GuardedBy("this")
    private ListenableFuture<Void> finishFuture;
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
            int chunkTargetSizeInBytes,
            int chunkMaxSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum,
            ChunkIdGenerator chunkIdGenerator,
            ExecutorService executor,
            Consumer<ChunkHandle> closedChunkConsumer)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.partitionId = partitionId;
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.chunkTargetSizeInBytes = chunkTargetSizeInBytes;
        this.chunkMaxSizeInBytes = chunkMaxSizeInBytes;
        this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.closedChunkConsumer = requireNonNull(closedChunkConsumer, "closedChunkConsumer is null");

        this.openChunk = createNewOpenChunk(chunkTargetSizeInBytes);
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public synchronized AddDataPagesResult addDataPages(int taskId, int attemptId, long dataPagesId, List<Slice> pages)
    {
        checkState(finishFuture == null, "exchange %s partition %s already finished", exchangeId, partitionId);

        TaskAttemptId taskAttemptId = new TaskAttemptId(taskId, attemptId);
        long lastDataPagesId = lastDataPagesIds.getOrDefault(taskAttemptId, -1L);
        checkArgument(dataPagesId >= lastDataPagesId,
                "dataPagesId should not decrease for the same writer: " +
                        "taskId %d, attemptId %d, dataPagesId %d, lastDataPagesId %d".formatted(taskId, attemptId, dataPagesId, lastDataPagesId));
        if (dataPagesId == lastDataPagesId) {
            return new AddDataPagesResult(currentFutures.getOrDefault(taskAttemptId, immediateVoidFuture()), false);
        }
        lastDataPagesIds.put(taskAttemptId, dataPagesId);

        AddDataPagesFuture addDataPagesFuture = new AddDataPagesFuture(taskId, attemptId, pages);
        currentFutures.put(taskAttemptId, addDataPagesFuture);
        addDataPagesFuture.addListener(() -> {
            synchronized (Partition.this) {
                ListenableFuture<Void> currentFuture = currentFutures.get(taskAttemptId);
                if (currentFuture == addDataPagesFuture) {
                    currentFutures.remove(taskAttemptId);
                }
            }
        }, executor);

        addDataPagesFutures.add(addDataPagesFuture);
        if (addDataPagesFutures.size() == 1) {
            addDataPagesFuture.process();
        }

        return new AddDataPagesResult(addDataPagesFuture, true);
    }

    public ChunkDataResult getChunkData(long bufferNodeId, long chunkId)
    {
        Chunk chunk = closedChunks.get(chunkId);
        ChunkDataLease chunkDataLease = (chunk == null ? null : chunk.getChunkDataLease());
        if (chunkDataLease == null) {
            // chunk already spooled
            return ChunkDataResult.of(spoolingStorage.getSpooledChunk(bufferNodeId, exchangeId, chunkId));
        }
        return ChunkDataResult.of(chunkDataLease);
    }

    public synchronized ListenableFuture<Void> finish()
    {
        if (finishFuture != null) {
            return finishFuture;
        }

        ListenableFuture<Void> inProgressAddDataPagesFuture;
        if (!addDataPagesFutures.isEmpty()) {
            // When we finish a partition while there are still in-progress writes,
            // we need to guarantee that the chunk won't get corrupted
            inProgressAddDataPagesFuture = addDataPagesFutures.poll();
            if (!addDataPagesFutures.isEmpty()) {
                // addDataPagesFutures execute sequentially, so we can safely early complete the rest of futures
                log.info("Early completing %d addDataPagesFutures for exchange %s, partition %d".formatted(addDataPagesFutures.size(), exchangeId, partitionId));
                addDataPagesFutures.forEach(AddDataPagesFuture::complete);
                addDataPagesFutures.clear();
            }
        }
        else {
            inProgressAddDataPagesFuture = immediateVoidFuture();
        }

        finishFuture = Futures.transform(
                inProgressAddDataPagesFuture,
                ignored -> {
                    synchronized (this) {
                        checkState(openChunk != null, "No open chunk exists for exchange %s partition %d".formatted(exchangeId, partitionId));
                        closeChunk(openChunk);
                        openChunk = null;
                    }
                    return null;
                },
                executor);

        return finishFuture;
    }

    public boolean hasOpenChunk()
    {
        return openChunk != null;
    }

    public int getClosedChunksCount()
    {
        return (int) closedChunks.values().stream().filter(Chunk::chunkDataInMemory).count();
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

    public synchronized Optional<Chunk> closeOpenChunkAndGet()
    {
        if (openChunk == null) {
            // partition already closed
            return Optional.empty();
        }
        if (!addDataPagesFutures.isEmpty()) {
            // we have pending writes, closing the open chunk will cause loss of data
            return Optional.empty();
        }
        if (openChunk.isEmpty()) {
            return Optional.empty();
        }

        Chunk chunk = openChunk;
        closeChunk(chunk);
        openChunk = createNewOpenChunk(chunkTargetSizeInBytes);

        return Optional.of(chunk);
    }

    @GuardedBy("this")
    private void closeChunk(Chunk chunk)
    {
        // ignore empty chunks
        if (chunk.isEmpty()) {
            chunk.release();
        }
        else {
            chunk.close();
            closedChunks.put(chunk.getChunkId(), chunk);
            closedChunkConsumer.accept(chunk.getHandle());
        }
    }

    @GuardedBy("this")
    private Chunk createNewOpenChunk(int chunkSizeInBytes)
    {
        checkState(!released, "new chunk creation after release of all chunks");
        long chunkId = chunkIdGenerator.getNextChunkId();
        return new Chunk(
                bufferNodeId,
                exchangeId,
                partitionId,
                chunkId,
                memoryAllocator,
                executor,
                chunkSizeInBytes,
                chunkSliceSizeInBytes,
                calculateDataPagesChecksum);
    }

    public Collection<Chunk> getClosedChunks()
    {
        return ImmutableList.copyOf(closedChunks.values());
    }

    private record TaskAttemptId(
            int taskId,
            int attemptId)
    {
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

        @GuardedBy("Partition.this")
        public void process()
        {
            if (currentChunkWriteFuture == null) {
                checkState(isCancelled(), "PartitionAddDataPagesFuture should be in cancelled state");
                return;
            }
            checkState(currentChunkWriteFuture.isDone(), "trying to process next page before previous page is done");

            Slice page = pages.next();
            int requiredStorageSize = DATA_PAGE_HEADER_SIZE + page.length();
            if (!openChunk.hasEnoughSpace(requiredStorageSize)) {
                // the open chunk doesn't have enough space available, close the chunk and create a new one
                closeChunk(openChunk);

                if (requiredStorageSize <= chunkTargetSizeInBytes) {
                    openChunk = createNewOpenChunk(chunkTargetSizeInBytes);
                }
                else if (requiredStorageSize <= chunkMaxSizeInBytes) {
                    openChunk = createNewOpenChunk(chunkMaxSizeInBytes);
                }
                else {
                    setException(new IllegalArgumentException("requiredStorageSize %d larger than chunkMaxSizeInBytes %d".formatted(requiredStorageSize, chunkMaxSizeInBytes)));
                    return;
                }
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
                                    if (addDataPagesFutures.isEmpty()) {
                                        // meaning partition already finished, we can terminate early
                                        completeFuture = true;
                                    }
                                    else if (!pages.hasNext()) {
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

        public void complete()
        {
            set(null);
        }

        @Override
        protected void interruptTask()
        {
            ListenableFuture<Void> futureToBeCancelled;
            synchronized (Partition.this) {
                futureToBeCancelled = currentChunkWriteFuture;
                currentChunkWriteFuture = null;
            }
            if (futureToBeCancelled != null) {
                futureToBeCancelled.cancel(true);
                synchronized (Partition.this) {
                    addDataPagesFutures.removeFirst();
                    if (!addDataPagesFutures.isEmpty()) {
                        addDataPagesFutures.peek().process();
                    }
                }
            }
        }
    }
}
