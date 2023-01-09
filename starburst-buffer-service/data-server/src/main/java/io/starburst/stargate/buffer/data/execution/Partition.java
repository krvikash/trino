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
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_DRAINED;
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
    private final ChunkIdGenerator chunkIdGenerator;
    private final ExecutorService executor;

    private final Map<Long, Chunk> closedChunks = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Map<TaskAttemptId, Long> lastDataPagesIds = new HashMap<>();
    @GuardedBy("this")
    private volatile Chunk openChunk;
    @GuardedBy("this")
    private boolean finished;
    @GuardedBy("this")
    private long lastConsumedChunkId = -1L;
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
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");
        this.executor = requireNonNull(executor, "executor is null");

        this.openChunk = createNewOpenChunk();
    }

    public int getPartitionId()
    {
        return partitionId;
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

    public ChunkDataResult getChunkData(long bufferNodeId, long chunkId, boolean startedDraining)
    {
        Chunk chunk = closedChunks.get(chunkId);
        ChunkDataHolder chunkDataHolder = (chunk == null ? null : chunk.getChunkData());
        if (chunkDataHolder == null) {
            if (startedDraining) {
                // chunk already drained
                throw new DataServerException(CHUNK_DRAINED, "Chunk %d already drained on node %d".formatted(chunkId, bufferNodeId));
            }
            // chunk already spooled
            return ChunkDataResult.of(spoolingStorage.getSpoolingFile(bufferNodeId, exchangeId, chunkId));
        }
        // TODO: memory account inaccuracy exists here: getChunkData and spooling can happen concurrently.
        // ChunkDataLease can hold a reference to ChunkData after spooling releases the chunk early.
        return ChunkDataResult.of(chunkDataHolder);
    }

    public synchronized void getNewlyClosedChunkHandles(ImmutableList.Builder<ChunkHandle> newlyClosedChunkHandles)
    {
        long maxChunkId = lastConsumedChunkId;
        for (Chunk chunk : closedChunks.values()) {
            long chunkId = chunk.getChunkId();
            if (chunkId > lastConsumedChunkId) {
                maxChunkId = Math.max(maxChunkId, chunkId);
                newlyClosedChunkHandles.add(chunk.getHandle());
            }
        }
        lastConsumedChunkId = maxChunkId;
    }

    public void finish()
    {
        List<AddDataPagesFuture> futuresToBeCancelled;
        synchronized (this) {
            // it's possible for finish() to be called multiple times due to retries, or we finish an exchange after it's drained
            if (finished) {
                return;
            }
            finished = true;
            futuresToBeCancelled = ImmutableList.copyOf(addDataPagesFutures);
        }
        // it's possible for finish() to be called when we have writes in progress, we simply cancel such writes
        futuresToBeCancelled.forEach(future -> future.cancel(true));
        synchronized (this) {
            checkState(openChunk != null, "No open chunk exists for exchange %s partition %d".formatted(exchangeId, partitionId));
            closeChunk(openChunk);
            openChunk = null;
        }
    }

    public boolean hasOpenChunk()
    {
        return openChunk != null;
    }

    public int getClosedChunksCount()
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
        openChunk = createNewOpenChunk();

        return Optional.of(chunk);
    }

    @GuardedBy("this")
    private void closeChunk(Chunk chunk)
    {
        // ignored empty chunks
        if (chunk.isEmpty()) {
            chunk.release();
        }
        else {
            chunk.close();
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
                exchangeId,
                partitionId,
                chunkId,
                memoryAllocator,
                executor,
                chunkMaxSizeInBytes,
                chunkSliceSizeInBytes,
                calculateDataPagesChecksum);
    }

    public Collection<Chunk> getClosedChunks()
    {
        return ImmutableList.copyOf(closedChunks.values());
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
            ListenableFuture<Void> futureToBeCancelled;
            synchronized (Partition.this) {
                futureToBeCancelled = currentChunkWriteFuture;
                currentChunkWriteFuture = null;
            }
            if (futureToBeCancelled != null) {
                futureToBeCancelled.cancel(true);
            }
        }
    }
}
