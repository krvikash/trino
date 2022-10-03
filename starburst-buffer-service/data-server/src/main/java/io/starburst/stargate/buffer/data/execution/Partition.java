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
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class Partition
{
    private final long bufferNodeId;
    private final String exchangeId;
    private final int partitionId;
    private final MemoryAllocator memoryAllocator;
    private final int chunkSizeInBytes;
    private final ChunkIdGenerator chunkIdGenerator;

    // chunkId -> closed chunk
    private final Map<Long, Chunk> closedChunks = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Map<TaskAttemptId, Long> lastDataPagesIds = new HashMap<>();
    @GuardedBy("this")
    private volatile Chunk openChunk;
    @GuardedBy("this")
    private boolean finished;

    public Partition(
            long bufferNodeId,
            String exchangeId,
            int partitionId,
            MemoryAllocator memoryAllocator,
            int chunkSizeInBytes,
            ChunkIdGenerator chunkIdGenerator)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.partitionId = partitionId;
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");
    }

    public synchronized void addDataPages(int taskId, int attemptId, long dataPagesId, List<Slice> pages)
    {
        TaskAttemptId taskAttemptId = new TaskAttemptId(taskId, attemptId);
        long lastDataPagesId = lastDataPagesIds.getOrDefault(taskAttemptId, -1L);
        checkArgument(dataPagesId >= lastDataPagesId,
                "dataPagesId should not decrease for the same writer: " +
                        "taskId %d, attemptId %d, dataPagesId %d, lastDataPagesId %d".formatted(taskId, attemptId, dataPagesId, lastDataPagesId));
        if (dataPagesId == lastDataPagesId) {
            return;
        }
        else {
            lastDataPagesIds.put(taskAttemptId, dataPagesId);
        }

        if (openChunk == null) {
            openChunk = createNewOpenChunk();
        }

        for (Slice page : pages) {
            if (!openChunk.hasEnoughSpace(page)) {
                // the open chunk doesn't have enough space available, close the chunk and create a new one
                closeChunk(openChunk);
                openChunk = createNewOpenChunk();
            }
            openChunk.write(taskId, attemptId, page);
        }
    }

    public List<DataPage> getChunkData(long chunkId)
    {
        Chunk chunk = closedChunks.get(chunkId);
        if (chunk == null) {
            throw new DataServerException(CHUNK_NOT_FOUND, "No closed chunk found for exchange %s, partition %d, chunk %d".formatted(exchangeId, partitionId, chunkId));
        }
        return chunk.readAll();
    }

    public void addNewlyClosedChunkHandles(ImmutableList.Builder<ChunkHandle> newlyClosedChunkHandles, Set<Long> consumedChunks)
    {
        for (Chunk chunk : closedChunks.values()) {
            long chunkId = chunk.getChunkId();
            if (!consumedChunks.contains(chunkId)) {
                consumedChunks.add(chunkId);
                newlyClosedChunkHandles.add(chunk.getHandle());
            }
        }
    }

    public synchronized void finish()
    {
        checkState(!finished, "already finished");
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
        return closedChunks.size();
    }

    public void releaseChunks()
    {
        synchronized (this) {
            if (openChunk != null) {
                openChunk.release();
            }
        }
        closedChunks.values().forEach(Chunk::release);
    }

    private void closeChunk(Chunk chunk)
    {
        chunk.close();
        closedChunks.put(chunk.getChunkId(), chunk);
    }

    private Chunk createNewOpenChunk()
    {
        long chunkId = chunkIdGenerator.getNextChunkId();
        return new Chunk(bufferNodeId, partitionId, chunkId, memoryAllocator, chunkSizeInBytes);
    }

    private record TaskAttemptId(
            int taskId,
            int attemptId) {
        public TaskAttemptId {
            checkArgument(taskId <= Short.MAX_VALUE, "taskId %s larger than %s", taskId, Short.MAX_VALUE);
            checkArgument(attemptId <= Byte.MAX_VALUE, "attemptId %s larger than %s", attemptId, Byte.MAX_VALUE);
        }
    }
}
