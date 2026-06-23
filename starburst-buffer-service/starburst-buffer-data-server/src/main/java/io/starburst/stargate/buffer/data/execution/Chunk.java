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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.execution.ChunkData.ChunkPlacement;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

// Note on thread-safety: only release(), getChunkDataLease(), getReclaimableBytes(), chunkPlacement(), and chunkDataInMemory()
// may be concurrently called after Chunk is closed; all are synchronized on Chunk
// This class is not thread safe
public class Chunk
{
    private final long bufferNodeId;
    private final String exchangeId;
    private final int partitionId;
    private final long chunkId;

    private ChunkData chunkData;
    private int dataSizeInBytes;
    private boolean closed;
    private ChunkHandle chunkHandle;

    public Chunk(
            long bufferNodeId,
            String exchangeId,
            int partitionId,
            long chunkId,
            ChunkData chunkData)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.partitionId = partitionId;
        this.chunkId = chunkId;
        this.chunkData = requireNonNull(chunkData, "chunkData is null");
    }

    // [test-only] placeholder for chunks
    @VisibleForTesting
    public Chunk(long chunkId)
    {
        this.bufferNodeId = 0L;
        this.exchangeId = "exchangeId";
        this.partitionId = 0;
        this.chunkId = chunkId;
    }

    public String getExchangeId()
    {
        return exchangeId;
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public synchronized boolean hasRecoverableIoFailure()
    {
        return switch (chunkData) {
            case null -> false;
            case DiskChunkData diskChunkData -> diskChunkData.hasRecoverableIoFailure();
            case MemoryChunkData ignored -> false;
        };
    }

    public synchronized Optional<ChunkPlacement> chunkPlacement()
    {
        if (chunkData == null) {
            return Optional.empty();
        }
        return Optional.of(chunkData.chunkPlacement());
    }

    public ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
    {
        checkState(!closed, "write() called on a closed chunk");
        return chunkData.write(taskId, attemptId, data);
    }

    public boolean hasEnoughSpace(int requiredStorageSize)
    {
        checkState(!closed, "hasEnoughSpace() called on a closed chunk");
        return chunkData.hasEnoughSpace(requiredStorageSize);
    }

    public boolean isEmpty()
    {
        checkState(!closed, "isEmpty() called on a closed chunk");
        checkState(chunkData != null, "isEmpty() called after release");
        return chunkData.isEmpty();
    }

    public int openChunkDataSizeInBytes()
    {
        checkState(!closed, "openChunkDataSizeInBytes() called on an closed chunk");
        return chunkData.dataSizeInBytes();
    }

    @VisibleForTesting
    int dataSizeInBytes()
    {
        checkState(closed, "dataSizeInBytes() called on an open chunk");
        return chunkData.dataSizeInBytes();
    }

    // null means chunk data has spooled
    public synchronized ChunkDataLease getChunkDataLease()
    {
        checkState(closed, "getChunkDataLease() called on an open chunk");
        if (chunkData == null) {
            return null;
        }
        return chunkData.get();
    }

    public synchronized boolean chunkDataInMemory()
    {
        checkState(closed, "chunkDataInMemory() called on an open chunk");
        return chunkData != null;
    }

    public synchronized int getReclaimableBytes()
    {
        checkState(closed, "getReclaimableBytes() called on an open chunk");
        if (chunkData == null) {
            return 0;
        }
        return chunkData.getReclaimableBytes();
    }

    public ChunkHandle getHandle()
    {
        checkState(closed, "getHandle() called on an open chunk");
        if (chunkHandle == null) {
            chunkHandle = new ChunkHandle(bufferNodeId, partitionId, chunkId, dataSizeInBytes);
        }
        return chunkHandle;
    }

    public synchronized void release()
    {
        if (chunkData != null) {
            chunkData.release();
            chunkData = null;
        }
    }

    public ListenableFuture<Void> close()
    {
        ListenableFuture<Void> future = chunkData.close();
        dataSizeInBytes = chunkData.dataSizeInBytes();
        closed = true;
        return future;
    }

    @Override
    public String toString()
    {
        return "chunkId %d in %s/%d".formatted(chunkId, exchangeId, partitionId);
    }
}
