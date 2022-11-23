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
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static java.util.Objects.requireNonNull;

// Note on thread-safety: only release() and getChunkData() may be concurrently called after Chunk is closed
@NotThreadSafe
public class Chunk
{
    private final long bufferNodeId;
    private final int partitionId;
    private final long chunkId;

    private ChunkData chunkData;
    private int dataSizeInBytes;
    private boolean closed;
    private ChunkHandle chunkHandle;

    public Chunk(
            long bufferNodeId,
            int partitionId,
            long chunkId,
            MemoryAllocator memoryAllocator,
            ExecutorService executor,
            int chunkMaxSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum)
    {
        this.bufferNodeId = bufferNodeId;
        this.partitionId = partitionId;
        this.chunkId = chunkId;
        this.chunkData = new ChunkData(
                memoryAllocator,
                executor,
                chunkMaxSizeInBytes,
                chunkSliceSizeInBytes,
                calculateDataPagesChecksum);
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
    {
        checkState(!closed, "write() called on a closed chunk");
        return chunkData.write(taskId, attemptId, data);
    }

    public boolean hasEnoughSpace(Slice data)
    {
        checkState(!closed, "hasEnoughSpace() called on a closed chunk");
        return chunkData.hasEnoughSpace(data);
    }

    @VisibleForTesting
    int dataSizeInBytes()
    {
        checkState(closed, "dataSizeInBytes() called on an open chunk");
        return chunkData.dataSizeInBytes();
    }

    // null means chunk data has spooled
    public synchronized ChunkDataHolder getChunkData()
    {
        checkState(closed, "getChunkData() called on an open chunk");
        if (chunkData == null) {
            return null;
        }
        return chunkData.get();
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

    public void close()
    {
        chunkData.close();
        dataSizeInBytes = chunkData.dataSizeInBytes();
        closed = true;
    }

    private static class ChunkData
    {
        private final MemoryAllocator memoryAllocator;
        private final ExecutorService executor;
        private final int chunkMaxSizeInBytes;
        private final int chunkSliceSizeInBytes;
        private final boolean calculateDataPagesChecksum;
        @GuardedBy("this")
        private final List<Slice> completedSlices;
        @GuardedBy("this")
        private final List<SliceLease> chunkSliceLeases;

        private final XxHash64 hash = new XxHash64();
        private final Slice headerSlice = Slices.allocate(DATA_PAGE_HEADER_SIZE);

        private int numBytesWritten;
        private int dataSizeInBytes;
        private int numDataPages;
        @GuardedBy("this")
        private SliceOutput sliceOutput;
        @GuardedBy("this")
        private boolean released;

        public ChunkData(
                MemoryAllocator memoryAllocator,
                ExecutorService executor,
                int chunkMaxSizeInBytes,
                int chunkSliceSizeInBytes,
                boolean calculateDataPagesChecksum)
        {
            checkArgument(chunkMaxSizeInBytes >= chunkSliceSizeInBytes && chunkMaxSizeInBytes % chunkSliceSizeInBytes == 0,
                    "chunkMaxSizeInBytes %s is not a multiple of chunkSliceSizeInBytes %s", chunkMaxSizeInBytes, chunkSliceSizeInBytes);
            this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.chunkMaxSizeInBytes = chunkMaxSizeInBytes;
            this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
            this.calculateDataPagesChecksum = calculateDataPagesChecksum;
            int initialCapacity = chunkMaxSizeInBytes / chunkSliceSizeInBytes;
            this.completedSlices = new ArrayList<>(initialCapacity);
            this.chunkSliceLeases = new ArrayList<>(initialCapacity);
        }

        public ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
        {
            int writableBytes = chunkMaxSizeInBytes - numBytesWritten;
            int dataSize = data.length();
            int requiredStorageSize = DATA_PAGE_HEADER_SIZE + dataSize;
            checkArgument(requiredStorageSize <= writableBytes, "requiredStorageSize %s larger than writableBytes %s", requiredStorageSize, writableBytes);

            if (calculateDataPagesChecksum) {
                hash.update(data);
            }
            numDataPages++;
            numBytesWritten += requiredStorageSize;
            dataSizeInBytes += dataSize;

            SliceOutput headerSliceOutput = headerSlice.getOutput();
            headerSliceOutput.writeShort(taskId);
            headerSliceOutput.writeByte(attemptId);
            headerSliceOutput.writeInt(dataSize);

            ChunkWriteFuture headerWriteFuture = new ChunkWriteFuture(headerSlice);
            headerWriteFuture.process();

            return Futures.transformAsync(
                    headerWriteFuture,
                    ignored -> {
                        ChunkWriteFuture dataWriteFuture = new ChunkWriteFuture(data);
                        dataWriteFuture.process();

                        return dataWriteFuture;
                    },
                    executor);
        }

        public boolean hasEnoughSpace(Slice data)
        {
            int requiredStorageSize = DATA_PAGE_HEADER_SIZE + data.length();
            int writableBytes = chunkMaxSizeInBytes - numBytesWritten;
            checkArgument(requiredStorageSize <= chunkMaxSizeInBytes, "requiredStorageSize %s larger than chunkMaxSizeInBytes %s", requiredStorageSize, chunkMaxSizeInBytes);
            return requiredStorageSize <= writableBytes;
        }

        public int dataSizeInBytes()
        {
            return dataSizeInBytes;
        }

        public synchronized ChunkDataHolder get()
        {
            if (!calculateDataPagesChecksum) {
                return new ChunkDataHolder(completedSlices, NO_CHECKSUM, numDataPages);
            }

            long checksum = hash.hash();
            if (checksum == NO_CHECKSUM) {
                checksum++;
            }
            return new ChunkDataHolder(completedSlices, checksum, numDataPages);
        }

        public synchronized void close()
        {
            if (sliceOutput != null) {
                completedSlices.add(sliceOutput.slice());
                sliceOutput = null;
            }
        }

        public synchronized void release()
        {
            chunkSliceLeases.forEach(SliceLease::release);
            released = true;
        }

        private class ChunkWriteFuture
                extends AbstractFuture<Void>
        {
            private final Slice data;

            @GuardedBy("ChunkData.this")
            private int offset;
            @GuardedBy("ChunkData.this")
            private ListenableFuture<SliceOutput> currentSliceOutput;

            ChunkWriteFuture(Slice data)
            {
                this.data = requireNonNull(data, "data is null");
                synchronized (ChunkData.this) {
                    if (sliceOutput != null) {
                        this.currentSliceOutput = immediateFuture(ChunkData.this.sliceOutput);
                    }
                    else {
                        this.currentSliceOutput = createNewSliceOutput();
                    }
                }
            }

            public void process()
            {
                synchronized (ChunkData.this) {
                    if (currentSliceOutput == null) {
                        checkState(isCancelled(), "ChunkWriteFuture should be in cancelled state");
                        return;
                    }

                    Futures.addCallback(
                            currentSliceOutput,
                            new FutureCallback<>() {
                                @Override
                                public void onSuccess(SliceOutput sliceOutput)
                                {
                                    try {
                                        boolean completeFuture = false;
                                        synchronized (ChunkData.this) {
                                            if (!sliceOutput.isWritable()) {
                                                ChunkData.this.completedSlices.add(sliceOutput.getUnderlyingSlice());
                                                currentSliceOutput = createNewSliceOutput();
                                                process();
                                                return;
                                            }

                                            int bytesToWrite = Math.min(data.length() - offset, sliceOutput.writableBytes());
                                            sliceOutput.writeBytes(data, offset, bytesToWrite);
                                            offset += bytesToWrite;

                                            if (offset == data.length()) {
                                                ChunkData.this.sliceOutput = sliceOutput;
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

            @GuardedBy("ChunkData.this")
            private ListenableFuture<SliceOutput> createNewSliceOutput()
            {
                checkState(!released, "new Slice allocation after release of ChunkData");
                SliceLease sliceLease = new SliceLease(memoryAllocator, ChunkData.this.chunkSliceSizeInBytes);
                ChunkData.this.chunkSliceLeases.add(sliceLease);
                return Futures.transform(
                        sliceLease.getSliceFuture(),
                        Slice::getOutput,
                        executor);
            }

            @Override
            protected synchronized void interruptTask()
            {
                synchronized (ChunkData.this) {
                    if (currentSliceOutput != null) {
                        currentSliceOutput.cancel(true);
                        currentSliceOutput = null;
                    }
                }
            }
        }
    }
}
