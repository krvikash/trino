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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.starburst.stargate.buffer.data.execution.CountedReference.Ref;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.finalizeChecksum;
import static io.starburst.stargate.buffer.data.execution.ChunkData.ChunkPlacement.MEMORY;
import static java.util.Objects.requireNonNull;

public final class MemoryChunkData
        implements ChunkData
{
    private final MemoryAllocator memoryAllocator;
    private final ExecutorService executor;
    private final int chunkSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    @GuardedBy("this")
    private final List<Slice> completedSlices;
    private final Ref<List<SliceLease>> sliceLeases;

    private final XxHash64 hash = new XxHash64();
    private final Slice headerSlice = Slices.allocate(DATA_PAGE_HEADER_SIZE);

    private int writtenBytes;
    private int dataSizeInBytes;
    private int numDataPages;
    @GuardedBy("this")
    private SliceOutput sliceOutput;

    public MemoryChunkData(
            MemoryAllocator memoryAllocator,
            ExecutorService executor,
            int chunkSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum)
    {
        checkArgument(chunkSizeInBytes >= chunkSliceSizeInBytes && chunkSizeInBytes % chunkSliceSizeInBytes == 0,
                "chunkSizeInBytes %s is not a multiple of chunkSliceSizeInBytes %s", chunkSizeInBytes, chunkSliceSizeInBytes);
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        int initialCapacity = this.chunkSizeInBytes / chunkSliceSizeInBytes;
        this.completedSlices = new ArrayList<>(initialCapacity);
        this.sliceLeases = CountedReference.create(
                () -> new ArrayList<>(initialCapacity),
                leases -> {
                    synchronized (MemoryChunkData.this) {
                        leases.forEach(SliceLease::release);
                        leases.clear();
                        completedSlices.clear();
                    }
                });
    }

    @Override
    public ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
    {
        int writableBytes = chunkSizeInBytes - writtenBytes;
        int dataSize = data.length();
        int requiredStorageSize = DATA_PAGE_HEADER_SIZE + dataSize;
        checkArgument(requiredStorageSize <= writableBytes, "requiredStorageSize %s larger than writableBytes %s", requiredStorageSize, writableBytes);

        if (calculateDataPagesChecksum) {
            hash.update(data);
        }
        numDataPages++;
        writtenBytes += requiredStorageSize;
        dataSizeInBytes += dataSize;

        SliceOutput headerSliceOutput = headerSlice.getOutput();
        headerSliceOutput.writeShort(taskId);
        headerSliceOutput.writeByte(attemptId);
        headerSliceOutput.writeInt(dataSize);

        ChunkWriteFuture chunkWriteFuture = new ChunkWriteFuture(headerSlice, data);
        chunkWriteFuture.process();
        return chunkWriteFuture;
    }

    @Override
    public boolean hasEnoughSpace(int requiredStorageSize)
    {
        int writableBytes = chunkSizeInBytes - writtenBytes;
        return requiredStorageSize <= writableBytes;
    }

    @Override
    public int dataSizeInBytes()
    {
        return dataSizeInBytes;
    }

    @Override
    public synchronized boolean isEmpty()
    {
        return completedSlices.isEmpty() && sliceOutput == null;
    }

    @Override
    public synchronized ChunkDataLease get()
    {
        Ref<List<SliceLease>> reference = sliceLeases.addReference();
        try {
            if (!calculateDataPagesChecksum) {
                return new MemoryChunkDataLease(
                        completedSlices,
                        NO_CHECKSUM,
                        numDataPages,
                        reference::release);
            }

            long checksum = finalizeChecksum(hash);
            return new MemoryChunkDataLease(
                    completedSlices,
                    checksum,
                    numDataPages,
                    reference::release);
        }
        catch (Exception e) {
            try {
                reference.release();
            }
            catch (Exception ex) {
                // ignore exception from release since we're already handling an exception, and we don't want to mask the original exception
            }
            throw e;
        }
    }

    @Override
    public ChunkPlacement chunkPlacement()
    {
        return MEMORY;
    }

    @Override
    public synchronized int getReclaimableBytes()
    {
        return sliceLeases.get().size() * chunkSliceSizeInBytes;
    }

    @Override
    public synchronized void close()
    {
        if (sliceOutput != null) {
            completedSlices.add(sliceOutput.slice());
            sliceOutput = null;
        }
    }

    @Override
    public void release()
    {
        sliceLeases.release();
    }

    private class ChunkWriteFuture
            extends AbstractFuture<Void>
    {
        private final Slice header;
        private final Slice data;
        private final int totalLength;

        @GuardedBy("MemoryChunkData.this")
        private int offset;
        @GuardedBy("MemoryChunkData.this")
        private ListenableFuture<SliceOutput> currentSliceOutput;

        ChunkWriteFuture(Slice header, Slice data)
        {
            this.header = requireNonNull(header, "header is null");
            this.data = requireNonNull(data, "data is null");
            this.totalLength = header.length() + data.length();
            synchronized (MemoryChunkData.this) {
                if (sliceOutput != null) {
                    this.currentSliceOutput = immediateFuture(MemoryChunkData.this.sliceOutput);
                }
                else {
                    this.currentSliceOutput = createNewSliceOutput();
                }
            }
        }

        public void process()
        {
            checkState(!isDone() || isCancelled(), "process() called on done, not cancelled ChunkWriteFuture()");
            synchronized (MemoryChunkData.this) {
                if (currentSliceOutput == null) {
                    checkState(isCancelled(), "ChunkWriteFuture should be in cancelled state");
                    return;
                }

                Futures.addCallback(
                        currentSliceOutput,
                        new FutureCallback<>()
                        {
                            @Override
                            public void onSuccess(SliceOutput sliceOutput)
                            {
                                try {
                                    boolean completeFuture = false;
                                    synchronized (MemoryChunkData.this) {
                                        if (!sliceOutput.isWritable()) {
                                            MemoryChunkData.this.completedSlices.add(sliceOutput.getUnderlyingSlice());
                                            currentSliceOutput = createNewSliceOutput();
                                            process();
                                            return;
                                        }

                                        if (offset < header.length()) {
                                            int bytesToWrite = Math.min(header.length() - offset, sliceOutput.writableBytes());
                                            sliceOutput.writeBytes(header, offset, bytesToWrite);
                                            offset += bytesToWrite;
                                        }

                                        if (header.length() <= offset && offset < totalLength) {
                                            int bytesToWrite = Math.min(totalLength - offset, sliceOutput.writableBytes());
                                            sliceOutput.writeBytes(data, offset - header.length(), bytesToWrite);
                                            offset += bytesToWrite;
                                        }

                                        if (offset == totalLength) {
                                            MemoryChunkData.this.sliceOutput = sliceOutput;
                                            completeFuture = true;
                                            // drop reference
                                            currentSliceOutput = null;
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
                                synchronized (MemoryChunkData.this) {
                                    // drop reference
                                    currentSliceOutput = null;
                                }
                                setException(throwable);
                            }
                        },
                        executor);
            }
        }

        @GuardedBy("MemoryChunkData.this")
        private ListenableFuture<SliceOutput> createNewSliceOutput()
        {
            SliceLease sliceLease = new SliceLease(memoryAllocator, MemoryChunkData.this.chunkSliceSizeInBytes);
            MemoryChunkData.this.sliceLeases.get().add(sliceLease);
            return Futures.transform(
                    sliceLease.getSliceFuture(),
                    Slice::getOutput,
                    executor);
        }

        @Override
        protected void interruptTask()
        {
            ListenableFuture<SliceOutput> futureToBeCancelled;
            synchronized (MemoryChunkData.this) {
                futureToBeCancelled = currentSliceOutput;
                currentSliceOutput = null;
            }
            if (futureToBeCancelled != null) {
                futureToBeCancelled.cancel(true);
            }
        }
    }
}
