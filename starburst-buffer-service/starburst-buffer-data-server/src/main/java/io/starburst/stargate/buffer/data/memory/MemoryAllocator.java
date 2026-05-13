/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryAllocator
{
    private static final Logger log = Logger.get(MemoryAllocator.class);

    private final long chunksMemory;
    private final long lowWatermark;
    private final long highWatermark;
    private final long chunkSlicePoolingLimit;
    private final int chunkSliceSizeInBytes;
    private final DataServerStats dataServerStats;
    @GuardedBy("this")
    private final Queue<Slice> chunkSlicePool;
    @GuardedBy("this")
    private final Queue<PendingAllocation> pendingAllocations = new ArrayDeque<>();

    @GuardedBy("this")
    private long allocatedBytes;
    @GuardedBy("this")
    private long nonPoolableAllocatedBytes;

    @Inject
    public MemoryAllocator(
            MemoryConfig memoryConfig,
            MemoryAllocatorConfig memoryAllocatorConfig,
            ChunkManagerConfig chunkManagerConfig,
            DataServerStats dataServerStats)
    {
        long baseMemory = memoryConfig.getBaseMemory().toBytes();
        chunksMemory = memoryConfig.getChunksMemory().toBytes();

        // sanity check
        long heapSize = Runtime.getRuntime().maxMemory();
        checkArgument(baseMemory <= heapSize, "Total memory %s should be less than or equalt to heap size %s", baseMemory, heapSize);
        checkArgument(chunksMemory <= baseMemory, "Chunks memory %s should be less or equal to total memory %s", chunksMemory, baseMemory);

        double lowWatermarkRatio = memoryAllocatorConfig.getSpoolingRatioLowWatermark();
        double highWatermarkRatio = memoryAllocatorConfig.getSpoolingRatioHighWatermark();
        checkArgument(0.0 <= lowWatermarkRatio && lowWatermarkRatio <= 1.0, "lowWatermarkRatio expected to be in range [0.0, 1.0], but is %s", lowWatermarkRatio);
        checkArgument(0.0 <= highWatermarkRatio && highWatermarkRatio <= 1.0, "highWatermarkRatio expected to be in range [0.0, 1.0], but is %s", highWatermarkRatio);
        checkArgument(lowWatermarkRatio <= highWatermarkRatio, "lowWatermarkRatio %s should be no larger than highWatermarkRatio %s", lowWatermarkRatio, highWatermarkRatio);
        this.lowWatermark = (long) (chunksMemory * lowWatermarkRatio);
        this.highWatermark = (long) (chunksMemory * highWatermarkRatio);
        double chunkSlicePoolingFraction = memoryAllocatorConfig.getChunkSlicePoolingFraction();
        checkArgument(0.0 <= chunkSlicePoolingFraction && chunkSlicePoolingFraction < 1.0,
                "chunkSlicePoolingFraction expected to be in range [0.0, 1.0), but is %s", chunkSlicePoolingFraction);
        this.chunkSlicePoolingLimit = (long) (chunksMemory * chunkSlicePoolingFraction);
        this.chunkSliceSizeInBytes = toIntExact(chunkManagerConfig.getChunkSliceSize().toBytes());
        this.chunkSlicePool = new ArrayDeque<>(toIntExact(chunkSlicePoolingLimit / chunkSliceSizeInBytes));
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        dataServerStats.updateTotalMemoryInBytes(chunksMemory);
        dataServerStats.updateFreeMemoryInBytes(getFreeMemory());

        log.info("Initializing MemoryAllocator; heapSize=%s, baseMemory=%s, chunksMemory=%s, lowWatermark=%s, highWatermark=%s, chunkSlicePoolingLimit=%s",
                DataSize.ofBytes(heapSize),
                DataSize.ofBytes(baseMemory),
                DataSize.ofBytes(chunksMemory),
                DataSize.ofBytes(lowWatermark),
                DataSize.ofBytes(highWatermark),
                DataSize.ofBytes(chunkSlicePoolingLimit));
    }

    public synchronized ListenableFuture<Slice> allocate(int bytes)
    {
        if (!hasEnoughSpace(bytes)) {
            SettableFuture<Slice> future = SettableFuture.create();
            PendingAllocation pendingAllocation = new PendingAllocation(bytes, future);
            pendingAllocations.add(pendingAllocation);
            return future;
        }

        return immediateFuture(allocateInternal(bytes));
    }

    public synchronized void release(Slice slice)
    {
        int bytes = slice.length();
        verify(allocatedBytes >= bytes, "%s bytes allocated, but trying to release %s bytes", allocatedBytes, bytes);
        if (bytes == chunkSliceSizeInBytes) {
            long poolableAllocatedBytes = allocatedBytes - nonPoolableAllocatedBytes;
            if (poolableAllocatedBytes <= chunkSlicePoolingLimit) {
                chunkSlicePool.offer(slice);
            }
        }
        else {
            verify(nonPoolableAllocatedBytes >= bytes, "%s non-poolable bytes allocated, but trying to release %s bytes", nonPoolableAllocatedBytes, bytes);
            nonPoolableAllocatedBytes -= bytes;
            dataServerStats.updateNonPoolableAllocatedMemoryInBytes(nonPoolableAllocatedBytes);
        }
        allocatedBytes -= bytes;
        dataServerStats.updateFreeMemoryInBytes(getFreeMemory());

        processPendingAllocations();
    }

    public long getTotalMemory()
    {
        return chunksMemory;
    }

    public synchronized double getAllocationPercentage()
    {
        return 100.0 * allocatedBytes / chunksMemory;
    }

    public synchronized long getFreeMemory()
    {
        return chunksMemory - allocatedBytes;
    }

    public synchronized boolean belowHighWatermark()
    {
        return allocatedBytes < highWatermark;
    }

    public synchronized boolean aboveLowWatermark()
    {
        return allocatedBytes > lowWatermark;
    }

    public synchronized long getRequiredMemoryToRelease()
    {
        return Math.max(0, allocatedBytes - lowWatermark);
    }

    @VisibleForTesting
    synchronized int getChunkSlicePoolSize()
    {
        return chunkSlicePool.size();
    }

    @VisibleForTesting
    synchronized long getAllocatedMemory()
    {
        return allocatedBytes;
    }

    @GuardedBy("this")
    private boolean hasEnoughSpace(int bytes)
    {
        long availableBytes = chunksMemory - allocatedBytes;
        return availableBytes >= bytes;
    }

    @GuardedBy("this")
    private Slice allocateInternal(int bytes)
    {
        allocatedBytes += bytes;
        dataServerStats.updateFreeMemoryInBytes(getFreeMemory());
        if (bytes == chunkSliceSizeInBytes && !chunkSlicePool.isEmpty()) {
            return chunkSlicePool.poll();
        }
        if (bytes != chunkSliceSizeInBytes) {
            nonPoolableAllocatedBytes += bytes;
            dataServerStats.updateNonPoolableAllocatedMemoryInBytes(nonPoolableAllocatedBytes);
        }
        return Slices.allocate(bytes);
    }

    @GuardedBy("this")
    private void processPendingAllocations()
    {
        // first in first out
        while (!pendingAllocations.isEmpty()) {
            PendingAllocation pendingAllocation = pendingAllocations.peek();
            SettableFuture<Slice> future = pendingAllocation.future();
            if (future.isCancelled()) {
                pendingAllocations.poll();
            }
            else {
                int bytes = pendingAllocation.bytes();
                if (hasEnoughSpace(bytes)) {
                    Slice slice = allocateInternal(bytes);
                    future.set(slice);
                    pendingAllocations.poll();
                    if (future.isCancelled()) {
                        release(slice);
                    }
                }
                else {
                    break;
                }
            }
        }
    }

    private record PendingAllocation(int bytes, SettableFuture<Slice> future)
    {
        public PendingAllocation {
            requireNonNull(future, "future is null");
        }
    }
}
