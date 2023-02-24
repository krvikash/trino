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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

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

    private final long maxBytes;
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
            MemoryAllocatorConfig memoryAllocatorConfig,
            ChunkManagerConfig chunkManagerConfig,
            DataServerStats dataServerStats)
    {
        long heapHeadroom = memoryAllocatorConfig.getHeapHeadroom().toBytes();
        long heapSize = Runtime.getRuntime().maxMemory();
        checkArgument(heapHeadroom < heapSize, "Heap headroom %s should be less than available heap size %s", heapHeadroom, heapSize);
        this.maxBytes = heapSize - heapHeadroom;
        double lowWatermarkRatio = memoryAllocatorConfig.getAllocationRatioLowWatermark();
        double highWatermarkRatio = memoryAllocatorConfig.getAllocationRatioHighWatermark();
        checkArgument(0.0 <= lowWatermarkRatio && lowWatermarkRatio <= 1.0, "lowWatermarkRatio expected to be in range [0.0, 1.0], but is %s", lowWatermarkRatio);
        checkArgument(0.0 <= highWatermarkRatio && highWatermarkRatio <= 1.0, "highWatermarkRatio expected to be in range [0.0, 1.0], but is %s", highWatermarkRatio);
        checkArgument(lowWatermarkRatio <= highWatermarkRatio, "lowWatermarkRatio %s should be no larger than highWatermarkRatio %s", lowWatermarkRatio, highWatermarkRatio);
        this.lowWatermark = (long) (maxBytes * lowWatermarkRatio);
        this.highWatermark = (long) (maxBytes * highWatermarkRatio);
        double chunkSlicePoolingFraction = memoryAllocatorConfig.getChunkSlicePoolingFraction();
        checkArgument(0.0 <= chunkSlicePoolingFraction && chunkSlicePoolingFraction < 1.0,
                "chunkSlicePoolingFraction expected to be in range [0.0, 1.0), but is %s", chunkSlicePoolingFraction);
        this.chunkSlicePoolingLimit = (long) (maxBytes * chunkSlicePoolingFraction);
        this.chunkSliceSizeInBytes = toIntExact(chunkManagerConfig.getChunkSliceSize().toBytes());
        this.chunkSlicePool = new ArrayDeque<>(toIntExact(chunkSlicePoolingLimit / chunkSliceSizeInBytes));
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        dataServerStats.updateTotalMemoryInBytes(maxBytes);
        dataServerStats.updateFreeMemoryInBytes(getFreeMemory());

        log.info("Initializing MemoryAllocator; heapSize=%s, heapHeadroom=%s, maxBytes=%s, lowWatermark=%s, highWatermark=%s, chunkSlicePoolingLimit=%s",
                DataSize.ofBytes(heapSize),
                DataSize.ofBytes(heapHeadroom),
                DataSize.ofBytes(maxBytes),
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

    public synchronized void release(Slice slice, boolean poolable)
    {
        int bytes = slice.length();
        verify(allocatedBytes >= bytes, "%s bytes allocated, but trying to release %s bytes", allocatedBytes, bytes);
        if (bytes == chunkSliceSizeInBytes && poolable) {
            long poolableAllocatedBytes = allocatedBytes - nonPoolableAllocatedBytes;
            if (poolableAllocatedBytes <= chunkSlicePoolingLimit) {
                chunkSlicePool.offer(slice);
            }
        }
        else {
            nonPoolableAllocatedBytes -= bytes;
            dataServerStats.updateNonPoolableAllocatedMemoryInBytes(nonPoolableAllocatedBytes);
        }
        allocatedBytes -= bytes;
        dataServerStats.updateFreeMemoryInBytes(getFreeMemory());

        processPendingAllocations();
    }

    public long getTotalMemory()
    {
        return maxBytes;
    }

    public synchronized double getAllocationPercentage()
    {
        return 100.0 * allocatedBytes / maxBytes;
    }

    public synchronized long getFreeMemory()
    {
        return maxBytes - allocatedBytes;
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
        long availableBytes = maxBytes - allocatedBytes;
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
                        release(slice, true);
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
