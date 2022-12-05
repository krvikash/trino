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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.server.DataServerStats;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryAllocator
{
    private final long maxBytes;
    private final long lowWatermark;
    private final long highWatermark;
    private final DataServerStats dataServerStats;
    @GuardedBy("this")
    private final Queue<PendingAllocation> pendingAllocations = new ArrayDeque<>();

    @GuardedBy("this")
    private long allocatedBytes;

    @Inject
    public MemoryAllocator(MemoryAllocatorConfig config, DataServerStats dataServerStats)
    {
        long heapHeadroom = config.getHeapHeadroom().toBytes();
        long heapSize = Runtime.getRuntime().maxMemory();
        checkArgument(heapHeadroom < heapSize, "Heap headroom %s should be less than available heap size %s", heapHeadroom, heapSize);
        this.maxBytes = heapSize - heapHeadroom;
        this.lowWatermark = (long) (maxBytes * config.getAllocationRatioLowWatermark());
        this.highWatermark = (long) (maxBytes * config.getAllocationRatioHighWatermark());
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        dataServerStats.updateTotalMemoryInBytes(maxBytes);
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
