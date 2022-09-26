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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.server.DataServerStats;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryAllocator
{
    private static final Logger log = Logger.get(MemoryAllocator.class);

    private final long maxBytes;
    private final DataServerStats dataServerStats;

    @GuardedBy("this")
    private long allocatedBytes;

    @Inject
    public MemoryAllocator(MemoryAllocatorConfig config, DataServerStats dataServerStats)
    {
        long heapHeadroom = config.getHeapHeadroom().toBytes();
        long heapSize = Runtime.getRuntime().maxMemory();
        checkArgument(heapHeadroom < heapSize, "Heap headroom %s should be less than available heap size %s", heapHeadroom, heapSize);
        this.maxBytes = heapSize - heapHeadroom;
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        dataServerStats.getTotalMemoryInBytes().add(maxBytes);
    }

    public synchronized Optional<Slice> allocate(int bytes)
    {
        long availableBytes = maxBytes - allocatedBytes;
        if (bytes > availableBytes) {
            log.info("%d bytes available, but trying to allocate %d bytes", availableBytes, bytes);
            return Optional.empty();
        }
        allocatedBytes += bytes;
        dataServerStats.getFreeMemoryInBytes().add(getFreeMemory());
        return Optional.of(Slices.allocate(bytes));
    }

    public synchronized void release(long bytes)
    {
        verify(allocatedBytes >= bytes, "%s bytes allocated, but trying to release %s bytes", allocatedBytes, bytes);
        allocatedBytes -= bytes;
        dataServerStats.getFreeMemoryInBytes().add(getFreeMemory());
    }

    public long getTotalMemory()
    {
        return maxBytes;
    }

    public synchronized long getFreeMemory()
    {
        return maxBytes - allocatedBytes;
    }
}
