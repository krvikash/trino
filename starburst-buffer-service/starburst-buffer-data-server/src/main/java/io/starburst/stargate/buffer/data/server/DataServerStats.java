/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DataServerStats
{
    private final AtomicLong totalMemoryInBytes = new AtomicLong();
    private final AtomicLong freeMemoryInBytes = new AtomicLong();
    private final AtomicLong nonPoolableAllocatedMemoryInBytes = new AtomicLong();
    private final AtomicLong trackedExchanges = new AtomicLong();
    private final AtomicLong openChunks = new AtomicLong();
    private final AtomicLong closedChunks = new AtomicLong();
    private final AtomicLong spooledChunks = new AtomicLong();
    private final AtomicLong spooledChunksByExchangeSize = new AtomicLong();
    private final CounterStat spooledDataSize = new CounterStat();
    private final CounterStat spoolingFailures = new CounterStat();
    private final DistributionStat spooledChunkSizeDistribution = new DistributionStat();
    private final CounterStat writtenDataSize = new CounterStat();
    private final DistributionStat writtenDataSizeDistribution = new DistributionStat();
    private final DistributionStat writtenDataSizePerPartitionDistribution = new DistributionStat();
    private final CounterStat readDataSize = new CounterStat();
    private final DistributionStat readDataSizeDistribution = new DistributionStat();
    private final AtomicLong inProgressAddDataPagesRequests = new AtomicLong();
    private final CounterStat overloadedAddDataPagesCount = new CounterStat();

    // how many chunks sharing exchange were spooled within single iteration
    private final DistributionStat spooledSharingExchangeCount = new DistributionStat();
    // what is the total size of chunks sharing exchange spooled withing single iteration
    private final DistributionStat spooledSharingExchangeSize = new DistributionStat();
    // tracks size of files written to spooling storage
    private final DistributionStat spooledFileSizeDistribution = new DistributionStat();

    private final DistributionStat diskWritePositionalLatencyNanos = new DistributionStat();
    private final DistributionStat diskChunkWallTimeNanos = new DistributionStat();
    private final DistributionStat diskChunkBytesAtClose = new DistributionStat();
    private final CounterStat diskChunksOpened = new CounterStat();
    private final CounterStat diskChunksClosed = new CounterStat();
    private final AtomicInteger diskOpenChunks = new AtomicInteger();
    private final CounterStat diskWrittenBytes = new CounterStat();
    private volatile ThreadPoolExecutor diskIoExecutor;

    private final CounterStat diskReadBytes = new CounterStat();
    private final CounterStat diskSpooledBytes = new CounterStat();
    private final CounterStat memorySpooledBytes = new CounterStat();

    public void updateTotalMemoryInBytes(long totalMemoryInBytes)
    {
        this.totalMemoryInBytes.set(totalMemoryInBytes);
    }

    @Managed
    public long getTotalMemoryInBytes()
    {
        return totalMemoryInBytes.get();
    }

    public void updateFreeMemoryInBytes(long freeMemoryInBytes)
    {
        this.freeMemoryInBytes.set(freeMemoryInBytes);
    }

    @Managed
    public long getFreeMemoryInBytes()
    {
        return freeMemoryInBytes.get();
    }

    public void updateNonPoolableAllocatedMemoryInBytes(long nonPoolableAllocatedMemoryInBytes)
    {
        this.nonPoolableAllocatedMemoryInBytes.set(nonPoolableAllocatedMemoryInBytes);
    }

    @Managed
    public long getNonPoolableAllocatedMemoryInBytes()
    {
        return nonPoolableAllocatedMemoryInBytes.get();
    }

    public void updateTrackedExchanges(long trackedExchanges)
    {
        this.trackedExchanges.set(trackedExchanges);
    }

    @Managed
    public long getTrackedExchanges()
    {
        return trackedExchanges.get();
    }

    public void updateOpenChunks(long openChunks)
    {
        this.openChunks.set(openChunks);
    }

    @Managed
    public long getOpenChunks()
    {
        return openChunks.get();
    }

    public void updateClosedChunks(long closedChunks)
    {
        this.closedChunks.set(closedChunks);
    }

    @Managed
    public long getClosedChunks()
    {
        return closedChunks.get();
    }

    public void updateSpooledChunks(long spooledChunks)
    {
        this.spooledChunks.set(spooledChunks);
    }

    @Managed
    public long getSpooledChunksCount()
    {
        return spooledChunks.get();
    }

    public void updateSpooledChunksByExchangeSize(long spooledChunksByExchangeSize)
    {
        this.spooledChunksByExchangeSize.set(spooledChunksByExchangeSize);
    }

    @Managed
    public long getSpooledChunksByExchangeSize()
    {
        return spooledChunksByExchangeSize.get();
    }

    @Managed
    @Nested
    public CounterStat getSpooledDataSize()
    {
        return spooledDataSize;
    }

    @Managed
    @Nested
    public CounterStat getSpoolingFailures()
    {
        return spoolingFailures;
    }

    @Managed
    @Nested
    public DistributionStat getSpooledChunkSizeDistribution()
    {
        return spooledChunkSizeDistribution;
    }

    @Managed
    @Nested
    public CounterStat getWrittenDataSize()
    {
        return writtenDataSize;
    }

    @Managed
    @Nested
    public DistributionStat getWrittenDataSizeDistribution()
    {
        return writtenDataSizeDistribution;
    }

    @Managed
    @Nested
    public DistributionStat getWrittenDataSizePerPartitionDistribution()
    {
        return writtenDataSizePerPartitionDistribution;
    }

    @Managed
    @Nested
    public CounterStat getReadDataSize()
    {
        return readDataSize;
    }

    @Managed
    @Nested
    public DistributionStat getReadDataSizeDistribution()
    {
        return readDataSizeDistribution;
    }

    @Managed
    public long getInProgressAddDataPagesRequests()
    {
        return inProgressAddDataPagesRequests.get();
    }

    public void updateInProgressAddDataPagesRequests(long inProgressAddDataPagesRequests)
    {
        this.inProgressAddDataPagesRequests.set(inProgressAddDataPagesRequests);
    }

    @Managed
    public CounterStat getOverloadedAddDataPagesCount()
    {
        return overloadedAddDataPagesCount;
    }

    @Managed
    @Nested
    public DistributionStat getSpooledSharingExchangeCount()
    {
        return spooledSharingExchangeCount;
    }

    @Managed
    @Nested
    public DistributionStat getSpooledSharingExchangeSize()
    {
        return spooledSharingExchangeSize;
    }

    @Managed
    @Nested
    public DistributionStat getSpooledFileSizeDistribution()
    {
        return spooledFileSizeDistribution;
    }

    public void setDiskIoExecutor(ThreadPoolExecutor diskIoExecutor)
    {
        this.diskIoExecutor = diskIoExecutor;
    }

    public void recordDiskChunkOpen()
    {
        diskChunksOpened.update(1);
        diskOpenChunks.incrementAndGet();
    }

    public void recordDiskChunkClose(long wallTimeNanos, long bytesAtClose)
    {
        diskChunksClosed.update(1);
        diskOpenChunks.decrementAndGet();
        diskChunkWallTimeNanos.add(wallTimeNanos);
        diskChunkBytesAtClose.add(bytesAtClose);
    }

    public void recordDiskWriteLatency(long latencyNanos, long bytes)
    {
        diskWritePositionalLatencyNanos.add(latencyNanos);
        diskWrittenBytes.update(bytes);
    }

    public void recordDiskBytesRead(long bytes)
    {
        diskReadBytes.update(bytes);
    }

    public void recordDiskSpooledBytes(long bytes)
    {
        diskSpooledBytes.update(bytes);
    }

    public void recordMemorySpooledBytes(long bytes)
    {
        memorySpooledBytes.update(bytes);
    }

    @Managed
    @Nested
    public DistributionStat getDiskWritePositionalLatencyNanos()
    {
        return diskWritePositionalLatencyNanos;
    }

    @Managed
    @Nested
    public DistributionStat getDiskChunkWallTimeNanos()
    {
        return diskChunkWallTimeNanos;
    }

    @Managed
    @Nested
    public DistributionStat getDiskChunkBytesAtClose()
    {
        return diskChunkBytesAtClose;
    }

    @Managed
    @Nested
    public CounterStat getDiskChunksOpened()
    {
        return diskChunksOpened;
    }

    @Managed
    @Nested
    public CounterStat getDiskChunksClosed()
    {
        return diskChunksClosed;
    }

    @Managed
    public int getDiskOpenChunks()
    {
        return diskOpenChunks.get();
    }

    @Managed
    @Nested
    public CounterStat getDiskWrittenBytes()
    {
        return diskWrittenBytes;
    }

    @Managed
    public int getDiskIoActiveThreads()
    {
        ThreadPoolExecutor exec = diskIoExecutor;
        return exec == null ? 0 : exec.getActiveCount();
    }

    @Managed
    public int getDiskIoQueueDepth()
    {
        ThreadPoolExecutor exec = diskIoExecutor;
        return exec == null ? 0 : exec.getQueue().size();
    }

    @Managed
    public int getDiskIoPoolSize()
    {
        ThreadPoolExecutor exec = diskIoExecutor;
        return exec == null ? 0 : exec.getPoolSize();
    }

    @Managed
    public long getDiskIoCompletedTasks()
    {
        ThreadPoolExecutor exec = diskIoExecutor;
        return exec == null ? 0L : exec.getCompletedTaskCount();
    }

    @Managed
    @Nested
    public CounterStat getDiskReadBytes()
    {
        return diskReadBytes;
    }

    @Managed
    @Nested
    public CounterStat getDiskSpooledBytes()
    {
        return diskSpooledBytes;
    }

    @Managed
    @Nested
    public CounterStat getMemorySpooledBytes()
    {
        return memorySpooledBytes;
    }
}
