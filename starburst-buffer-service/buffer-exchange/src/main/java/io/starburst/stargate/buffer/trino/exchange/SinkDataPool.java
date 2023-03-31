/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SinkDataPool
{
    private final DataSize memoryLowWaterMark;
    private final DataSize memoryHighWaterMark;
    private final long maxWaitInMillis;
    private final int minWrittenPagesCount;
    private final long minWrittenPagesSizeInBytes;
    private final int targetWrittenPagesCount;
    private final long targetWrittenPagesSizeInBytes;
    private final int targetWrittenPartitionsCount;
    private final Ticker ticker;
    private final SettableFuture<Void> finishedFuture;

    @GuardedBy("this")
    private final Map<Integer, Deque<Slice>> dataQueues = new HashMap<>();
    @GuardedBy("this")
    private final Map<Integer, AtomicLong> dataQueueBytes = new HashMap<>();
    @GuardedBy("this")
    private volatile long memoryUsageBytes;
    @GuardedBy("this")
    private SettableFuture<Void> memoryBlockedFuture;
    @GuardedBy("this")
    private boolean noMoreData;
    @GuardedBy("this")
    private long lastWriteTimestamp;

    public SinkDataPool(
            DataSize memoryLowWaterMark,
            DataSize memoryHighWaterMark,
            Duration maxWait,
            int minWrittenPagesCount,
            DataSize minWrittenPagesSize,
            int targetWrittenPagesCount,
            DataSize targetWrittenPagesSize,
            int targetWrittenPartitionsCount,
            Ticker ticker)
    {
        this.memoryLowWaterMark = requireNonNull(memoryLowWaterMark, "memoryLowWaterMark is null");
        this.memoryHighWaterMark = requireNonNull(memoryHighWaterMark, "memoryHighWaterMark is null");
        this.maxWaitInMillis = requireNonNull(maxWait, "maxWait is null").toMillis();
        this.minWrittenPagesCount = minWrittenPagesCount;
        this.minWrittenPagesSizeInBytes = requireNonNull(minWrittenPagesSize, "minWrittenPagesSize is null").toBytes();
        this.targetWrittenPagesCount = targetWrittenPagesCount;
        this.targetWrittenPagesSizeInBytes = targetWrittenPagesSize.toBytes();
        this.targetWrittenPartitionsCount = targetWrittenPartitionsCount;
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.finishedFuture = SettableFuture.create();
    }

    public synchronized void add(Integer partitionId, Slice data)
    {
        checkArgument(data.length() > 0, "cannot add empty data page");
        checkArgument(!noMoreData, "cannot add data if noMoreData is already set");
        Deque<Slice> queue = getDataQueue(partitionId);
        queue.add(data);
        getDataQueueBytes(partitionId).addAndGet(data.length());
        long retainedSize = data.getRetainedSize();
        verify(retainedSize > 0, "expected retainedSize to be greater than 0; got %s for %s", retainedSize, data);
        updateMemoryUsage(retainedSize);
    }

    public void noMoreData()
    {
        boolean poolFinished = false;
        synchronized (this) {
            this.noMoreData = true;
            if (memoryUsageBytes == 0) {
                poolFinished = true;
            }
        }
        if (poolFinished) {
            finishedFuture.set(null);
        }
    }

    public ListenableFuture<Void> whenFinished()
    {
        return finishedFuture;
    }

    @GuardedBy("this")
    private AtomicLong getDataQueueBytes(Integer partitionId)
    {
        return dataQueueBytes.computeIfAbsent(partitionId, ignored -> new AtomicLong());
    }

    @GuardedBy("this")
    private Deque<Slice> getDataQueue(Integer partitionId)
    {
        return dataQueues.computeIfAbsent(partitionId, ignored -> new ArrayDeque<>());
    }

    @GuardedBy("this")
    private int getDataQueuePageCount(Integer partitionId)
    {
        return getDataQueue(partitionId).size();
    }

    @GuardedBy("this")
    private void updateMemoryUsage(long delta)
    {
        memoryUsageBytes += delta;
        if (memoryBlockedFuture != null && !memoryBlockedFuture.isDone() && memoryUsageBytes < memoryLowWaterMark.toBytes()) {
            memoryBlockedFuture.set(null);
            return;
        }
        if ((memoryBlockedFuture == null || memoryBlockedFuture.isDone()) && memoryUsageBytes > memoryHighWaterMark.toBytes()) {
            memoryBlockedFuture = SettableFuture.create();
        }
    }

    public synchronized Optional<PollResult> pollBest(Set<Integer> partitionSet, boolean finishing)
    {
        boolean canWrite = false;
        if (finishing || tickerReadMillis() - lastWriteTimestamp >= maxWaitInMillis || (memoryBlockedFuture != null && !memoryBlockedFuture.isDone())) {
            canWrite = true;
        }
        else {
            long queuedBytes = 0;
            int numPages = 0;
            for (Integer partition : partitionSet) {
                queuedBytes += getDataQueueBytes(partition).get();
                numPages += getDataQueuePageCount(partition);

                if (numPages >= minWrittenPagesCount || queuedBytes >= minWrittenPagesSizeInBytes) {
                    canWrite = true;
                    break;
                }
            }
        }

        if (!canWrite) {
            return Optional.empty();
        }

        Comparator<Integer> byQueueBytes = Comparator.comparing(partition -> {
            AtomicLong queueBytes = dataQueueBytes.get(partition);
            if (queueBytes == null) {
                return 0L;
            }
            return -queueBytes.get(); // reversed order
        });

        List<Integer> partitionsOrdered = ImmutableList.sortedCopyOf(byQueueBytes, partitionSet);

        ImmutableListMultimap.Builder<Integer, Slice> dataByPartition = ImmutableListMultimap.builder();
        boolean resultEmpty = true;

        int polledPagesCount = 0;
        int polledPagesSize = 0;
        int polledPartitionsCount = 0;
        for (int partition : partitionsOrdered) {
            if (polledPagesCount >= targetWrittenPagesCount || polledPagesSize >= targetWrittenPagesSizeInBytes || polledPartitionsCount >= targetWrittenPartitionsCount) {
                break; // collected enough
            }

            if (getDataQueueBytes(partition).get() == 0) {
                break; // no need to go further; all following queues will be empty
                // note: the fact that we can bail out here comes from the fact that we only decrement dataQueueBytes when we commit PollResult
            }

            Deque<Slice> queue = dataQueues.get(partition);
            if (queue == null) {
                break; // no need to go further when we see first empty queue
            }

            if (queue.isEmpty()) {
                // uncommitted data polled from queue; we have no guarantee following queues are empty
                continue;
            }

            while (polledPagesCount < targetWrittenPagesCount && polledPagesSize < targetWrittenPagesSizeInBytes) {
                Slice page = queue.pollFirst();
                if (page == null) {
                    break;
                }
                polledPagesCount++;
                polledPagesSize += page.length();
                dataByPartition.put(partition, page);
                resultEmpty = false;
            }
            polledPartitionsCount++;
        }

        if (resultEmpty) {
            return Optional.empty();
        }
        lastWriteTimestamp = tickerReadMillis();

        return Optional.of(new PollResult(dataByPartition.build()));
    }

    public synchronized ListenableFuture<Void> isBlocked()
    {
        if (memoryBlockedFuture != null && !memoryBlockedFuture.isDone()) {
            return memoryBlockedFuture;
        }
        return Futures.immediateVoidFuture();
    }

    public long getMemoryUsage()
    {
        return memoryUsageBytes;
    }

    private long tickerReadMillis()
    {
        return ticker.read() / 1_000_000;
    }

    public class PollResult
    {
        private final ListMultimap<Integer, Slice> dataByPartition;
        private boolean done;

        public PollResult(ListMultimap<Integer, Slice> dataByPartition)
        {
            requireNonNull(dataByPartition, "dataByPartition is null");
            this.dataByPartition = ImmutableListMultimap.copyOf(dataByPartition);
        }

        public ListMultimap<Integer, Slice> getDataByPartition()
        {
            return dataByPartition;
        }

        public void commit()
        {
            markDone();
            boolean poolFinished = false;
            synchronized (SinkDataPool.this) {
                for (Map.Entry<Integer, Collection<Slice>> entry : dataByPartition.asMap().entrySet()) {
                    int partition = entry.getKey();
                    List<Slice> data = (List<Slice>) entry.getValue();

                    // commit global and per queue memory usage (slices are removed from queue on poll)
                    long dataSize = 0;
                    long retainedSize = 0;
                    for (Slice slice : data) {
                        dataSize += slice.length();
                        retainedSize += slice.getRetainedSize();
                    }
                    getDataQueueBytes(partition).addAndGet(-dataSize);
                    updateMemoryUsage(-retainedSize);
                }

                if (noMoreData && memoryUsageBytes == 0) {
                    poolFinished = true;
                }
            }
            if (poolFinished) {
                // complete the future outside the synchronized section
                finishedFuture.set(null);
            }
        }

        public void rollback()
        {
            markDone();
            synchronized (SinkDataPool.this) {
                for (Map.Entry<Integer, Collection<Slice>> entry : dataByPartition.asMap().entrySet()) {
                    int partition = entry.getKey();
                    List<Slice> data = (List<Slice>) entry.getValue();

                    Deque<Slice> dataQueue = dataQueues.get(partition);
                    // reinsert slices into queue
                    for (Slice slice : Lists.reverse(data)) {
                        dataQueue.addFirst(slice);
                    }
                }
            }
        }

        private void markDone()
        {
            checkState(!done, "already done");
            done = true;
        }
    }
}
