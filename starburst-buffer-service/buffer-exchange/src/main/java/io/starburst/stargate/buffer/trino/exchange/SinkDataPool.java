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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SinkDataPool
{
    private final DataSize memoryLowWaterMark;
    private final DataSize memoryHighWaterMark;
    private final int targetWrittenPagesCount;
    private final long targetWrittenPagesSize;
    private final int targetWrittenPartitionsCount;

    @GuardedBy("this")
    private final Map<Integer, Deque<Slice>> dataQueues = new HashMap<>();
    @GuardedBy("this")
    private final Map<Integer, AtomicLong> dataQueueBytes = new HashMap<>();

    @GuardedBy("this")
    private final Set<Integer> currentPolls = new HashSet<>();

    @GuardedBy("this")
    private volatile long memoryUsageBytes;
    @GuardedBy("this")
    private SettableFuture<Void> memoryBlockedFuture;

    private final SettableFuture<Void> finishedFuture;

    @GuardedBy("this")
    private boolean noMoreData;

    public SinkDataPool(DataSize memoryLowWaterMark, DataSize memoryHighWaterMark, int targetWrittenPagesCount, DataSize targetWrittenPagesSize, int targetWrittenPartitionsCount)
    {
        this.memoryLowWaterMark = requireNonNull(memoryLowWaterMark, "memoryLowWaterMark is null");
        this.memoryHighWaterMark = requireNonNull(memoryHighWaterMark, "memoryHighWaterMark is null");
        this.targetWrittenPagesCount = targetWrittenPagesCount;
        this.targetWrittenPagesSize = targetWrittenPagesSize.toBytes();
        this.targetWrittenPartitionsCount = targetWrittenPartitionsCount;
        this.finishedFuture = SettableFuture.create();
    }

    public synchronized void add(Integer partitionId, Slice data)
    {
        checkArgument(data.length() > 0, "cannot add empty data page");
        checkArgument(!noMoreData, "cannot add data if noMoreData is already set");
        Deque<Slice> queue = dataQueues.computeIfAbsent(partitionId, ignored -> new ArrayDeque<>());
        queue.add(data);
        dataQueueBytes.computeIfAbsent(partitionId, ignored -> new AtomicLong()).addAndGet(data.length());
        updateMemoryUsage(data.getRetainedSize());
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

    public synchronized Optional<PollResult> pollBest(Set<Integer> partitionSet)
    {
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
            if (polledPagesCount >= targetWrittenPagesCount || polledPagesSize >= targetWrittenPagesSize || polledPartitionsCount >= targetWrittenPartitionsCount) {
                break; // collected enough
            }

            checkArgument(!currentPolls.contains(partition), "poll already exists for partition %s", partition);
            Deque<Slice> queue = dataQueues.get(partition);
            if (queue == null || queue.isEmpty()) {
                break; // no need to go further when we see first empty queue
            }

            while (polledPagesCount < targetWrittenPagesCount && polledPagesSize < targetWrittenPagesSize) {
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
            currentPolls.add(partition);
        }

        if (resultEmpty) {
            return Optional.empty();
        }
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

    public class PollResult
    {
        private final ListMultimap<Integer, Slice> dataByPartition;

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
                    dataQueueBytes.computeIfAbsent(partition, ignored -> new AtomicLong()).addAndGet(-dataSize);
                    updateMemoryUsage(-retainedSize);

                    verify(currentPolls.remove(partition), "poll was not registered; %s", partition);
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
            synchronized (SinkDataPool.this) {
                for (Map.Entry<Integer, Collection<Slice>> entry : dataByPartition.asMap().entrySet()) {
                    int partition = entry.getKey();
                    List<Slice> data = (List<Slice>) entry.getValue();

                    Deque<Slice> dataQueue = dataQueues.get(partition);
                    // reinsert slices into queue
                    for (Slice slice : Lists.reverse(data)) {
                        dataQueue.addFirst(slice);
                    }
                    verify(currentPolls.remove(partition), "poll was not registered; %s", partition);
                }
            }
        }
    }
}
