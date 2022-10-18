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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
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
    private static final int TARGET_PAGES_COUNT = 3;
    private static final long TARGET_PAGES_SIZE = DataSize.of(8, DataSize.Unit.MEGABYTE).toBytes();

    private final DataSize memoryLowWaterMark;
    private final DataSize memoryHighWaterMark;

    @GuardedBy("this")
    private final Map<Integer, Deque<Slice>> dataQueues = new HashMap<>();
    @GuardedBy("this")
    private final Map<Integer, AtomicLong> dataQueueBytes = new HashMap<>();

    @GuardedBy("this")
    private final Map<Integer, PollResult> currentPolls = new HashMap<>();

    @GuardedBy("this")
    private volatile long memoryUsageBytes;
    @GuardedBy("this")
    private SettableFuture<Void> memoryBlockedFuture;

    private final SettableFuture<Void> finishedFuture;

    @GuardedBy("this")
    private boolean noMoreData;

    public SinkDataPool(DataSize memoryLowWaterMark, DataSize memoryHighWaterMark)
    {
        this.memoryLowWaterMark = requireNonNull(memoryLowWaterMark, "memoryLowWaterMark is null");
        this.memoryHighWaterMark = requireNonNull(memoryHighWaterMark, "memoryHighWaterMark is null");
        this.finishedFuture = SettableFuture.create();
    }

    public synchronized void add(Integer partitionId, Slice data)
    {
        Deque<Slice> queue = dataQueues.computeIfAbsent(partitionId, ignored -> new ArrayDeque<>());
        queue.add(data);
        long dataSize = data.getRetainedSize();
        dataQueueBytes.computeIfAbsent(partitionId, ignored -> new AtomicLong()).addAndGet(dataSize);
        updateMemoryUsage(dataSize);
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
        int selectedPartition = -1;
        long selectedPartitionBytes = 0;
        for (int partition : partitionSet) {
            Deque<Slice> queue = dataQueues.get(partition);
            if (queue == null) {
                continue;
            }
            long queueBytes = dataQueueBytes.get(partition).get();
            if (queueBytes > selectedPartitionBytes) {
                selectedPartition = partition;
            }
        }
        if (selectedPartition == -1) {
            return Optional.empty();
        }

        return Optional.of(poll(selectedPartition));
    }

    @GuardedBy("this")
    private PollResult poll(int partition)
    {
        PollResult currenPoll = currentPolls.get(partition);
        checkArgument(currenPoll == null, "poll already exists for partition %s", partition);
        Deque<Slice> queue = dataQueues.get(partition);
        verify(!queue.isEmpty(), "expected non-empty queue");

        int polledPagesCount = 0;
        int polledPagesSize = 0;
        ImmutableList.Builder<Slice> polledPages = ImmutableList.builder();
        while (polledPagesCount < TARGET_PAGES_COUNT && polledPagesSize < TARGET_PAGES_SIZE) {
            Slice page = queue.pollFirst();
            if (page == null) {
                break;
            }
            polledPagesCount++;
            polledPagesSize += page.length();
            polledPages.add(page);
        }
        PollResult pollResult = new PollResult(partition, polledPages.build());
        currentPolls.put(partition, pollResult);
        return pollResult;
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
        private final int partition;
        private final List<Slice> data;

        public PollResult(int partition, List<Slice> data)
        {
            this.partition = partition;
            requireNonNull(data, "data is null");
            this.data = ImmutableList.copyOf(data);
        }

        public int getPartition()
        {
            return partition;
        }

        public List<Slice> getData()
        {
            return data;
        }

        public void commit()
        {
            boolean poolFinished = false;
            synchronized (SinkDataPool.this) {
                // commit global and per queue memory usage (slices are removed from queue on poll)
                long dataSize = 0;
                for (Slice slice : data) {
                    dataSize += slice.getRetainedSize();
                }
                dataQueueBytes.computeIfAbsent(partition, ignored -> new AtomicLong()).addAndGet(-dataSize);
                updateMemoryUsage(-dataSize);

                PollResult removedPollResult = currentPolls.remove(partition);
                verify(removedPollResult == this, "unexpected poll result removed; %s", removedPollResult);
                if (noMoreData && memoryUsageBytes == 0) {
                    poolFinished = true;
                }
                if (poolFinished) {
                    finishedFuture.set(null);
                }
            }
        }

        public void rollback()
        {
            synchronized (SinkDataPool.this) {
                int slicesCount = data.size();
                Deque<Slice> dataQueue = dataQueues.get(partition);
                // reinsert slices into queue
                for (int i = slicesCount - 1; i >= 0; i--) {
                    Slice slice = data.get(i);
                    dataQueue.addFirst(slice);
                }
                PollResult removedPollResult = currentPolls.remove(partition);
                verify(removedPollResult == this, "unexpected poll result removed; %s", removedPollResult);
            }
        }
    }
}
