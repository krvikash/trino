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

import com.google.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Tracks the number of in-progress addDataPages requests and updates stats accordingly.
 * Provides latches that automatically decrement the counter when released.
 * Unreleased latches are kept so that requests blocking a drain can be reported.
 */
public class AddDataPagesInProgressTracker
{
    private final DataServerStats stats;
    private final AtomicInteger inProgressAddDataPagesRequests = new AtomicInteger();
    private final Set<InProgressLatch> inProgressLatches = ConcurrentHashMap.newKeySet();

    @Inject
    public AddDataPagesInProgressTracker(DataServerStats stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
    }

    /**
     * Increments the in-progress request counter and returns a latch that can be used to decrement it.
     * The latch ensures the counter is only decremented once by calling release(), even if release() is called multiple times.
     */
    public InProgressLatch incrementAndGetLatch(String exchangeId, int taskId, int attemptId, long dataPagesId)
    {
        int currentRequestsCount = inProgressAddDataPagesRequests.incrementAndGet();
        stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
        InProgressLatch latch = new InProgressLatch(currentRequestsCount, exchangeId, taskId, attemptId, dataPagesId);
        inProgressLatches.add(latch);
        return latch;
    }

    public int getInProgressAddDataPagesRequests()
    {
        return inProgressAddDataPagesRequests.get();
    }

    public List<String> describeInProgressRequests()
    {
        long now = System.currentTimeMillis();
        return inProgressLatches.stream()
                .sorted(Comparator.comparingLong(InProgressLatch::startMillis))
                .map(latch -> latch.describe(now))
                .collect(toImmutableList());
    }

    public enum Stage
    {
        AWAITING_MEMORY,
        READING_BODY,
        ADDING_PAGES,
        AWAITING_PAGE_WRITES,
    }

    public class InProgressLatch
    {
        private final long currentRequestsCount;
        private final String exchangeId;
        private final int taskId;
        private final int attemptId;
        private final long dataPagesId;
        private final long startMillis = System.currentTimeMillis();
        private final AtomicBoolean released = new AtomicBoolean(false);
        private volatile Stage stage = Stage.AWAITING_MEMORY;
        private volatile long stageStartMillis = startMillis;

        private InProgressLatch(int currentRequestsCount, String exchangeId, int taskId, int attemptId, long dataPagesId)
        {
            this.currentRequestsCount = currentRequestsCount;
            this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
            this.taskId = taskId;
            this.attemptId = attemptId;
            this.dataPagesId = dataPagesId;
        }

        public void enterStage(Stage stage)
        {
            this.stage = requireNonNull(stage, "stage is null");
            this.stageStartMillis = System.currentTimeMillis();
        }

        public void release()
        {
            if (released.compareAndSet(false, true)) {
                inProgressLatches.remove(this);
                int currentRequestsCount = inProgressAddDataPagesRequests.decrementAndGet();
                stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
            }
        }

        public long currentRequestsCount()
        {
            return currentRequestsCount;
        }

        private long startMillis()
        {
            return startMillis;
        }

        private String describe(long now)
        {
            return "exchange=%s, taskId=%s, attemptId=%s, dataPagesId=%s, stage=%s, inStageMillis=%s, inProgressMillis=%s".formatted(
                    exchangeId,
                    taskId,
                    attemptId,
                    dataPagesId,
                    stage,
                    now - stageStartMillis,
                    now - startMillis);
        }
    }
}
