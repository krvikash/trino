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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Tracks the number of in-progress addDataPages requests and updates stats accordingly.
 * Provides latches that automatically decrement the counter when released.
 */
public class AddDataPagesInProgressTracker
{
    private final DataServerStats stats;
    private final AtomicInteger inProgressAddDataPagesRequests = new AtomicInteger();

    @Inject
    public AddDataPagesInProgressTracker(DataServerStats stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
    }

    /**
     * Increments the in-progress request counter and returns a latch that can be used to decrement it.
     * The latch ensures the counter is only decremented once by calling release(), even if release() is called multiple times.
     */
    public InProgressLatch incrementAndGetLatch()
    {
        int currentRequestsCount = inProgressAddDataPagesRequests.incrementAndGet();
        stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
        return new InProgressLatch(currentRequestsCount);
    }

    public int getInProgressAddDataPagesRequests()
    {
        return inProgressAddDataPagesRequests.get();
    }

    public class InProgressLatch
    {
        private final long currentRequestsCount;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private InProgressLatch(int currentRequestsCount)
        {
            this.currentRequestsCount = currentRequestsCount;
        }

        public void release()
        {
            if (released.compareAndSet(false, true)) {
                int currentRequestsCount = inProgressAddDataPagesRequests.decrementAndGet();
                stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
            }
        }

        public long currentRequestsCount()
        {
            return currentRequestsCount;
        }
    }
}
