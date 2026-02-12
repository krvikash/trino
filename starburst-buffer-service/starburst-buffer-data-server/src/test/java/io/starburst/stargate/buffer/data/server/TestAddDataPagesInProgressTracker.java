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

import io.starburst.stargate.buffer.data.server.AddDataPagesInProgressTracker.InProgressLatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAddDataPagesInProgressTracker
{
    @Test
    public void testBasicIncrementAndRelease()
    {
        DataServerStats stats = new DataServerStats();
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);

        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);

        InProgressLatch latch1 = tracker.incrementAndGetLatch();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(1);
        assertThat(latch1.currentRequestsCount()).isEqualTo(1);

        InProgressLatch latch2 = tracker.incrementAndGetLatch();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(2);
        assertThat(latch2.currentRequestsCount()).isEqualTo(2);

        latch1.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(1);

        latch2.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);
    }

    @Test
    public void testMultipleReleaseProtection()
    {
        DataServerStats stats = new DataServerStats();
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);

        InProgressLatch latch = tracker.incrementAndGetLatch();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(1);

        // First release should work
        latch.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);

        // Second release should be ignored
        latch.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);

        // Third release should also be ignored
        latch.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);
    }

    @Test
    public void testMultipleLatches()
    {
        DataServerStats stats = new DataServerStats();
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);

        InProgressLatch latch1 = tracker.incrementAndGetLatch();
        InProgressLatch latch2 = tracker.incrementAndGetLatch();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(2);

        latch1.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(1);

        latch2.release();
        assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);
    }

    @Test
    public void testStatsIntegration()
    {
        DataServerStats stats = new DataServerStats();
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);

        InProgressLatch latch1 = tracker.incrementAndGetLatch();
        assertThat(stats.getInProgressAddDataPagesRequests()).isEqualTo(1);

        InProgressLatch latch2 = tracker.incrementAndGetLatch();
        assertThat(stats.getInProgressAddDataPagesRequests()).isEqualTo(2);

        latch1.release();
        assertThat(stats.getInProgressAddDataPagesRequests()).isEqualTo(1);

        latch2.release();
        assertThat(stats.getInProgressAddDataPagesRequests()).isEqualTo(0);
    }

    @Test
    public void testConcurrentIncrements()
            throws Exception
    {
        DataServerStats stats = new DataServerStats();
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);

        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Future<InProgressLatch>> futures = new ArrayList<>();

        try {
            // Submit all tasks to increment concurrently
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    barrier.await(10, SECONDS);
                    return tracker.incrementAndGetLatch();
                }));
            }

            // Wait for all increments to complete
            for (Future<InProgressLatch> future : futures) {
                future.get(10, SECONDS);
            }

            // Verify final count is correct
            assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(threadCount);
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(10, SECONDS);
        }
    }

    @Test
    public void testConcurrentReleases()
            throws Exception
    {
        DataServerStats stats = new DataServerStats();
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);

        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            // Create latches
            List<InProgressLatch> latches = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                latches.add(tracker.incrementAndGetLatch());
            }

            assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(threadCount);

            // Release all concurrently
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            List<Future<?>> futures = new ArrayList<>();
            for (InProgressLatch latch : latches) {
                futures.add(executor.submit(() -> {
                    try {
                        barrier.await(10, SECONDS);
                        latch.release();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            // Wait for all releases to complete
            for (Future<?> future : futures) {
                future.get(10, SECONDS);
            }

            // Verify count is back to zero
            assertThat(tracker.getInProgressAddDataPagesRequests()).isEqualTo(0);
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
