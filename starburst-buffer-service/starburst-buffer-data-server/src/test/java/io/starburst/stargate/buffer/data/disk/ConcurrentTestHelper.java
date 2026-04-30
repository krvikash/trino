/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

final class ConcurrentTestHelper
{
    private ConcurrentTestHelper() {}

    @FunctionalInterface
    interface ThrowingRunnable
    {
        void run()
                throws Exception;
    }

    static void runConcurrently(int threadCount, ThrowingRunnable task)
            throws Exception
    {
        runConcurrently(Collections.nCopies(threadCount, task));
    }

    static void runConcurrently(List<ThrowingRunnable> tasks)
            throws Exception
    {
        int threadCount = tasks.size();
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Callable<Void>> callables = tasks.stream()
                .<Callable<Void>>map(task -> () -> {
                    barrier.await(30, SECONDS);
                    task.run();
                    return null;
                })
                .toList();
        try (ExecutorService executor = newFixedThreadPool(threadCount)) {
            for (Future<Void> future : executor.invokeAll(callables)) {
                future.get();
            }
        }
    }
}
