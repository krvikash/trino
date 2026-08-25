/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(60)
final class TestIcebergCommitQueue
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final SchemaTableName OTHER_TABLE = new SchemaTableName("schema", "other_table");

    @Test
    void testSameTableCommitsAreSerialized()
            throws Exception
    {
        IcebergCommitQueue queue = new IcebergCommitQueue(new Duration(1, MINUTES), 1000);
        int threads = 8;
        AtomicInteger concurrentCommits = new AtomicInteger();
        AtomicInteger maxConcurrentCommits = new AtomicInteger();
        AtomicInteger completedCommits = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Object>> futures = IntStream.range(0, threads)
                    .mapToObj(_ -> executor.submit(() -> {
                        start.await();
                        queue.commitSerially(TABLE, () -> {
                            int concurrent = concurrentCommits.incrementAndGet();
                            maxConcurrentCommits.accumulateAndGet(concurrent, Math::max);
                            try {
                                Thread.sleep(10);
                            }
                            catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            concurrentCommits.decrementAndGet();
                            completedCommits.incrementAndGet();
                        });
                        return null;
                    }))
                    .collect(toImmutableList());
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(maxConcurrentCommits.get()).isEqualTo(1);
        assertThat(completedCommits.get()).isEqualTo(threads);
        assertThat(queue.activeTableCount()).isEqualTo(0);
    }

    @Test
    void testDifferentTablesCommitConcurrently()
            throws Exception
    {
        IcebergCommitQueue queue = new IcebergCommitQueue(new Duration(1, MINUTES), 1000);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            // each commit finishes only after the other commit has started, so both must run at once
            CountDownLatch bothStarted = new CountDownLatch(2);
            Future<?> first = executor.submit(() -> queue.commitSerially(TABLE, () -> rendezvous(bothStarted)));
            Future<?> second = executor.submit(() -> queue.commitSerially(OTHER_TABLE, () -> rendezvous(bothStarted)));
            first.get();
            second.get();
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(queue.activeTableCount()).isEqualTo(0);
    }

    @Test
    void testFullQueueBypassesSerialization()
            throws Exception
    {
        IcebergCommitQueue queue = new IcebergCommitQueue(new Duration(1, MINUTES), 1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch firstCommitStarted = new CountDownLatch(1);
            CountDownLatch releaseFirstCommit = new CountDownLatch(1);
            Future<?> first = executor.submit(() -> queue.commitSerially(TABLE, () -> {
                firstCommitStarted.countDown();
                await(releaseFirstCommit);
            }));
            firstCommitStarted.await();
            // the single queue slot is taken, so this commit runs immediately without waiting
            Future<?> second = executor.submit(() -> queue.commitSerially(TABLE, releaseFirstCommit::countDown));
            second.get(10, SECONDS);
            first.get();
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(queue.activeTableCount()).isEqualTo(0);
    }

    @Test
    void testWaitTimeoutBypassesSerialization()
            throws Exception
    {
        IcebergCommitQueue queue = new IcebergCommitQueue(new Duration(100, MILLISECONDS), 1000);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch firstCommitStarted = new CountDownLatch(1);
            CountDownLatch releaseFirstCommit = new CountDownLatch(1);
            Future<?> first = executor.submit(() -> queue.commitSerially(TABLE, () -> {
                firstCommitStarted.countDown();
                await(releaseFirstCommit);
            }));
            firstCommitStarted.await();
            // the first commit never yields within the wait, so this commit times out and proceeds
            Future<?> second = executor.submit(() -> queue.commitSerially(TABLE, releaseFirstCommit::countDown));
            second.get(10, SECONDS);
            first.get();
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(queue.activeTableCount()).isEqualTo(0);
    }

    @Test
    void testInterruptedWaiterThrowsAndUnregisters()
            throws Exception
    {
        IcebergCommitQueue queue = new IcebergCommitQueue(new Duration(1, MINUTES), 1000);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            CountDownLatch firstCommitStarted = new CountDownLatch(1);
            CountDownLatch releaseFirstCommit = new CountDownLatch(1);
            Future<?> first = executor.submit(() -> queue.commitSerially(TABLE, () -> {
                firstCommitStarted.countDown();
                await(releaseFirstCommit);
            }));
            firstCommitStarted.await();

            AtomicBoolean waiterCommitRan = new AtomicBoolean();
            AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
            AtomicBoolean interruptFlagRestored = new AtomicBoolean();
            Thread waiter = new Thread(() -> {
                try {
                    queue.commitSerially(TABLE, () -> waiterCommitRan.set(true));
                }
                catch (Throwable throwable) {
                    waiterFailure.set(throwable);
                    interruptFlagRestored.set(Thread.currentThread().isInterrupted());
                }
            });
            waiter.start();
            // pending interrupt status makes the semaphore wait throw even if the waiter has not blocked yet
            waiter.interrupt();
            waiter.join();
            releaseFirstCommit.countDown();
            first.get();

            assertThat(waiterCommitRan).isFalse();
            assertThat(waiterFailure.get())
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Interrupted while waiting to commit");
            assertThat(interruptFlagRestored).isTrue();
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(queue.activeTableCount()).isEqualTo(0);
    }

    @Test
    void testQueueEntryRemovedAfterFailedCommit()
    {
        IcebergCommitQueue queue = new IcebergCommitQueue(new Duration(1, MINUTES), 1000);
        assertThatThrownBy(() -> queue.commitSerially(TABLE, () -> {
            throw new RuntimeException("commit failed");
        }))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("commit failed");
        assertThat(queue.activeTableCount()).isEqualTo(0);
    }

    private static void rendezvous(CountDownLatch latch)
    {
        latch.countDown();
        await(latch);
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
