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

import io.airlift.concurrent.MoreFutures;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergCommitQueueConcurrentWrites
        extends AbstractTestQueryFramework
{
    private static final int THREADS = 8;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.commit-queue.enabled", "true")
                .build();
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentInserts()
            throws Exception
    {
        String tableName = "test_queued_concurrent_inserts_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) WITH (max_commit_retry = 0)");
        try {
            runConcurrently(THREADS, threadNumber -> getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (" + threadNumber + ", 10)"));
            assertQuery("SELECT count(*), sum(a) FROM " + tableName, "VALUES (8, 28)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentDeletes()
            throws Exception
    {
        String tableName = "test_queued_concurrent_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) WITH (partitioning = ARRAY['part'], max_commit_retry = 0)");
        assertUpdate(
                "INSERT INTO " + tableName + " SELECT a, part FROM UNNEST(sequence(1, 8)) AS t(a) CROSS JOIN UNNEST(sequence(1, 8)) AS u(part)",
                64);
        try {
            runConcurrently(THREADS, threadNumber -> getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = " + (threadNumber + 1) + " AND a <= 4"));
            assertQuery("SELECT count(*), min(a) FROM " + tableName, "VALUES (32, 5)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentInsertsAndDeletes()
            throws Exception
    {
        String tableName = "test_queued_concurrent_mixed_writes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) WITH (partitioning = ARRAY['part'], max_commit_retry = 0)");
        assertUpdate("INSERT INTO " + tableName + " SELECT a, a FROM UNNEST(sequence(1, 4)) AS t(a)", 4);
        try {
            runConcurrently(THREADS, threadNumber -> {
                if (threadNumber % 2 == 0) {
                    getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (" + (threadNumber + 100) + ", " + (threadNumber + 100) + ")");
                }
                else {
                    getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = " + threadNumber);
                }
            });
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (2, 2), (4, 4), (100, 100), (102, 102), (104, 104), (106, 106)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentOptimizeAndInserts()
            throws Exception
    {
        String tableName = "test_queued_concurrent_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT) WITH (max_commit_retry = 0)");
        for (int value = 1; value <= 4; value++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES " + value, 1);
        }
        try {
            runConcurrently(THREADS, threadNumber -> {
                if (threadNumber == 0) {
                    getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE optimize");
                }
                else {
                    getQueryRunner().execute("INSERT INTO " + tableName + " VALUES " + (threadNumber + 100));
                }
            });
            assertQuery("SELECT count(*), sum(a) FROM " + tableName, "VALUES (11, 738)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentWritesToDifferentTables()
            throws Exception
    {
        String firstTableName = "test_queued_concurrent_first_" + randomNameSuffix();
        String secondTableName = "test_queued_concurrent_second_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + firstTableName + " (a INT) WITH (max_commit_retry = 0)");
        assertUpdate("CREATE TABLE " + secondTableName + " (a INT) WITH (max_commit_retry = 0)");
        try {
            runConcurrently(THREADS, threadNumber -> {
                String tableName;
                if (threadNumber % 2 == 0) {
                    tableName = firstTableName;
                }
                else {
                    tableName = secondTableName;
                }
                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES " + threadNumber);
            });
            assertQuery("SELECT * FROM " + firstTableName, "VALUES 0, 2, 4, 6");
            assertQuery("SELECT * FROM " + secondTableName, "VALUES 1, 3, 5, 7");
        }
        finally {
            assertUpdate("DROP TABLE " + firstTableName);
            assertUpdate("DROP TABLE " + secondTableName);
        }
    }

    // Tables are created with max_commit_retry = 0, so any commit conflict fails the query.
    private static void runConcurrently(int threads, IntConsumer action)
            throws Exception
    {
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            executor.invokeAll(IntStream.range(0, threads)
                            .mapToObj(threadNumber -> (Callable<Void>) () -> {
                                barrier.await(10, SECONDS);
                                action.accept(threadNumber);
                                return null;
                            })
                            .collect(toImmutableList()))
                    .forEach(MoreFutures::getDone);
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }
}
