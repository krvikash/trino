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
package io.trino.tests.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.tests.benchmark.BenchmarkRunner.isOutOfMemory;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

/**
 * Builds the warp index before measured runs. Warp calls index building warming, which is the name
 * its JMX counters use. Every tracked run starts and ends with warp warming idle, so the counter
 * difference across a run is exactly the warming that run caused. Warp drops some index tasks, for
 * example when its task queue is full. A query is indexed once warp drops none of its tasks.
 */
final class WarpIndexing
{
    private static final Logger log = Logger.get(WarpIndexing.class);

    private static final int MAX_ROUNDS = 10;
    private static final long IDLE_POLL_MILLIS = 10;
    private static final long STALL_TIMEOUT_NANOS = MINUTES.toNanos(5);

    /**
     * Cumulative warming service counters summed over nodes. Scheduled minus finished is the number
     * of warming tasks in flight.
     */
    private static final List<String> WARMING_COUNTERS = ImmutableList.of(
            "warm_scheduled",
            "warm_finished",
            "warm_started",
            "warm_accomplished",
            "warm_failed",
            "warm_skipped_due_reaching_threshold",
            "warmup_elements_count",
            "row_group_count");

    /**
     * Warming tasks the executor rejects for a full queue or too many pending tasks on one row group.
     */
    private static final String REJECTED_COUNTER = "task_skipped_due_queue_size";

    private static final List<String> COUNTERS = ImmutableList.<String>builder()
            .addAll(WARMING_COUNTERS)
            .add(REJECTED_COUNTER)
            .build();

    private final DistributedQueryRunner runner;
    private final String sampleSql;
    private final Set<String> indexedQueries = new HashSet<>();
    private final Map<String, Long> measuredWarming = new LinkedHashMap<>();
    private Map<String, Long> idleSample;

    WarpIndexing(DistributedQueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
        String warmingTable = resolveStatsTable(runner, "warmingservicestats");
        String executorTable = resolveStatsTable(runner, "workertaskexecutorservicestats");
        log.info("Warp warming stats tables: %s, %s", warmingTable, executorTable);
        String warmingProjections = WARMING_COUNTERS.stream()
                .map(counter -> format("coalesce(sum(%s), 0)", counter))
                .collect(joining(", "));
        this.sampleSql = format(
                "SELECT * FROM (SELECT %s FROM jmx.current.\"%s\") CROSS JOIN (SELECT coalesce(sum(%s), 0) FROM jmx.current.\"%s\")",
                warmingProjections,
                warmingTable,
                REJECTED_COUNTER,
                executorTable);
        // queries run before the benchmark, such as ANALYZE, may still be warming
        this.idleSample = awaitWarmingIdle();
    }

    /**
     * Runs a suite warmup query and records whether warp dropped any of its index tasks.
     */
    void runAndTrack(String query, String sql)
    {
        indexedQueries.remove(query);
        WarmingRun run = trackRun(sql);
        if (run.lost() == 0) {
            indexedQueries.add(query);
        }
        log.info("%s warmup run: %s", query, run.describe());
    }

    /**
     * Reruns each query whose last run had index tasks dropped until warp drops none.
     */
    void finishIndexing(List<String> queries, Function<String, String> sqlForQuery)
    {
        long startNanos = System.nanoTime();
        List<String> unindexedQueries = queries.stream()
                .filter(query -> !indexedQueries.contains(query))
                .collect(toImmutableList());
        int runs = 0;
        for (String query : unindexedQueries) {
            runs += indexQuery(query, sqlForQuery.apply(query));
        }
        log.info(
                "Warp indexing: %d of %d queries rerun, %d runs in %d s",
                unindexedQueries.size(),
                queries.size(),
                runs,
                NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
    }

    private int indexQuery(String query, String sql)
    {
        for (int round = 1; round <= MAX_ROUNDS; round++) {
            WarmingRun run;
            try {
                run = trackRun(sql);
            }
            catch (RuntimeException e) {
                if (!isOutOfMemory(e)) {
                    throw e;
                }
                log.warn(e, "%s warp indexing round %d: out of memory, measuring anyway", query, round);
                return round;
            }
            log.info("%s warp indexing round %d: %s", query, round, run.describe());
            if (run.lost() == 0) {
                indexedQueries.add(query);
                return round;
            }
            if (run.count("warm_accomplished") == 0) {
                log.warn("%s: warp lost %d warming tasks and accomplished none, measuring anyway", query, run.lost());
                return round;
            }
        }
        log.warn("%s: warp still drops index tasks after %d rounds, measuring anyway", query, MAX_ROUNDS);
        return MAX_ROUNDS;
    }

    /**
     * Waits for warming left by a query's warmup iterations before its measured runs start.
     */
    void startMeasuredRuns()
    {
        idleSample = awaitWarmingIdle();
    }

    /**
     * Records warming started since {@link #startMeasuredRuns}, which the query's measured timings include.
     */
    void checkMeasuredRuns(String query)
    {
        Map<String, Long> before = idleSample;
        idleSample = awaitWarmingIdle();
        WarmingRun measuredRuns = new WarmingRun(before, idleSample, 0, 0);
        if (measuredRuns.count("warm_started") > 0) {
            measuredWarming.put(query, measuredRuns.count("warm_started"));
            log.warn("%s: warp warmed during the measured runs, so their timings include warming: %s", query, measuredRuns.describeWarming());
        }
    }

    /**
     * Reports the queries whose measured runs started warming.
     */
    void reportMeasuredRuns()
    {
        if (measuredWarming.isEmpty()) {
            log.info("Warp did not warm during the measured runs");
            return;
        }
        log.warn(
                "Warp warmed during the measured runs of %d queries: %s",
                measuredWarming.size(),
                measuredWarming.entrySet().stream()
                        .map(entry -> format("%s (%d tasks)", entry.getKey(), entry.getValue()))
                        .collect(joining(", ")));
    }

    private WarmingRun trackRun(String sql)
    {
        Map<String, Long> before = idleSample;
        long startNanos = System.nanoTime();
        try {
            runner.execute(sql);
        }
        catch (RuntimeException e) {
            // the next tracked run starts from idle even after a failed query
            try {
                idleSample = awaitWarmingIdle();
            }
            catch (RuntimeException idleFailure) {
                e.addSuppressed(idleFailure);
            }
            throw e;
        }
        long queryNanos = System.nanoTime() - startNanos;
        idleSample = awaitWarmingIdle();
        return new WarmingRun(before, idleSample, queryNanos, System.nanoTime() - startNanos - queryNanos);
    }

    /**
     * Idle means no warming task in flight across two consecutive samples with equal counters. A
     * finishing task counts itself finished before it schedules its next task, and the second
     * sample catches that next task.
     */
    private Map<String, Long> awaitWarmingIdle()
    {
        Map<String, Long> previous = sample();
        long lastChangeNanos = System.nanoTime();
        while (true) {
            try {
                Thread.sleep(IDLE_POLL_MILLIS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for warp warming", e);
            }
            Map<String, Long> current = sample();
            long inFlight = current.get("warm_scheduled") - current.get("warm_finished");
            if (!current.equals(previous)) {
                lastChangeNanos = System.nanoTime();
            }
            else if (inFlight == 0) {
                return current;
            }
            else if (System.nanoTime() - lastChangeNanos > STALL_TIMEOUT_NANOS) {
                throw new IllegalStateException(format(
                        "Warp warming made no progress for %d minutes with %d tasks in flight: %s",
                        NANOSECONDS.toMinutes(STALL_TIMEOUT_NANOS),
                        inFlight,
                        current));
            }
            previous = current;
        }
    }

    private Map<String, Long> sample()
    {
        MaterializedRow row = runner.execute(sampleSql)
                .getMaterializedRows()
                .getFirst();
        return COUNTERS.stream()
                .collect(toImmutableMap(counter -> counter, counter -> ((Number) row.getField(COUNTERS.indexOf(counter))).longValue()));
    }

    private static String resolveStatsTable(DistributedQueryRunner runner, String type)
    {
        // the MBean's name property carries a hash suffix, so resolve the table name once
        return (String) runner.execute(
                "SELECT DISTINCT table_name FROM jmx.information_schema.tables" +
                        " WHERE table_schema = 'current' AND table_name LIKE 'io.trino.plugin.warp.gen.stats:%" + type + "%'").getOnlyValue();
    }

    private record WarmingRun(Map<String, Long> before, Map<String, Long> after, long queryNanos, long idleWaitNanos)
    {
        long count(String counter)
        {
            return after.get(counter) - before.get(counter);
        }

        long lost()
        {
            return count(REJECTED_COUNTER) + count("warm_failed") + count("warm_skipped_due_reaching_threshold");
        }

        String describe()
        {
            return format("query %d ms, warming tail %d ms, %s", NANOSECONDS.toMillis(queryNanos), NANOSECONDS.toMillis(idleWaitNanos), describeWarming());
        }

        String describeWarming()
        {
            return format(
                    "started=%d accomplished=%d lost=%d (rejected=%d failed=%d threshold=%d), %d elements over %d row groups",
                    count("warm_started"),
                    count("warm_accomplished"),
                    lost(),
                    count(REJECTED_COUNTER),
                    count("warm_failed"),
                    count("warm_skipped_due_reaching_threshold"),
                    count("warmup_elements_count"),
                    count("row_group_count"));
        }
    }
}
