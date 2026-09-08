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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Warms the warp index by running each benchmark query until warp starts no further work.
 */
final class WarpWarmup
{
    private static final Logger log = Logger.get(WarpWarmup.class);

    private static final int MAX_ROUNDS = 10;
    private static final long IDLE_POLL_MILLIS = 10_000;
    private static final int IDLE_STABLE_POLLS = 3;
    private static final long ROUND_TIMEOUT_NANOS = 2L * 60 * 60 * 1_000_000_000;

    /**
     * Counters sampled around every warm round. The scheduled/started/accomplished/failed group
     * says how much warming a round produced, the skip counters say what warp turned down, and the
     * element and row group counts say how much the index grew. All are cumulative, so a round or
     * a query is measured as the difference between two samples.
     */
    private static final List<String> COUNTERS = ImmutableList.of(
            "warm_scheduled",
            "warm_started",
            "warm_accomplished",
            "warm_failed",
            "warm_skipped_due_key_conflict",
            "warm_skipped_due_reaching_threshold",
            "warmup_elements_count",
            "row_group_count");

    private final DistributedQueryRunner runner;
    private final String statsTable;

    WarpWarmup(DistributedQueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
        // the MBean's catalog property carries a hash suffix, so resolve the table name once
        this.statsTable = (String) runner.execute(
                "SELECT table_name FROM jmx.information_schema.tables" +
                        " WHERE table_schema = 'current' AND table_name LIKE 'io.trino.plugin.warp.gen.stats:%warmingservicestats%'").getOnlyValue();
        log.info("Warp warming stats table: %s", statsTable);
    }

    /**
     * Runs a query until a round warms nothing further.
     */
    void warmQuery(String query, String sql)
    {
        long startNanos = System.nanoTime();
        Map<String, Long> beforeQuery = sample();
        int round = 1;
        for (; round <= MAX_ROUNDS; round++) {
            Map<String, Long> beforeRound = sample();
            runner.execute(sql);
            awaitWarmingIdle();
            Map<String, Long> afterRound = sample();
            // a round without a new warmup element leaves the index as the next round would find it
            if (delta(beforeRound, afterRound, "warm_accomplished") == 0) {
                break;
            }
        }
        if (round > MAX_ROUNDS) {
            log.warn("%s: warp warming still active after %d rounds, measuring anyway", query, MAX_ROUNDS);
            round = MAX_ROUNDS;
        }
        Map<String, Long> afterQuery = sample();
        log.info(
                "%s warp warmup: %d rounds in %d s, scheduled=%d started=%d accomplished=%d failed=%d skipped=%d, warming created %d elements over %d row groups",
                query,
                round,
                (System.nanoTime() - startNanos) / 1_000_000_000,
                delta(beforeQuery, afterQuery, "warm_scheduled"),
                delta(beforeQuery, afterQuery, "warm_started"),
                delta(beforeQuery, afterQuery, "warm_accomplished"),
                delta(beforeQuery, afterQuery, "warm_failed"),
                delta(beforeQuery, afterQuery, "warm_skipped_due_key_conflict") + delta(beforeQuery, afterQuery, "warm_skipped_due_reaching_threshold"),
                delta(beforeQuery, afterQuery, "warmup_elements_count"),
                delta(beforeQuery, afterQuery, "row_group_count"));
    }

    private static long delta(Map<String, Long> before, Map<String, Long> after, String counter)
    {
        return after.get(counter) - before.get(counter);
    }

    /**
     * Warming runs asynchronously after query execution, so idle means the warming counters stay
     * unchanged across consecutive polls.
     */
    private void awaitWarmingIdle()
    {
        long deadline = System.nanoTime() + ROUND_TIMEOUT_NANOS;
        Map<String, Long> previous = ImmutableMap.of();
        int stablePolls = 0;
        while (true) {
            Map<String, Long> current = sample();
            if (current.equals(previous)) {
                stablePolls++;
                if (stablePolls == IDLE_STABLE_POLLS) {
                    return;
                }
            }
            else {
                stablePolls = 0;
                previous = current;
            }
            if (System.nanoTime() - deadline > 0) {
                log.warn("Warp warming did not go idle within the round timeout, moving on");
                return;
            }
            try {
                Thread.sleep(IDLE_POLL_MILLIS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for warp warming", e);
            }
        }
    }

    private Map<String, Long> sample()
    {
        String projections = COUNTERS.stream()
                .map(counter -> format("coalesce(sum(%s), 0)", counter))
                .collect(joining(", "));
        MaterializedRow row = runner.execute(format("SELECT %s FROM jmx.current.\"%s\"", projections, statsTable))
                .getMaterializedRows()
                .getFirst();
        return COUNTERS.stream()
                .collect(toImmutableMap(counter -> counter, counter -> ((Number) row.getField(COUNTERS.indexOf(counter))).longValue()));
    }
}
