/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.results;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.QueryProfilerConfig;
import io.starburst.server.profiler.rules.ProfilerFixtures;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.operator.ResourceUsageTimeSeriesRecorder.ResourceUsageTimeSeriesSnapshot;
import io.trino.plugin.base.metrics.DistributionSnapshot;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryStats;
import static io.starburst.server.profiler.rules.RuleTestSupport.getFinalQueryInfo;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQuerySummary
{
    private static final Duration ZERO = new Duration(0, NANOSECONDS);
    private static final DataSize ZERO_BYTES = DataSize.ofBytes(0);

    @Test
    public void testTopOperatorsByCpuTimeStopsAtCumulativeThreshold()
    {
        // operators: 6s + 3s + 1s = 10s total
        // threshold 80%: after 2nd operator cumulative is 90%, so only 2 should appear
        Duration totalCpu = new Duration(10, SECONDS);
        List<OperatorStats> operators = ImmutableList.of(
                operatorWithCpuTime("1", 6_000_000_000L),
                operatorWithCpuTime("2", 3_000_000_000L),
                operatorWithCpuTime("3", 1_000_000_000L));

        QueryInfo info = queryInfo(
                jdbcScan("root"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(ZERO_BYTES, totalCpu, totalCpu, ZERO, operators),
                ImmutableList.of());

        QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(info, new QueryProfilerConfig()));

        assertThat(summary.topOperatorsByCpuTime()).hasSize(2);

        Map<Integer, OperatorStatsSummary> operatorsById = summary.operators().stream().collect(toImmutableMap(OperatorStatsSummary::id, o -> o));
        List<Integer> topIds = summary.topOperatorsByCpuTime();
        assertThat(operatorsById.get(topIds.get(0)).cpuTime().roundTo(NANOSECONDS))
                .isGreaterThanOrEqualTo(operatorsById.get(topIds.get(1)).cpuTime().roundTo(NANOSECONDS));
    }

    @Test
    public void testTopOperatorsIsLimitedByMaximum()
    {
        Duration totalCpu = new Duration(10, SECONDS);
        List<OperatorStats> operators = ImmutableList.of(
                operatorWithCpuTime("1", 2_000_000_000L),
                operatorWithCpuTime("2", 2_000_000_000L),
                operatorWithCpuTime("3", 2_000_000_000L),
                operatorWithCpuTime("4", 2_000_000_000L),
                operatorWithCpuTime("5", 2_000_000_000L));

        QueryInfo info = queryInfo(
                jdbcScan("root"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(ZERO_BYTES, totalCpu, totalCpu, ZERO, operators),
                ImmutableList.of());

        QueryProfilerConfig config = new QueryProfilerConfig()
                .setMaxTopOperators(2)
                .setTopOperatorsPercentage(1.0);

        QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(info, config));

        assertThat(summary.topOperatorsByCpuTime()).hasSize(2);
        assertThat(summary.topOperatorsByScheduledTime()).hasSize(2);
    }

    @Test
    public void testColumnIndexUsageFromWarpMatchMetrics()
    {
        Metrics warpMatchMetrics = new Metrics(ImmutableMap.of(
                "warp-match:col1:WARM_UP_TYPE_BASIC", new DistributionSnapshot(80L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
        OperatorStats op = operatorWithConnectorMetrics("root", 100L, warpMatchMetrics);

        Duration totalCpu = new Duration(1, SECONDS);
        QueryInfo info = queryInfo(
                jdbcScan("root"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(ZERO_BYTES, totalCpu, totalCpu, ZERO, ImmutableList.of(op)),
                ImmutableList.of());

        QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(info, new QueryProfilerConfig()));

        assertThat(summary.planNodes()).anySatisfy(node -> {
            assertThat(node.columnIndexUsage()).isNotEmpty();
            assertThat(node.columnIndexUsage().getFirst().columnName()).isEqualTo("col1");
            assertThat(node.columnIndexUsage().getFirst().indexUsedCount()).isEqualTo(80L);
            assertThat(node.columnIndexUsage().getFirst().totalReads()).isEqualTo(100L);
        });
    }

    @Test
    public void testColumnIndexUsageEmptyWhenNoWarpMatchMetrics()
    {
        OperatorStats op = ProfilerFixtures.operator("root", "TableScanOperator", 1);

        Duration totalCpu = new Duration(1, SECONDS);
        QueryInfo info = queryInfo(
                jdbcScan("root"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(ZERO_BYTES, totalCpu, totalCpu, ZERO, ImmutableList.of(op)),
                ImmutableList.of());

        QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(info, new QueryProfilerConfig()));

        assertThat(summary.planNodes())
                .allSatisfy(node -> assertThat(node.columnIndexUsage()).isEmpty());
    }

    @Test
    public void testOperatorsInSummaryMatchTopOperatorLists()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            QueryId queryId = queryRunner.executeWithQueryId(
                    queryRunner.getDefaultSession(),
                    "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey").queryId();
            QueryInfo queryInfo = getFinalQueryInfo(queryRunner, queryId);

            QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(queryInfo, new QueryProfilerConfig()));

            Set<Integer> topIds = ImmutableSet.<Integer>builder()
                    .addAll(summary.topOperatorsByCpuTime())
                    .addAll(summary.topOperatorsByScheduledTime())
                    .build();

            assertThat(summary.operators().stream().map(OperatorStatsSummary::id).collect(toImmutableSet()))
                    .containsExactlyInAnyOrderElementsOf(topIds);
        }
    }

    @Test
    public void testTopStageIdsReferenceValidStages()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            QueryId queryId = queryRunner.executeWithQueryId(
                    queryRunner.getDefaultSession(),
                    "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey").queryId();
            QueryInfo queryInfo = getFinalQueryInfo(queryRunner, queryId);

            QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(queryInfo, new QueryProfilerConfig()));

            Set<Integer> validStageIds = summary.stages().stream().map(StageSummary::stageId).collect(toImmutableSet());

            assertThat(summary.topStagesByCpuTime()).allMatch(validStageIds::contains);
            assertThat(summary.topStagesByScheduledTime()).allMatch(validStageIds::contains);
            assertThat(summary.topStagesByPeakMemory()).allMatch(validStageIds::contains);
            assertThat(summary.topStagesByNetworkData()).allMatch(validStageIds::contains);
        }
    }

    @Test
    public void testResourceUsageTimeSeriesByOperatorType()
    {
        Metrics tableScanMetrics = new Metrics(ImmutableMap.of(
                "CPU and scheduled time usage over time", ResourceUsageTimeSeriesSnapshot.create(1000, 1, new long[] {1_000_000, 2_000_000}, new long[] {3_000_000, 4_000_000})));
        Metrics filterMetrics = new Metrics(ImmutableMap.of(
                "CPU and scheduled time usage over time", ResourceUsageTimeSeriesSnapshot.create(1000, 1, new long[] {5_000_000}, new long[] {6_000_000})));
        List<OperatorStats> operators = ImmutableList.of(
                operatorWithMetrics("1", "TableScanOperator", tableScanMetrics),
                operatorWithMetrics("2", "FilterOperator", filterMetrics));

        Duration totalCpu = new Duration(10, SECONDS);
        QueryInfo info = queryInfo(
                jdbcScan("1"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(ZERO_BYTES, totalCpu, totalCpu, ZERO, operators),
                ImmutableList.of());

        QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(info, new QueryProfilerConfig()));

        ResourceUsageTimeSeries timeSeries = summary.resourceUsageTimeSeriesByOperatorType();
        assertThat(timeSeries.startTimeEpochSeconds()).isEqualTo(1000);
        assertThat(timeSeries.bucketWidthSeconds()).isEqualTo(1);
        assertThat(timeSeries.bucketCount()).isEqualTo(2);
        assertThat(timeSeries.operatorTypes()).containsExactly("FilterOperator", "TableScanOperator");
        assertThat(timeSeries.cpuMillisByOperatorType().get("TableScanOperator")).containsExactly(1.0, 2.0);
        assertThat(timeSeries.wallMillisByOperatorType().get("TableScanOperator")).containsExactly(3.0, 4.0);
        assertThat(timeSeries.cpuMillisByOperatorType().get("FilterOperator")).containsExactly(5.0, 0.0);
        assertThat(timeSeries.wallMillisByOperatorType().get("FilterOperator")).containsExactly(6.0, 0.0);
    }

    @Test
    public void testResourceUsageTimeSeriesEmptyWhenNoSnapshots()
    {
        OperatorStats op = ProfilerFixtures.operator("root", "TableScanOperator", 1);

        Duration totalCpu = new Duration(1, SECONDS);
        QueryInfo info = queryInfo(
                jdbcScan("root"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(ZERO_BYTES, totalCpu, totalCpu, ZERO, ImmutableList.of(op)),
                ImmutableList.of());

        QuerySummary summary = QuerySummaryFactory.from(new QueryExecutionDetails(info, new QueryProfilerConfig()));

        ResourceUsageTimeSeries timeSeries = summary.resourceUsageTimeSeriesByOperatorType();
        assertThat(timeSeries.bucketCount()).isEqualTo(0);
        assertThat(timeSeries.operatorTypes()).isEmpty();
        assertThat(timeSeries.cpuMillisByOperatorType()).isEmpty();
        assertThat(timeSeries.wallMillisByOperatorType()).isEmpty();
    }

    private static OperatorStats operatorWithMetrics(String planNodeId, String operatorType, Metrics metrics)
    {
        Duration cpu = new Duration(1, SECONDS);
        return new OperatorStats(
                0,
                0,
                0,
                0,
                new PlanNodeId(planNodeId),
                Optional.empty(),
                operatorType,
                1,
                0,
                cpu,
                cpu,
                ZERO_BYTES,
                0,
                ZERO,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                0.0,
                0,
                ZERO,
                ZERO,
                ZERO_BYTES,
                0,
                0,
                metrics,
                Metrics.EMPTY,
                Metrics.EMPTY,
                ZERO_BYTES,
                ZERO,
                0,
                ZERO,
                ZERO,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                Optional.empty(),
                null);
    }

    private static OperatorStats operatorWithCpuTime(String planNodeId, long cpuNanos)
    {
        Duration cpu = new Duration(cpuNanos, NANOSECONDS);
        return new OperatorStats(
                0,
                0,
                0,
                0,
                new PlanNodeId(planNodeId),
                Optional.empty(),
                "TestOperator",
                1,
                0,
                cpu,
                cpu,
                ZERO_BYTES,
                0,
                ZERO,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                0.0,
                0,
                ZERO,
                ZERO,
                ZERO_BYTES,
                0,
                0,
                Metrics.EMPTY,
                Metrics.EMPTY,
                Metrics.EMPTY,
                ZERO_BYTES,
                ZERO,
                0,
                ZERO,
                ZERO,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                Optional.empty(),
                null);
    }

    private static OperatorStats operatorWithConnectorMetrics(String planNodeId, long totalDrivers, Metrics connectorMetrics)
    {
        Duration cpu = new Duration(1, SECONDS);
        return new OperatorStats(
                0,
                0,
                0,
                0,
                new PlanNodeId(planNodeId),
                Optional.empty(),
                "TableScanOperator",
                totalDrivers,
                0,
                cpu,
                cpu,
                ZERO_BYTES,
                0,
                ZERO,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                0.0,
                0,
                ZERO,
                ZERO,
                ZERO_BYTES,
                0,
                0,
                Metrics.EMPTY,
                connectorMetrics,
                Metrics.EMPTY,
                ZERO_BYTES,
                ZERO,
                0,
                ZERO,
                ZERO,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                Optional.empty(),
                null);
    }
}
