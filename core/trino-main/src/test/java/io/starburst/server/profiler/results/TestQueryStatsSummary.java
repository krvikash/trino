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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.spi.QueryId;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfoWithWorkerNodes;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryStats;
import static io.starburst.server.profiler.rules.RuleTestSupport.getFinalQueryInfo;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryStatsSummary
{
    @Test
    public void testWorkerlessQueryZeroesPerWorkerMetrics()
    {
        DataSize outputDataSize = DataSize.of(100, DataSize.Unit.KILOBYTE);
        Duration totalCpu = new Duration(10, SECONDS);
        Duration executionTime = new Duration(5, SECONDS);

        QueryInfo info = queryInfo(
                jdbcScan("root"),
                ImmutableList.of(),
                ImmutableMap.of(),
                queryStats(outputDataSize, totalCpu, totalCpu, executionTime, ImmutableList.of()),
                ImmutableList.of());

        QueryStatsSummary summary = QueryStatsSummaryFactory.from(info);

        assertThat(summary.workerCount()).isZero();
        assertThat(summary.cpuLoadAveragePerWorker()).isZero();
        assertThat(summary.averageNetworkPerWorkerPerSecond().toBytes()).isZero();
        assertThat(summary.finishingTimeExcessive()).isFalse();
        assertThat(summary.totalCpuTime()).isEqualTo(totalCpu);
        assertThat(summary.executionTime()).isEqualTo(executionTime);
        assertThat(summary.outputDataSize().toBytes()).isEqualTo(outputDataSize.toBytes());
        assertThat(summary.totalNetworkDataSize().toBytes()).isZero();
        assertThat(summary.createTime()).isNotNull();
    }

    @Test
    public void testPerWorkerNetworkRateBelowOneBytePerMillisIsNotTruncated()
    {
        Duration executionTime = new Duration(1, SECONDS);
        QueryStats stats = queryStats(
                DataSize.ofBytes(0),
                new Duration(1, SECONDS),
                new Duration(1, SECONDS),
                executionTime,
                DataSize.ofBytes(0),
                DataSize.ofBytes(500),
                ImmutableList.of());

        QueryStatsSummary summary = QueryStatsSummaryFactory.from(
                queryInfoWithWorkerNodes(stats, ImmutableList.of("worker1")));

        assertThat(summary.workerCount()).isEqualTo(1);
        assertThat(summary.totalNetworkDataSize().toBytes()).isEqualTo(500);
        assertThat(summary.averageNetworkPerWorkerPerSecond().toBytes()).isEqualTo(500);
    }

    @Test
    public void testCpuLoadAveragePerWorkerAcrossWorkers()
    {
        Duration executionTime = new Duration(10, SECONDS);
        Duration totalCpu = new Duration(10, SECONDS);
        QueryStats stats = queryStats(
                DataSize.ofBytes(0),
                totalCpu,
                totalCpu,
                executionTime,
                ImmutableList.of());

        QueryStatsSummary summary = QueryStatsSummaryFactory.from(
                queryInfoWithWorkerNodes(stats, ImmutableList.of("worker1", "worker2")));

        assertThat(summary.workerCount()).isEqualTo(2);
        assertThat(summary.cpuLoadAveragePerWorker()).isEqualTo(0.5);
    }

    @Test
    public void testZeroExecutionTimeScalesRawNetworkBytes()
    {
        QueryStats stats = queryStats(
                DataSize.ofBytes(0),
                new Duration(1, SECONDS),
                new Duration(1, SECONDS),
                new Duration(0, SECONDS),
                DataSize.ofBytes(0),
                DataSize.ofBytes(1),
                ImmutableList.of());

        QueryStatsSummary summary = QueryStatsSummaryFactory.from(
                queryInfoWithWorkerNodes(stats, ImmutableList.of("worker1")));

        assertThat(summary.workerCount()).isEqualTo(1);
        assertThat(summary.cpuLoadAveragePerWorker()).isZero();
        assertThat(summary.averageNetworkPerWorkerPerSecond().toBytes()).isEqualTo(1000);
    }

    @Test
    public void testSummaryFromExecutedQuery()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            QueryId queryId = queryRunner.executeWithQueryId(
                    queryRunner.getDefaultSession(),
                    "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey").queryId();
            QueryInfo queryInfo = getFinalQueryInfo(queryRunner, queryId);

            QueryStatsSummary summary = QueryStatsSummaryFactory.from(queryInfo);

            assertThat(summary.workerCount()).isGreaterThanOrEqualTo(1);
            assertThat(summary.totalDrivers()).isPositive();
            assertThat(summary.cpuLoadAveragePerWorker()).isGreaterThanOrEqualTo(0.0);
            assertThat(summary.totalNetworkDataSize().toBytes())
                    .isGreaterThanOrEqualTo(summary.physicalInputDataSize().toBytes());
            assertThat(summary.createTime()).isNotNull();
            assertThat(summary.endTime()).isNotNull();
            assertThat(summary.endTime()).isAfterOrEqualTo(summary.createTime());
        }
    }
}
