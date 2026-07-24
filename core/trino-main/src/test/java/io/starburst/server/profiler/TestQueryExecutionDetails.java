/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.common.collect.ImmutableSet;
import io.starburst.server.profiler.results.OperatorStatsSummary;
import io.starburst.server.profiler.results.QueryProfilerResult;
import io.starburst.server.profiler.results.QuerySummary;
import io.starburst.server.profiler.results.StageSummary;
import io.starburst.server.profiler.rules.RuleTestSupport;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryExecutionDetails
{
    @Language("SQL")
    private static final String JOIN_QUERY = "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey";

    @Test
    public void testPopulatesFlatPlanNodeListWithStatsForJoinQuery()
    {
        try (StandaloneQueryRunner queryRunner = ProfilerQueryRunner.create(_ -> {})) {
            QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, JOIN_QUERY).queryId();
            QueryInfo queryInfo = RuleTestSupport.getFinalQueryInfo(queryRunner, queryId);

            QueryExecutionDetails details = new QueryExecutionDetails(queryInfo, new QueryProfilerConfig());

            assertThat(details.planNodesById().values()).isNotEmpty();
            assertThat(details.rootNode()).isPresent();
            assertThat(details.planNodesById().values())
                    .as("at least one plan node has stats attached")
                    .anyMatch(node -> details.stats(node.getId()).isPresent());
        }
    }

    @Test
    public void testProfilerCapturesFinalResultForJoinQuery()
    {
        CapturingResultSink sink = new CapturingResultSink();
        try (StandaloneQueryRunner queryRunner = ProfilerQueryRunner.create(sink)) {
            QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, JOIN_QUERY).queryId();

            assertEventually(() -> assertThat(sink.results).containsKey(queryId.toString()));

            QuerySummary summary = sink.results.get(queryId.toString()).querySummary();
            assertThat(summary.stages()).isNotEmpty();
            assertThat(summary.operators()).isNotEmpty();

            Set<Integer> topOperatorIds = ImmutableSet.<Integer>builder()
                    .addAll(summary.topOperatorsByCpuTime())
                    .addAll(summary.topOperatorsByScheduledTime())
                    .build();
            assertThat(summary.operators().stream().map(OperatorStatsSummary::id).collect(toImmutableSet()))
                    .containsExactlyInAnyOrderElementsOf(topOperatorIds);

            Set<Integer> validStageIds = summary.stages().stream().map(StageSummary::stageId).collect(toImmutableSet());
            assertThat(summary.topStagesByCpuTime()).allMatch(validStageIds::contains);
            assertThat(summary.topStagesByScheduledTime()).allMatch(validStageIds::contains);
            assertThat(summary.topStagesByPeakMemory()).allMatch(validStageIds::contains);
            assertThat(summary.topStagesByNetworkData()).allMatch(validStageIds::contains);
        }
    }

    private static final class CapturingResultSink
            implements ProfilerResultSink
    {
        public final Map<String, QueryProfilerResult> results = new ConcurrentHashMap<>();

        @Override
        public void accept(QueryProfilerResult result)
        {
            results.put(result.queryId(), result);
        }
    }
}
