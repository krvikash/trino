/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.server.castleinsights.CastleQueryAnalysis;
import io.starburst.server.castleinsights.model.CastleInsight;
import io.starburst.server.castleinsights.model.CastleInsightsResult;
import io.starburst.server.castleinsights.model.JoinInsight;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.starburst.server.castleinsights.model.JoinType.INNER;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestCastleInsightsExtractor
{
    private static final NodeVersion NODE_VERSION = new NodeVersion("test-version");

    private StandaloneQueryRunner queryRunner;
    private QueryInfo queryInfo;

    @BeforeAll
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().orElseThrow(), "tpch", ImmutableMap.of());
        queryInfo = execute("SELECT n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey");
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        queryInfo = null;
    }

    @Test
    public void testNoAnalyzersYieldsEmptyResult()
    {
        CastleInsightsStats stats = new CastleInsightsStats();
        CastleInsightsResult result = extractor(stats, ImmutableSet.of()).process(queryInfo);

        assertThat(result.insights()).isEmpty();
        assertThat(result.queryId()).isEqualTo(queryInfo.getQueryId().toString());
        assertThat(result.trinoVersion()).isEqualTo("test-version");

        assertThat(stats.getAnalyzedQueries().getTotalCount()).isEqualTo(1);
        assertThat(stats.getEmittedInsights().getTotalCount()).isEqualTo(0);
        assertThat(stats.getFailedAnalyzers().getTotalCount()).isEqualTo(0);
    }

    @Test
    public void testCollectsInsightsFromEveryAnalyzer()
    {
        CastleInsightsStats stats = new CastleInsightsStats();
        CastleInsightsResult result = extractor(stats, ImmutableSet.of(
                fixedAnalyzer(joinInsight("orders")),
                fixedAnalyzer(joinInsight("lineitem"), joinInsight("customer"))))
                .process(queryInfo);

        assertThat(result.insights())
                .extracting(insight -> ((JoinInsight) insight).left().table())
                .containsExactlyInAnyOrder("orders", "lineitem", "customer");
        assertThat(stats.getEmittedInsights().getTotalCount()).isEqualTo(3);
        assertThat(stats.getFailedAnalyzers().getTotalCount()).isEqualTo(0);
    }

    @Test
    public void testFailingAnalyzerIsSkippedAndOthersStillRun()
    {
        CastleInsightsStats stats = new CastleInsightsStats();
        CastleInsightsResult result = extractor(stats, ImmutableSet.of(
                throwingAnalyzer(),
                fixedAnalyzer(joinInsight("orders"))))
                .process(queryInfo);

        assertThat(result.insights())
                .as("the surviving analyzer's insight is still reported")
                .hasSize(1);
        assertThat(stats.getFailedAnalyzers().getTotalCount()).isEqualTo(1);
        assertThat(stats.getAnalyzedQueries().getTotalCount())
                .as("the query still counts as analyzed")
                .isEqualTo(1);
    }

    @Test
    public void testAnalyzersSeeTheQueryPlan()
    {
        AtomicReference<CastleQueryAnalysis> seen = new AtomicReference<>();
        extractor(new CastleInsightsStats(), ImmutableSet.of(analysis -> {
            seen.set(analysis);
            return ImmutableList.of();
        })).process(queryInfo);

        assertThat(seen.get()).isNotNull();
        assertThat(seen.get().queryId()).isEqualTo(queryInfo.getQueryId().toString());
        assertThat(seen.get().rootNode()).isPresent();
    }

    private static CastleInsightsExtractor extractor(CastleInsightsStats stats, Set<CastleInsightAnalyzer> analyzers)
    {
        return new CastleInsightsExtractor(analyzers, NODE_VERSION, stats);
    }

    private static CastleInsightAnalyzer fixedAnalyzer(CastleInsight... insights)
    {
        List<CastleInsight> fixed = ImmutableList.copyOf(insights);
        return _ -> fixed;
    }

    private static CastleInsightAnalyzer throwingAnalyzer()
    {
        return _ -> {
            throw new IllegalStateException("analyzer blew up");
        };
    }

    private static JoinInsight joinInsight(String leftTable)
    {
        return new JoinInsight(
                new TableRef("tpch", "tiny", leftTable),
                ImmutableList.of("custkey"),
                new TableRef("tpch", "tiny", "customer"),
                ImmutableList.of("custkey"),
                INNER,
                10,
                5,
                5);
    }

    private QueryInfo execute(String sql)
    {
        QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, sql).queryId();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<QueryInfo> result = new AtomicReference<>();
        queryRunner.getCoordinator().addFinalQueryInfoListener(queryId, info -> {
            result.set(info);
            latch.countDown();
        });
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for final query info for query " + queryId);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return result.get();
    }
}
