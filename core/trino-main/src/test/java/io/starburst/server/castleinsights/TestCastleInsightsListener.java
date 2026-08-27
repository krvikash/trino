/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.server.castleinsights.analyzer.CastleInsightAnalyzer;
import io.starburst.server.castleinsights.model.CastleInsight;
import io.starburst.server.castleinsights.model.CastleInsightsResult;
import io.starburst.server.castleinsights.model.JoinInsight;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.starburst.server.castleinsights.model.JoinType.INNER;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCastleInsightsListener
{
    @Language("SQL")
    private static final String JOIN_QUERY = "SELECT n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey";

    @Test
    public void testInsightsReachTheSink()
    {
        CapturingSink sink = new CapturingSink();
        try (StandaloneQueryRunner queryRunner = createQueryRunner(sink, new FixedAnalyzer())) {
            QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, JOIN_QUERY).queryId();

            assertEventually(() -> assertThat(sink.results).containsKey(queryId.toString()));

            CastleInsightsResult result = sink.results.get(queryId.toString());
            assertThat(result.insights()).hasSize(1);
        }
    }

    @Test
    public void testRunsWithoutAnyAnalyzerBound()
    {
        CapturingSink sink = new CapturingSink();
        try (StandaloneQueryRunner queryRunner = createQueryRunner(sink)) {
            QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, JOIN_QUERY).queryId();

            assertEventually(() -> assertThat(sink.results).containsKey(queryId.toString()));
            assertThat(sink.results.get(queryId.toString()).insights()).isEmpty();
        }
    }

    @Test
    public void testSkipsNonAnalyzableQuery()
    {
        CapturingSink sink = new CapturingSink();
        try (StandaloneQueryRunner queryRunner = createQueryRunner(sink, new FixedAnalyzer())) {
            QueryId explainId = queryRunner.executeWithQueryId(TEST_SESSION, "EXPLAIN " + JOIN_QUERY).queryId();
            QueryId joinId = queryRunner.executeWithQueryId(TEST_SESSION, JOIN_QUERY).queryId();

            // Extraction is single threaded and FIFO, so the join result landing means an EXPLAIN
            // task submitted before it would already have run.
            assertEventually(() -> assertThat(sink.results).containsKey(joinId.toString()));
            assertThat(sink.results).doesNotContainKey(explainId.toString());
        }
    }

    private static StandaloneQueryRunner createQueryRunner(CastleInsightsResultSink sink, CastleInsightAnalyzer... analyzers)
    {
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                        {
                            @Override
                            protected void setup(Binder binder)
                            {
                                newOptionalBinder(binder, CastleInsightsResultSink.class).setBinding().toInstance(sink);
                                Multibinder<CastleInsightAnalyzer> analyzerBinder = Multibinder.newSetBinder(binder, CastleInsightAnalyzer.class);
                                for (CastleInsightAnalyzer analyzer : analyzers) {
                                    analyzerBinder.addBinding().toInstance(analyzer);
                                }
                            }
                        })
                        .setProperties(ImmutableMap.of(
                                "castle-insights.enabled", "true",
                                // One thread over a FIFO queue, so a query submitted earlier is always
                                // extracted earlier, which is what lets the skip assertion below be exact.
                                "castle-insights.thread-pool-size", "1")));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().orElseThrow(), "tpch", ImmutableMap.of());
        return queryRunner;
    }

    private static final class CapturingSink
            implements CastleInsightsResultSink
    {
        private final Map<String, CastleInsightsResult> results = new ConcurrentHashMap<>();

        @Override
        public void accept(CastleInsightsResult result)
        {
            results.put(result.queryId(), result);
        }
    }

    private static final class FixedAnalyzer
            implements CastleInsightAnalyzer
    {
        @Override
        public List<CastleInsight> analyze(CastleQueryAnalysis analysis)
        {
            return ImmutableList.of(new JoinInsight(
                    new TableRef("tpch", "tiny", "nation"),
                    ImmutableList.of("regionkey"),
                    new TableRef("tpch", "tiny", "region"),
                    ImmutableList.of("regionkey"),
                    INNER,
                    25,
                    5,
                    25));
        }
    }
}
