/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.QueryProfilerConfig;
import io.starburst.server.profiler.results.PlanNodeFinding;
import io.starburst.server.profiler.results.QueryFinding;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.results.StageFinding;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.type.TypeDescriptor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.type.TypeDescriptorDeserializer;
import io.trino.type.TypeDescriptorKeyDeserializer;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public final class RuleTestSupport
{
    private static final JsonCodec<QueryInfo> QUERY_INFO_CODEC = new JsonCodecFactory(
            new JsonMapperProvider()
                    .withJsonSerializers(Map.of(Span.class, new SpanSerializer(OpenTelemetry.noop())))
                    .withJsonDeserializers(Map.of(
                            Span.class, new SpanDeserializer(OpenTelemetry.noop()),
                            TypeDescriptor.class, new TypeDescriptorDeserializer()))
                    .withKeyDeserializers(Map.of(
                            TypeDescriptor.class, new TypeDescriptorKeyDeserializer(),
                            Symbol.class, new SymbolKeyDeserializer(TESTING_TYPE_MANAGER)))
                    .get())
            .jsonCodec(QueryInfo.class);

    private RuleTestSupport() {}

    public static StandaloneQueryRunner tpchQueryRunner()
    {
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().orElseThrow(), "tpch", ImmutableMap.of());
        return queryRunner;
    }

    public static List<RuleFinding> findings(
            QueryProfilerRule rule,
            StandaloneQueryRunner queryRunner,
            Session session,
            @Language("SQL") String sql,
            UnaryOperator<QueryProfilerConfig> configCustomizer)
    {
        QueryId queryId = queryRunner.executeWithQueryId(session, sql).queryId();
        QueryInfo queryInfo = getFinalQueryInfo(queryRunner, queryId);
        return analyze(rule, queryInfo, configCustomizer);
    }

    public static QueryInfo getFinalQueryInfo(StandaloneQueryRunner queryRunner, QueryId queryId)
    {
        CountDownLatch finalQueryInfoLatch = new CountDownLatch(1);
        AtomicReference<QueryInfo> finalQueryInfo = new AtomicReference<>();
        queryRunner.getCoordinator().addFinalQueryInfoListener(queryId, queryInfo -> {
            finalQueryInfo.set(queryInfo);
            finalQueryInfoLatch.countDown();
        });
        try {
            if (!finalQueryInfoLatch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for final query info for query " + queryId);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return finalQueryInfo.get();
    }

    /**
     * Replays a {@link QueryInfo} JSON fixture captured from a real run
     * instead of executing a query live.
     */
    public static List<RuleFinding> findings(
            QueryProfilerRule rule,
            String resourcePath,
            UnaryOperator<QueryProfilerConfig> configCustomizer)
    {
        return analyze(rule, readQueryInfo(resourcePath), configCustomizer);
    }

    /**
     * Analyzes a {@link QueryInfo} assembled in-test from characteristic parts (see
     * {@link ProfilerFixtures}) for rules that cannot be provoked by a live TPCH query.
     */
    public static List<RuleFinding> findings(
            QueryProfilerRule rule,
            QueryInfo queryInfo,
            UnaryOperator<QueryProfilerConfig> configCustomizer)
    {
        return analyze(rule, queryInfo, configCustomizer);
    }

    private static List<RuleFinding> analyze(QueryProfilerRule rule, QueryInfo queryInfo, UnaryOperator<QueryProfilerConfig> configCustomizer)
    {
        QueryProfilerConfig config = configCustomizer.apply(new QueryProfilerConfig());
        return rule.analyze(new QueryExecutionDetails(queryInfo, config));
    }

    private static QueryInfo readQueryInfo(String resourcePath)
    {
        try {
            return QUERY_INFO_CODEC.fromJson(Resources.toByteArray(Resources.getResource(resourcePath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static PlanNodeFinding onlyPlanNodeFinding(List<RuleFinding> findings)
    {
        return (PlanNodeFinding) getOnlyElement(findings);
    }

    public static StageFinding onlyStageFinding(List<RuleFinding> findings)
    {
        return (StageFinding) getOnlyElement(findings);
    }

    public static QueryFinding onlyQueryFinding(List<RuleFinding> findings)
    {
        return (QueryFinding) getOnlyElement(findings);
    }
}
