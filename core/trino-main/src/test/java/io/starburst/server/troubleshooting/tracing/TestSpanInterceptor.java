/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.server.ServerConfig;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import static io.trino.tracing.TrinoAttributes.QUERY_ID;
import static org.assertj.core.api.Assertions.assertThat;

class TestSpanInterceptor
{
    @Test
    void testWorkerDoesNotTrackQueriesWithoutCollectRequest()
    {
        SpanInterceptor interceptor = spanInterceptor(false);
        try (SdkTracerProvider tracerProvider = tracerProvider(interceptor)) {
            Tracer tracer = tracerProvider.get("test");
            for (int i = 0; i < 10; i++) {
                runQueryTrace(tracer, new QueryId("untracked_" + i));
            }
        }

        assertThat(interceptor.getTrackedTraceCount()).isZero();
        assertThat(interceptor.getCollectedSpans(new QueryId("untracked_0"))).isEmpty();
    }

    @Test
    void testWorkerCollectsAndForgetsRequestedQuery()
    {
        SpanInterceptor interceptor = spanInterceptor(false);
        QueryId requested = new QueryId("requested");
        QueryId untracked = new QueryId("untracked");
        interceptor.requestSpanCollect(requested);
        try (SdkTracerProvider tracerProvider = tracerProvider(interceptor)) {
            Tracer tracer = tracerProvider.get("test");
            runQueryTrace(tracer, requested);
            runQueryTrace(tracer, untracked);
        }

        assertThat(interceptor.getTrackedTraceCount()).isEqualTo(1);
        assertThat(interceptor.getCollectedSpans(requested))
                .extracting(SpanData::getName)
                .containsExactlyInAnyOrder("task", "child-without-query-id");
        assertThat(interceptor.getCollectedSpans(untracked)).isEmpty();

        interceptor.remove(requested);
        assertThat(interceptor.getTrackedTraceCount()).isZero();
        assertThat(interceptor.getCollectedSpans(requested)).isEmpty();
    }

    @Test
    void testCoordinatorTracksQueriesBeforeCollectRequest()
    {
        SpanInterceptor interceptor = spanInterceptor(true);
        QueryId queryId = new QueryId("coordinator");
        try (SdkTracerProvider tracerProvider = tracerProvider(interceptor)) {
            Tracer tracer = tracerProvider.get("test");
            // The query span starts before troubleshooting requests span collection
            Span querySpan = tracer.spanBuilder("query")
                    .setAttribute(QUERY_ID, queryId.id())
                    .startSpan();
            interceptor.requestSpanCollect(queryId);
            tracer.spanBuilder("planner")
                    .setParent(Context.current().with(querySpan))
                    .startSpan()
                    .end();
            querySpan.end();
        }

        assertThat(interceptor.getCollectedSpans(queryId))
                .extracting(SpanData::getName)
                .containsExactlyInAnyOrder("query", "planner");

        interceptor.forgetTracking(queryId);
        assertThat(interceptor.getTrackedTraceCount()).isZero();
    }

    private static void runQueryTrace(Tracer tracer, QueryId queryId)
    {
        Span taskSpan = tracer.spanBuilder("task")
                .setAttribute(QUERY_ID, queryId.id())
                .startSpan();
        tracer.spanBuilder("child-without-query-id")
                .setParent(Context.current().with(taskSpan))
                .startSpan()
                .end();
        taskSpan.end();
    }

    private static SpanInterceptor spanInterceptor(boolean coordinator)
    {
        return new SpanInterceptor(SpanSerializer::new, new ServerConfig().setCoordinator(coordinator));
    }

    private static SdkTracerProvider tracerProvider(SpanInterceptor interceptor)
    {
        return SdkTracerProvider.builder()
                .addSpanProcessor(new TroubleshootingSpanProcessor(interceptor))
                .build();
    }
}
