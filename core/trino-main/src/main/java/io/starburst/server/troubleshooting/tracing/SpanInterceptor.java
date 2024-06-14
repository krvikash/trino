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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class SpanInterceptor
{
    private final Provider<SpanSerializer> spanSerializerProvider;
    private ConcurrentMap<String, QueryId> queryIdByTraceId = new ConcurrentHashMap<>();
    private ConcurrentMap<QueryId, Set<SpanData>> spansByQueryIds = new ConcurrentHashMap<>();
    private ConcurrentMap<QueryId, QueryId> collectSpansForTheseQueryIds = new ConcurrentHashMap<>();

    @Inject
    public SpanInterceptor(Provider<SpanSerializer> spanSerializerProvider)
    {
        this.spanSerializerProvider = requireNonNull(spanSerializerProvider, "spanSerializerProvider is null");
    }

    public void requestSpanCollect(QueryId queryId)
    {
        collectSpansForTheseQueryIds.put(queryId, queryId);
    }

    public void keepTracking(QueryId queryId, String traceId)
    {
        queryIdByTraceId.put(traceId, queryId);
    }

    public void saveSpanOnlyIfRequested(ReadableSpan span)
    {
        QueryId queryId = queryIdByTraceId.get(span.getSpanContext().getTraceId());
        if (null != queryId) {
            if (null != collectSpansForTheseQueryIds.get(queryId)) {
                Set<SpanData> spanDatas = spansByQueryIds.computeIfAbsent(queryId, q -> ConcurrentHashMap.newKeySet());
                SpanData spanData = span.toSpanData();
                spanDatas.add(spanData);
                spansByQueryIds.put(queryId, spanDatas);
            }
        }
    }

    public void forgetTracking(QueryId queryId)
    {
        collectSpansForTheseQueryIds.remove(queryId);
        queryIdByTraceId.entrySet().removeIf(entry -> entry.getValue().equals(queryId));
    }

    public InputStream getSpans(QueryId queryId)
    {
        return spanSerializerProvider.get().execute(Optional.ofNullable(spansByQueryIds.get(queryId)).orElse(emptySet()));
    }

    public void remove(QueryId queryId)
    {
        forgetTracking(queryId);
        spansByQueryIds.remove(queryId);
    }
}
