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
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.trino.spi.QueryId;

import static io.trino.tracing.TrinoAttributes.QUERY_ID;

public class TroubleshootingSpanProcessor
        implements SpanProcessor
{
    private final SpanInterceptor spanInterceptor;

    @Inject
    public TroubleshootingSpanProcessor(SpanInterceptor spanInterceptor)
    {
        this.spanInterceptor = spanInterceptor;
    }

    @Override
    public void onStart(Context context, ReadWriteSpan readWriteSpan)
    {
        String queryId = readWriteSpan.getAttribute(QUERY_ID);
        if (null != queryId) {
            spanInterceptor.keepTracking(QueryId.valueOf(queryId), readWriteSpan.getSpanContext().getTraceId());
        }
    }

    @Override
    public boolean isStartRequired()
    {
        return true;
    }

    @Override
    public void onEnd(ReadableSpan readableSpan)
    {
        spanInterceptor.saveSpanOnlyIfRequested(readableSpan);
    }

    @Override
    public boolean isEndRequired()
    {
        return true;
    }
}
