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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;

import java.io.InputStream;
import java.util.Map;

public class OpenTelemetryTraceProvider
        implements TroubleshootingProvider
{
    private final SpanInterceptor spanInterceptor;

    @Inject
    public OpenTelemetryTraceProvider(SpanInterceptor spanInterceptor)
    {
        this.spanInterceptor = spanInterceptor;
    }

    @Override
    public void onContextStarted(TroubleshootingContext context)
    {
        spanInterceptor.requestSpanCollect(context.getQueryId());
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return ImmutableMap.of("opentelemetry-coordinator.grpc", spanInterceptor.removeSpans(context.getQueryId()));
    }
}
