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
import io.starburst.server.troubleshooting.tracing.RemoteTroubleshootingTraceClient.DownloadResult;
import io.trino.spi.Node;

import java.io.InputStream;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class OpenTelemetryTraceProvider
        implements TroubleshootingProvider
{
    private final SpanInterceptor spanInterceptor;
    private final RemoteTroubleshootingTraceClient remoteTroubleshootingTraceClient;

    @Inject
    public OpenTelemetryTraceProvider(SpanInterceptor spanInterceptor, RemoteTroubleshootingTraceClient remoteTroubleshootingTraceClient)
    {
        this.spanInterceptor = spanInterceptor;
        this.remoteTroubleshootingTraceClient = requireNonNull(remoteTroubleshootingTraceClient, "remoteTroubleshootingTraceClient is null");
    }

    @Override
    public void onContextStarted(TroubleshootingContext context)
    {
        spanInterceptor.requestSpanCollect(context.getQueryId());
        remoteTroubleshootingTraceClient.start(context.getQueryId());
    }

    @Override
    public void onContextFinished(TroubleshootingContext context)
    {
        remoteTroubleshootingTraceClient.retain(context.getQueryId(), context.getTraceCollectedNodes());
    }

    @Override
    public void onContextRemoved(TroubleshootingContext context)
    {
        spanInterceptor.remove(context.getQueryId());
        remoteTroubleshootingTraceClient.remove(context.getQueryId());
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        Map<Node, DownloadResult> remoteInputStreams = remoteTroubleshootingTraceClient.download(context.getQueryId(), context.getTraceCollectedNodes());
        Map<String, InputStream> remoteInputStreamsByNodeId = remoteInputStreams.entrySet()
                .stream()
                .collect(toImmutableMap(
                        (Map.Entry<Node, DownloadResult> entry) -> entry.getValue().isSuccessful() ?
                                "traces/opentelemetry-worker-" + entry.getKey().getNodeIdentifier() + ".grpc.gz" :
                                "traces/opentelemetry-worker-" + entry.getKey().getNodeIdentifier() + ".error.txt",
                        entry -> entry.getValue().inputStream()));
        return ImmutableMap.<String, InputStream>builder()
                .put("traces/opentelemetry-coordinator.grpc.gz", spanInterceptor.removeSpans(context.getQueryId()))
                .putAll(remoteInputStreamsByNodeId)
                .buildOrThrow();
    }
}
