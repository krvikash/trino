/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.providers;

import com.google.common.collect.ImmutableMap;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;

public class FailureInfoProvider
        implements TroubleshootingProvider
{
    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        Optional<ExecutionFailureInfo> executionFailureInfo = context.get(QueryInfo.class).map(QueryInfo::getFailureInfo);
        if (executionFailureInfo.isPresent()) {
            return ImmutableMap.of("failure_info.txt", toInputStream(mapToString(executionFailureInfo.get())));
        }

        if (context.has(QueryCompletedEvent.class)) {
            QueryCompletedEvent event = context.getOrThrow(QueryCompletedEvent.class);
            if (event.getFailureInfo().isPresent()) {
                return ImmutableMap.of("failure_info.txt", toInputStream(mapToString(event.getFailureInfo().get())));
            }
        }

        return ImmutableMap.of();
    }

    private String mapToString(ExecutionFailureInfo failureInfo)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Error code: ").append(failureInfo.getErrorCode()).append('\n');
        builder.append("Error message: ").append(failureInfo.getMessage()).append('\n');
        builder.append("Error location: ").append(failureInfo.getErrorLocation()).append('\n');
        builder.append("Remote host: ").append(failureInfo.getRemoteHost()).append('\n');
        builder.append("Stack trace: ").append(failureInfo.getStack()).append('\n');
        return builder.toString();
    }

    private String mapToString(QueryFailureInfo info)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Error code: ").append(info.getErrorCode()).append('\n');
        info.getFailureMessage().ifPresent(value -> builder.append("Failure message: ").append(value).append('\n'));
        info.getFailureType().ifPresent(value -> builder.append("Failure type: ").append(value).append('\n'));
        info.getFailureHost().ifPresent(value -> builder.append("Host: ").append(value).append('\n'));
        info.getFailureTask().ifPresent(value -> builder.append("Failed task: ").append(value).append('\n'));
        return builder.toString();
    }
}
