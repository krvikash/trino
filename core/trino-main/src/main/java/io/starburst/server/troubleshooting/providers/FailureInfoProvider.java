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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;

import java.io.InputStream;
import java.util.Map;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;

public class FailureInfoProvider
        implements TroubleshootingProvider
{
    private static final Joiner JOINER = Joiner.on('\n');

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return context.get(QueryInfo.class)
                .map(QueryInfo::getFailureInfo)
                .map(failureInfo -> ImmutableMap.of(
                        "failure_info.txt", toInputStream(mapToString(failureInfo)),
                        "failure_stack_trace.txt", toInputStream(JOINER.join(failureInfo.getStack())))).orElseGet(ImmutableMap::of);
    }

    private String mapToString(ExecutionFailureInfo failureInfo)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Error code: ").append(failureInfo.getErrorCode()).append('\n');
        builder.append("Error message: ").append(failureInfo.getMessage()).append('\n');
        builder.append("Error location: ").append(failureInfo.getErrorLocation()).append('\n');
        builder.append("Remote host: ").append(failureInfo.getRemoteHost()).append('\n');
        if (failureInfo.getCause() != null) {
            builder.append("Cause: ").append(mapToString(failureInfo.getCause())).append('\n');
        }
        return builder.toString();
    }
}
