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
import io.trino.SessionRepresentation;
import io.trino.execution.QueryInfo;

import java.io.InputStream;
import java.util.Map;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;

public class SessionInfoProvider
        implements TroubleshootingProvider
{
    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return context.get(QueryInfo.class)
                .map(info -> ImmutableMap.of("session.txt", toInputStream(mapToString(info.getSession()))))
                .orElse(ImmutableMap.of());
    }

    private String mapToString(String prefix, Map<String, String> values)
    {
        StringBuilder builder = new StringBuilder();
        values.forEach((sessionKey, sessionValue) ->
                builder.append(prefix).append(sessionKey).append(" = ").append(sessionValue).append('\n'));
        return builder.toString();
    }

    private String mapToString(SessionRepresentation session)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(mapToString("", session.getSystemProperties()));
        session.getCatalogProperties().forEach((catalogName, sessionProperties) ->
                builder.append(mapToString(catalogName + ".", sessionProperties)));
        return builder.toString();
    }
}
