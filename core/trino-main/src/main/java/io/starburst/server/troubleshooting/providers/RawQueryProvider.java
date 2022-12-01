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
import io.trino.spi.eventlistener.QueryCompletedEvent;

import java.io.InputStream;
import java.util.Map;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;

public class RawQueryProvider
        implements TroubleshootingProvider
{
    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        if (context.has(QueryCompletedEvent.class)) {
            return ImmutableMap.of("query.sql", toInputStream(context.getOrThrow(QueryCompletedEvent.class).getMetadata().getQuery()));
        }

        return ImmutableMap.of();
    }
}
