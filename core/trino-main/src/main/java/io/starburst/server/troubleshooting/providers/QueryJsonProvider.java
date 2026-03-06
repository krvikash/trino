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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.trino.execution.QueryInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;
import static io.trino.dispatcher.DispatchManager.stopTheLeak;
import static java.util.Objects.requireNonNull;

public class QueryJsonProvider
        implements TroubleshootingProvider
{
    private final ObjectMapper objectMapper;

    @Inject
    public QueryJsonProvider(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return context.get(QueryInfo.class).map(queryInfo -> {
            try {
                String queryJson = objectMapper.writeValueAsString(queryInfo.pruneCatalogProperties());
                return ImmutableMap.of("query.json", toInputStream(stopTheLeak(queryJson)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).orElse(ImmutableMap.of());
    }
}
