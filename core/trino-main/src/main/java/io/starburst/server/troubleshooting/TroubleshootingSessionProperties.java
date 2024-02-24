/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class TroubleshootingSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String TROUBLESHOOTING_JFR_MAX_COLLECTED_WORKERS = "troubleshooting_jfr_max_collected_workers";
    public static final String TROUBLESHOOTING_TRACE_MAX_COLLECTED_WORKERS = "troubleshooting_trace_max_collected_workers";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TroubleshootingSessionProperties(TroubleshootingConfig config)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        TROUBLESHOOTING_JFR_MAX_COLLECTED_WORKERS,
                        "Limits the number of workers nodes from which jfr profile is collected during troubleshooting",
                        config.getMaxCollectedWorkersJfr(),
                        false),
                integerProperty(
                        TROUBLESHOOTING_TRACE_MAX_COLLECTED_WORKERS,
                        "Limits the number of workers nodes from which opentelemetry trace is collected during troubleshooting",
                        config.getMaxCollectedWorkersTrace(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getMaxCollectedWorkersJfr(Session session)
    {
        return session.getSystemProperty(TROUBLESHOOTING_JFR_MAX_COLLECTED_WORKERS, Integer.class);
    }

    public static int getMaxCollectedWorkersTrace(Session session)
    {
        return session.getSystemProperty(TROUBLESHOOTING_TRACE_MAX_COLLECTED_WORKERS, Integer.class);
    }
}
