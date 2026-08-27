/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import io.airlift.log.Logger;
import io.starburst.server.castleinsights.model.CastleInsightsResult;

/**
 * Default sink, used when the surrounding product has not supplied one. Cork extracts insights but
 * has nowhere to ship them; the transport belongs to the integration.
 */
public class DiscardingCastleInsightsResultSink
        implements CastleInsightsResultSink
{
    private static final Logger log = Logger.get(DiscardingCastleInsightsResultSink.class);

    @Override
    public void accept(CastleInsightsResult result)
    {
        log.debug("Discarding %s insights for query %s; no result sink is bound", result.insights().size(), result.queryId());
    }
}
