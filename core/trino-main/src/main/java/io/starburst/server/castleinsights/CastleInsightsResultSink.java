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

import io.starburst.server.castleinsights.model.CastleInsightsResult;

/**
 * Where a query's insights go once they have been extracted.
 */
public interface CastleInsightsResultSink
{
    void accept(CastleInsightsResult result);
}
