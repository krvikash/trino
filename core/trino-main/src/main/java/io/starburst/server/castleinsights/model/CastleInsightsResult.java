/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.model;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Everything extracted from a single completed query. Query metadata such as the user and the
 * end time is deliberately absent; consumers take it from the query history entry for the same
 * {@code queryId}.
 */
public record CastleInsightsResult(
        String queryId,
        String trinoVersion,
        List<CastleInsight> insights)
{
    public CastleInsightsResult
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(trinoVersion, "trinoVersion is null");
        insights = ImmutableList.copyOf(insights);
    }
}
