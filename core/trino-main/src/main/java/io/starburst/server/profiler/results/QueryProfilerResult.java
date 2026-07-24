/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.results;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record QueryProfilerResult(
        String queryId,
        String profilerVersion,
        String trinoVersion,
        List<PlanNodeFinding> planNodeFindings,
        List<StageFinding> stageFindings,
        List<QueryFinding> queryFindings,
        QueryStatsSummary queryStatsSummary,
        QuerySummary querySummary)
{
    public QueryProfilerResult
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(profilerVersion, "profilerVersion is null");
        requireNonNull(trinoVersion, "trinoVersion is null");
        requireNonNull(planNodeFindings, "planNodeFindings is null");
        requireNonNull(stageFindings, "stageFindings is null");
        requireNonNull(queryFindings, "queryFindings is null");
        requireNonNull(queryStatsSummary, "queryStatsSummary is null");
        requireNonNull(querySummary, "querySummary is null");
    }
}
