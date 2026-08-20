/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules.stage;

import io.airlift.stats.Distribution.DistributionSnapshot;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.execution.StageInfo;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SlowSplitDistribution
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        long queryExecutionTime = details.queryStats().getExecutionTime().roundTo(NANOSECONDS);
        return details.stageInfos().stream()
                .filter(stageInfo -> isSlowSplitDistribution(stageInfo, queryExecutionTime, details.thresholds()))
                .map(stageInfo -> toFinding(stageInfo, queryExecutionTime))
                .collect(toImmutableList());
    }

    private static boolean isSlowSplitDistribution(StageInfo stageInfo, long queryExecutionTime, QueryExecutionDetails.Thresholds thresholds)
    {
        if (queryExecutionTime <= 0) {
            return false;
        }
        return (double) maxSplitGenerationTime(stageInfo) / queryExecutionTime > thresholds.slowSplitDistributionPercentage();
    }

    private static long maxSplitGenerationTime(StageInfo stageInfo)
    {
        Map<PlanNodeId, DistributionSnapshot> splitDistribution = stageInfo.stageStats().getGetSplitDistribution();
        return splitDistribution.values().stream().mapToLong(snapshot -> (long) snapshot.total()).max().orElse(0L);
    }

    private static RuleFinding toFinding(StageInfo stageInfo, long queryExecutionTime)
    {
        long maxSplitGenerationTime = maxSplitGenerationTime(stageInfo);
        return new RuleFinding.Stage(
                stageInfo.stageId().toString(),
                "Stage split generation took at least %s ns (%.1f%% of query execution time).".formatted(
                        maxSplitGenerationTime,
                        100.0 * maxSplitGenerationTime / queryExecutionTime));
    }
}
