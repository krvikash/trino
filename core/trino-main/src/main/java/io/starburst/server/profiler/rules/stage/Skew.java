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

import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.execution.StageInfo;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class Skew
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.stageInfos().stream()
                .filter(Skew::isSkewed)
                .map(Skew::toFinding)
                .collect(toImmutableList());
    }

    private static boolean isSkewed(StageInfo stageInfo)
    {
        TDigest taskInputDistribution = taskInputDistribution(stageInfo);
        return taskInputDistribution.getMax() > taskInputDistribution.valueAt(0.5) * 5;
    }

    private static TDigest taskInputDistribution(StageInfo stageInfo)
    {
        TDigest taskInputDistribution = new TDigest();
        stageInfo.tasks().forEach(task -> taskInputDistribution.add(task.stats().processedInputDataSize().toBytes()));
        return taskInputDistribution;
    }

    private static RuleFinding toFinding(StageInfo stageInfo)
    {
        TDigest taskInputDistribution = taskInputDistribution(stageInfo);
        return new RuleFinding.Stage(
                stageInfo.stageId().toString(),
                "Stage input between workers is skewed. Max input size (%s) is significantly larger than median input size (%s).".formatted(
                        DataSize.ofBytes((long) taskInputDistribution.getMax()),
                        DataSize.ofBytes((long) taskInputDistribution.valueAt(0.5))));
    }
}
