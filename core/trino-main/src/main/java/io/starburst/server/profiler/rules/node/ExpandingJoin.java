/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules.node;

import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.profiler.Severity.CRITICAL;

public class ExpandingJoin
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(node -> node instanceof JoinNode && isCrossJoin(node, details))
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static boolean isCrossJoin(PlanNode node, QueryExecutionDetails details)
    {
        return outputRatio(node, details) > details.thresholds().crossJoinScalingFactor();
    }

    private static double outputRatio(PlanNode node, QueryExecutionDetails details)
    {
        List<PlanNode> sources = details.children(node);
        double leftEstimatedRows = details.estimates().getOrDefault(sources.get(0).getId(), PlanNodeStatsEstimate.unknown()).getOutputRowCount();
        double rightEstimatedRows = details.estimates().getOrDefault(sources.get(1).getId(), PlanNodeStatsEstimate.unknown()).getOutputRowCount();
        double joinOutputEstimatedRows = details.estimates().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown()).getOutputRowCount();
        double outputRatio = joinOutputEstimatedRows / Math.max(leftEstimatedRows, rightEstimatedRows);

        if (Double.isNaN(outputRatio)) {
            long leftPositions = details.outputPositions(sources.get(0).getId());
            long rightPositions = details.outputPositions(sources.get(1).getId());
            long outputPositions = details.outputPositions(node.getId());
            return (double) outputPositions / Math.max(leftPositions, rightPositions);
        }
        return outputRatio;
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        List<PlanNode> sources = details.children(node);
        long leftPositions = details.outputPositions(sources.get(0).getId());
        long rightPositions = details.outputPositions(sources.get(1).getId());
        long outputPositions = details.outputPositions(node.getId());
        double outputRatio = outputRatio(node, details);
        return new RuleFinding.Node(
                node.getId().toString(),
                "Expanding Join of left table with %s positions and right table with %s positions ".formatted(leftPositions, rightPositions) +
                        "produced output of %s which multiplying factor is %s".formatted(outputPositions, outputRatio),
                CRITICAL,
                Optional.empty());
    }
}
