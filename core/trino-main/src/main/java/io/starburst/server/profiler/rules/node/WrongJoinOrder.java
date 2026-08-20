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

import io.airlift.units.DataSize;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.AUTOMATIC;

public class WrongJoinOrder
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .flatMap(node -> detectWrongJoinOrder(node, details)
                        .map(explanation -> toFinding(node, explanation))
                        .stream())
                .collect(toImmutableList());
    }

    private static Optional<String> detectWrongJoinOrder(PlanNode node, QueryExecutionDetails details)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }
        List<PlanNode> sources = details.children(node);
        PlanNode left = sources.get(0);
        PlanNode right = sources.get(1);
        DataSize leftSize = details.outputDataSize(left.getId());
        DataSize rightSize = details.outputDataSize(right.getId());
        if (leftSize.toBytes() >= rightSize.toBytes() * 0.7) {
            return Optional.empty();
        }

        String base = "Build side (%s) is bigger than the probe side (%s).".formatted(rightSize, leftSize);
        String reason = detectReason(left, right, details);
        if (!reason.isEmpty()) {
            return Optional.of(base + " " + reason);
        }
        return Optional.of(base);
    }

    private static String detectReason(PlanNode left, PlanNode right, QueryExecutionDetails details)
    {
        Map<String, String> systemProperties = details.systemProperties();
        String joinReorderingStrategy = systemProperties.getOrDefault(JOIN_REORDERING_STRATEGY, AUTOMATIC.name());
        if (!AUTOMATIC.name().equalsIgnoreCase(joinReorderingStrategy)) {
            return "Possible cause: %s is set to '%s' instead of '%s'.".formatted(JOIN_REORDERING_STRATEGY, joinReorderingStrategy, AUTOMATIC.name());
        }

        PlanNodeStatsEstimate leftEstimate = details.estimates().getOrDefault(left.getId(), PlanNodeStatsEstimate.unknown());
        PlanNodeStatsEstimate rightEstimate = details.estimates().getOrDefault(right.getId(), PlanNodeStatsEstimate.unknown());

        List<String> missingStatsTables = new ArrayList<>();
        collectTableScansWithMissingStats(left, details, missingStatsTables);
        collectTableScansWithMissingStats(right, details, missingStatsTables);

        if (leftEstimate.isOutputRowCountUnknown() || rightEstimate.isOutputRowCountUnknown() || !missingStatsTables.isEmpty()) {
            if (!missingStatsTables.isEmpty()) {
                return "Possible cause: missing statistics on table(s): %s.".formatted(String.join(", ", missingStatsTables));
            }
            return "Possible cause: missing statistics (row count estimates are unknown).";
        }

        double leftEstimatedRows = leftEstimate.getOutputRowCount();
        double rightEstimatedRows = rightEstimate.getOutputRowCount();
        long leftActualRows = details.outputPositions(left.getId());
        long rightActualRows = details.outputPositions(right.getId());

        boolean leftEstimateOff = isEstimateSignificantlyOff(leftEstimatedRows, leftActualRows);
        boolean rightEstimateOff = isEstimateSignificantlyOff(rightEstimatedRows, rightActualRows);

        if (leftEstimateOff || rightEstimateOff) {
            return "Possible cause: bad row count estimates (left side estimated %.0f rows vs actual %d, right side estimated %.0f rows vs actual %d).".formatted(
                    leftEstimatedRows, leftActualRows, rightEstimatedRows, rightActualRows);
        }

        return "";
    }

    private static boolean isEstimateSignificantlyOff(double estimated, long actual)
    {
        if (actual == 0) {
            return estimated > 1000;
        }
        double ratio = estimated / actual;
        return ratio > 10 || ratio < 0.1;
    }

    private static void collectTableScansWithMissingStats(PlanNode node, QueryExecutionDetails details, List<String> result)
    {
        if (node instanceof TableScanNode tableScanNode) {
            PlanNodeStatsEstimate estimate = details.estimates().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown());
            boolean missingStats = estimate.isOutputRowCountUnknown() ||
                    tableScanNode.getOutputSymbols().stream().anyMatch(symbol -> estimate.getSymbolStatistics(symbol).isUnknown());
            if (missingStats) {
                result.add(details.tableName(node.getId()).orElse("UNKNOWN"));
            }
        }
        for (PlanNode source : details.children(node)) {
            collectTableScansWithMissingStats(source, details, result);
        }
    }

    private static RuleFinding toFinding(PlanNode node, String explanation)
    {
        return new RuleFinding.Node(node.getId().toString(), explanation);
    }
}
