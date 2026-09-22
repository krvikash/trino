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
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.profiler.Severity.CRITICAL;

public class WrongJoinDistributionType
        implements QueryProfilerRule
{
    private static final double ERROR_MARGIN = 1.5;

    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(node -> isWrongJoinDistributionType(node, details))
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static boolean isWrongJoinDistributionType(PlanNode node, QueryExecutionDetails details)
    {
        if (!(node instanceof JoinNode joinNode && joinNode.getDistributionType().isPresent())) {
            return false;
        }
        QueryExecutionDetails.Thresholds thresholds = details.thresholds();
        DistributionType distributionType = joinNode.getDistributionType().orElseThrow();
        DataSize rightSize = details.outputDataSize(details.children(node).get(1).getId());
        return (distributionType == DistributionType.REPLICATED && rightSize.toBytes() > thresholds.joinMaxBroadcastTableSize().toBytes() * ERROR_MARGIN) ||
                (distributionType == DistributionType.PARTITIONED && rightSize.toBytes() < thresholds.joinMaxBroadcastTableSize().toBytes() / ERROR_MARGIN);
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        QueryExecutionDetails.Thresholds thresholds = details.thresholds();
        JoinNode joinNode = (JoinNode) node;
        DataSize rightSize = details.outputDataSize(details.children(node).get(1).getId());
        if (joinNode.getDistributionType().orElseThrow() == DistributionType.REPLICATED) {
            String description = "The build side (%s) is bigger than max broadcast size (%s).".formatted(
                    rightSize, thresholds.joinMaxBroadcastTableSize());
            return new RuleFinding.Node(node.getId().toString(), description, CRITICAL, Optional.of("The join should be PARTITIONED."));
        }
        String description = "The build side (%s) is smaller than max broadcast size (%s).".formatted(
                rightSize, thresholds.joinMaxBroadcastTableSize());
        return new RuleFinding.Node(node.getId().toString(), description, CRITICAL, Optional.of("The join should be REPLICATED."));
    }
}
