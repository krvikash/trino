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
import io.trino.execution.TableInfo;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.DistributionSnapshot;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class SmallFiles
        implements QueryProfilerRule
{
    private static final String INPUT_ROWS_DISTRIBUTION_METRIC = "Input rows distribution";

    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.queryStats().getOperatorSummaries().stream()
                .filter(operator -> isSmallFilesTableScan(operator, details))
                .map(operator -> toFinding(operator, details))
                .collect(toImmutableList());
    }

    private static boolean isSmallFilesTableScan(OperatorStats operator, QueryExecutionDetails details)
    {
        if (!details.tables().containsKey(operator.getPlanNodeId())) {
            return false;
        }
        DistributionSnapshot inputRowsDistribution = inputRowsDistribution(operator);
        if (inputRowsDistribution == null) {
            return false;
        }
        return operator.getTotalDrivers() >= details.thresholds().smallFilesMinNumberOfSplits()
                && inputRowsDistribution.p75() < details.thresholds().smallFilesPositionsPerSplitThreshold();
    }

    private static DistributionSnapshot inputRowsDistribution(OperatorStats operator)
    {
        Object metric = operator.getMetrics().getMetrics().get(INPUT_ROWS_DISTRIBUTION_METRIC);
        return metric instanceof DistributionSnapshot snapshot ? snapshot : null;
    }

    private static RuleFinding toFinding(OperatorStats operator, QueryExecutionDetails details)
    {
        PlanNodeId planNodeId = operator.getPlanNodeId();
        TableInfo tableInfo = details.tables().get(planNodeId);
        DistributionSnapshot inputRowsDistribution = inputRowsDistribution(operator);
        return new RuleFinding.Node(
                planNodeId.toString(),
                "Table scan %s on table '%s' produced many (%s) small (p75 row count: %s) splits.".formatted(
                        planNodeId, tableInfo.tableName(), operator.getTotalDrivers(), inputRowsDistribution.p75()));
    }
}
