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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toCollection;

public class MissingStats
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(node -> (node instanceof TableScanNode) && hasMissingStats(node, details))
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static boolean hasMissingStats(PlanNode node, QueryExecutionDetails details)
    {
        return details.estimates().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown()).isOutputRowCountUnknown() || !columnsWithMissingStats(node, details).isEmpty();
    }

    private static Set<String> columnsWithMissingStats(PlanNode node, QueryExecutionDetails details)
    {
        TableScanNode tableScanNode = (TableScanNode) node;
        PlanNodeStatsEstimate estimate = details.estimates().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown());
        return tableScanNode.getOutputSymbols().stream()
                .filter(symbol -> estimate.getSymbolStatistics(symbol).isUnknown())
                .map(Symbol::name)
                .collect(toCollection(TreeSet::new));
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        String tableName = details.tableName(node.getId()).orElse("UNKNOWN");
        Set<String> columnsWithMissingStats = columnsWithMissingStats(node, details);

        StringBuilder message = new StringBuilder("Table scan on table '%s' is missing stats:".formatted(tableName));
        if (details.estimates().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown()).isOutputRowCountUnknown()) {
            message.append(" row count is unknown;");
        }
        if (!columnsWithMissingStats.isEmpty()) {
            message.append(" columns without statistics: ")
                    .append(String.join(", ", columnsWithMissingStats))
                    .append(";");
        }
        return new RuleFinding.Node(node.getId().toString(), message.toString());
    }
}
