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
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.profiler.Severity.CRITICAL;

public class FullTableScan
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(node -> node instanceof TableScanNode tableScanNode && tableScanNode.getEnforcedConstraint().isAll())
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        String tableName = details.tableName(node.getId()).orElse("UNKNOWN");
        return new RuleFinding.Node(
                node.getId().toString(),
                "Table scan on the %s table is a Full Table Scan.".formatted(tableName),
                CRITICAL,
                Optional.of("Try partitioning the table or applying filters."));
    }
}
