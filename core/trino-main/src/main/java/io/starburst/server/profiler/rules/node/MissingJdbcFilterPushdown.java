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
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.profiler.rules.ProfilerRulesUtil.isJdbc;

public class MissingJdbcFilterPushdown
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(MissingJdbcFilterPushdown::isMissingPredicatePushdown)
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static boolean isMissingPredicatePushdown(PlanNode node)
    {
        if (!(node instanceof FilterNode filterNode)) {
            return false;
        }
        if (!(filterNode.getSource() instanceof TableScanNode tableScanNode) || !isJdbc(tableScanNode)) {
            return false;
        }
        return containsStaticFilter(filterNode.getPredicate());
    }

    private static boolean containsStaticFilter(Expression predicate)
    {
        if (predicate instanceof Call call) {
            return call.function().deterministic() && !DynamicFilters.isDynamicFilter(call);
        }
        if (predicate instanceof Logical logical) {
            return logical.terms().stream().anyMatch(MissingJdbcFilterPushdown::containsStaticFilter);
        }
        return true;
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        FilterNode filterNode = (FilterNode) node;
        TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
        String tableName = details.tableName(tableScanNode.getId()).orElse("UNKNOWN");
        return new RuleFinding.Node(node.getId().toString(), "Missing predicate pushdown on table %s: [%s]".formatted(tableName, filterNode.getPredicate()));
    }
}
