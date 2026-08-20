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
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.profiler.rules.ProfilerRulesUtil.isJdbc;
import static java.util.stream.Collectors.joining;

public class MissingJdbcProjectionPushdown
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(MissingJdbcProjectionPushdown::isMissingProjectionPushdown)
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static boolean isMissingProjectionPushdown(PlanNode node)
    {
        if (!(node instanceof ProjectNode projectNode)) {
            return false;
        }
        if (!(projectNode.getSource() instanceof TableScanNode tableScanNode) || !isJdbc(tableScanNode)) {
            return false;
        }
        return hasNonIdentityAssignments(projectNode.getAssignments());
    }

    private static boolean hasNonIdentityAssignments(Assignments assignments)
    {
        return assignments.entrySet().stream().anyMatch(entry -> !assignments.isIdentity(entry.getKey()));
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        ProjectNode projectNode = (ProjectNode) node;
        TableScanNode tableScanNode = (TableScanNode) projectNode.getSource();
        String tableName = details.tableName(tableScanNode.getId()).orElse("UNKNOWN");

        Assignments assignments = projectNode.getAssignments();
        String nonIdentityExpressions = assignments.entrySet().stream()
                .filter(entry -> !assignments.isIdentity(entry.getKey()))
                .map(entry -> "%s := %s".formatted(entry.getKey().name(), entry.getValue()))
                .collect(joining(", "));

        return new RuleFinding.Node(node.getId().toString(), "Missing projection pushdown on table %s: [%s]".formatted(tableName, nonIdentityExpressions));
    }
}
