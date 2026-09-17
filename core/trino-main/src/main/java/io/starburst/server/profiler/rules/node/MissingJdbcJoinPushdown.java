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

import com.google.common.collect.ImmutableList;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.ProfilerRulesUtil;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.connector.CatalogHandle;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class MissingJdbcJoinPushdown
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        return details.planNodesById().values().stream()
                .filter(node -> isMissingJdbcJoinPushdown(node, details))
                .map(node -> toFinding(node, details))
                .collect(toImmutableList());
    }

    private static boolean isMissingJdbcJoinPushdown(PlanNode node, QueryExecutionDetails details)
    {
        if (!(node instanceof JoinNode)) {
            return false;
        }
        List<PlanNode> sources = details.children(node);
        Set<CatalogHandle> leftCatalogs = jdbcCatalogs(sources.get(0), details);
        Set<CatalogHandle> rightCatalogs = jdbcCatalogs(sources.get(1), details);
        return !leftCatalogs.isEmpty() && leftCatalogs.equals(rightCatalogs);
    }

    private static Set<CatalogHandle> jdbcCatalogs(PlanNode node, QueryExecutionDetails details)
    {
        return collectTableScans(node, details).stream()
                .filter(ProfilerRulesUtil::isJdbc)
                .map(tableScanNode -> tableScanNode.getTable().catalogHandle())
                .collect(toImmutableSet());
    }

    private static List<TableScanNode> collectTableScans(PlanNode node, QueryExecutionDetails details)
    {
        if (node instanceof TableScanNode tableScanNode) {
            return ImmutableList.of(tableScanNode);
        }
        return details.children(node).stream()
                .flatMap(source -> collectTableScans(source, details).stream())
                .collect(toImmutableList());
    }

    private static RuleFinding toFinding(PlanNode node, QueryExecutionDetails details)
    {
        List<String> catalogs = jdbcCatalogs(details.children(node).getFirst(), details).stream()
                .map(catalogHandle -> catalogHandle.getCatalogName().toString())
                .sorted()
                .collect(toImmutableList());
        return new RuleFinding.Node(
                node.getId().toString(),
                "Join is not pushed down to JDBC catalogs %s.".formatted(catalogs));
    }
}
