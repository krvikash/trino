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

import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.ProfilerRulesUtil;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SlowJdbcTableScan
        implements QueryProfilerRule
{
    private static final Set<String> TABLE_SCAN_OPERATORS = ImmutableSet.of("TableScanOperator", "ScanFilterAndProjectOperator");

    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        QueryStats queryStats = details.queryStats();
        return queryStats.getOperatorSummaries().stream()
                .filter(operator -> isSlowJdbcTableScan(queryStats, operator, details))
                .map(operator -> toFinding(operator, details))
                .collect(toImmutableList());
    }

    private static boolean isSlowJdbcTableScan(QueryStats queryStats, OperatorStats operator, QueryExecutionDetails details)
    {
        if (!TABLE_SCAN_OPERATORS.contains(operator.getOperatorType())) {
            return false;
        }
        double scheduledTimeRatio = (double) new Duration(
                operator.getAddInputWall().roundTo(NANOSECONDS) + operator.getGetOutputWall().roundTo(NANOSECONDS) + operator.getFinishWall().roundTo(NANOSECONDS),
                NANOSECONDS).toMillis() / queryStats.getTotalScheduledTime().toMillis();
        if (scheduledTimeRatio < details.thresholds().slowJdbcTableMinScheduledTimeRatio()) {
            return false;
        }
        return Optional.ofNullable(details.planNodesById().get(operator.getPlanNodeId()))
                .flatMap(SlowJdbcTableScan::findTableScanNode)
                .map(ProfilerRulesUtil::isJdbc)
                .orElse(false);
    }

    private static Optional<TableScanNode> findTableScanNode(PlanNode planNode)
    {
        if (planNode instanceof TableScanNode tableScanNode) {
            return Optional.of(tableScanNode);
        }
        if (planNode instanceof FilterNode filterNode) {
            return findTableScanNode(filterNode.getSource());
        }
        if (planNode instanceof ProjectNode projectNode) {
            return findTableScanNode(projectNode.getSource());
        }
        return Optional.empty();
    }

    private static RuleFinding toFinding(OperatorStats operator, QueryExecutionDetails details)
    {
        PlanNode planNode = Optional.ofNullable(details.planNodesById().get(operator.getPlanNodeId())).orElseThrow();
        TableScanNode tableScanNode = findTableScanNode(planNode).orElseThrow();
        String tableName = details.tableName(tableScanNode.getId()).orElse("UNKNOWN");
        String message = "Slow JDBC table scan on %s is using %d driver%s.".formatted(
                tableName, operator.getTotalDrivers(), operator.getTotalDrivers() == 1 ? "" : "s");
        return new RuleFinding.Node(operator.getPlanNodeId().toString(), message);
    }
}
