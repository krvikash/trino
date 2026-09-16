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
import com.google.common.collect.Sets;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.execution.QueryStats;
import io.trino.execution.TableInfo;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public class SameTableScan
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        Map<PlanNodeId, TableInfo> tables = details.tables();
        Map<TableInfo, List<PlanNode>> nodesByTable = details.planNodesById().values().stream()
                .filter(node -> tables.containsKey(node.getId()))
                .collect(groupingBy(node -> tables.get(node.getId())));

        ImmutableList.Builder<RuleFinding> findings = ImmutableList.builder();
        Set<PlanNodeId> reported = new HashSet<>();
        for (Map.Entry<TableInfo, List<PlanNode>> entry : nodesByTable.entrySet()) {
            List<PlanNode> nodes = entry.getValue();
            if (nodes.size() < 2) {
                continue;
            }
            String tableName = entry.getKey().tableName().toString();
            for (Set<PlanNode> overlapping : overlappingExpensiveGroups(nodes, details)) {
                for (PlanNode node : overlapping) {
                    if (reported.add(node.getId())) {
                        findings.add(toFinding(node, tableName, overlapping.size()));
                    }
                }
            }
        }
        return findings.build();
    }

    private static List<Set<PlanNode>> overlappingExpensiveGroups(List<PlanNode> nodes, QueryExecutionDetails details)
    {
        QueryExecutionDetails.Thresholds thresholds = details.thresholds();
        ImmutableList.Builder<Set<PlanNode>> groups = ImmutableList.builder();
        for (PlanNode node : nodes) {
            Set<PlanNode> overlapping = nodes.stream()
                    .filter(other -> overlapPercentage(node, other) > thresholds.minOverlappingColumns())
                    .collect(toCollection(HashSet::new));
            overlapping.removeIf(candidate -> !isExpensive(candidate, details));
            if (overlapping.size() > 1) {
                groups.add(overlapping);
            }
        }
        return groups.build();
    }

    private static double overlapPercentage(PlanNode source, PlanNode other)
    {
        Set<ColumnHandle> sourceColumns = new HashSet<>(((TableScanNode) source).getAssignments().values());
        Set<ColumnHandle> otherColumns = new HashSet<>(((TableScanNode) other).getAssignments().values());
        if (sourceColumns.isEmpty()) {
            return 0;
        }
        return (double) Sets.intersection(otherColumns, sourceColumns).size() / sourceColumns.size();
    }

    private static boolean isExpensive(PlanNode node, QueryExecutionDetails details)
    {
        QueryStats queryStats = details.queryStats();
        QueryExecutionDetails.Thresholds thresholds = details.thresholds();
        double cpuTimePercent = percentage(queryStats.getTotalCpuTime(), details.cpuTime(node.getId()));
        double scheduledTimePercent = percentage(queryStats.getTotalScheduledTime(), details.scheduledTime(node.getId()));
        double dataPercent = percentage(queryStats.getOutputDataSize(), details.outputDataSize(node.getId()));
        return cpuTimePercent > thresholds.minOverlappingScanPercentage()
                || scheduledTimePercent > thresholds.minOverlappingScanPercentage()
                || dataPercent > thresholds.minOverlappingScanPercentage();
    }

    private static double percentage(Duration total, Duration part)
    {
        double totalNanos = total.roundTo(NANOSECONDS);
        return totalNanos > 0 ? part.roundTo(NANOSECONDS) / totalNanos : 0;
    }

    private static double percentage(DataSize total, DataSize part)
    {
        double totalBytes = total.toBytes();
        return totalBytes > 0 ? part.toBytes() / totalBytes : 0;
    }

    private static RuleFinding toFinding(PlanNode node, String tableName, int occurrences)
    {
        return new RuleFinding.Node(
                node.getId().toString(),
                "Table scan on '%s' occurs %d times.".formatted(tableName, occurrences));
    }
}
