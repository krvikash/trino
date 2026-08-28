/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.execution.TableInfo;
import io.trino.operator.OperatorStats;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.planprinter.PlanNodeStats;
import io.trino.sql.planner.planprinter.PlanNodeStatsSummarizer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * A completed query in the shape the insight analyzers want: one flat view of the distributed plan,
 * with each node's table, column names, and row counts reachable by {@link PlanNodeId}.
 * <p>
 * Castle-owned on purpose. It overlaps with the profiler's {@code QueryExecutionDetails} today, but
 * the two answer to different consumers and are free to drift apart.
 */
public final class CastleQueryAnalysis
{
    private final String queryId;
    private final Optional<PlanNode> rootNode;
    private final Map<PlanFragmentId, PlanNode> rootByFragmentId;
    private final Map<PlanNodeId, PlanNode> planNodesById;
    private final Map<PlanNodeId, PlanNodeStats> stats;
    private final Map<PlanNodeId, PlanNodeStatsEstimate> statsEstimates;
    private final Map<PlanNodeId, TableRef> tables;
    private final Map<PlanNodeId, Map<String, String>> columnNamesBySymbolName;
    private final ListMultimap<PlanNodeId, OperatorStats> operatorStats;

    public CastleQueryAnalysis(QueryInfo queryInfo)
    {
        requireNonNull(queryInfo, "queryInfo is null");

        this.queryId = queryInfo.getQueryId().toString();
        this.columnNamesBySymbolName = computeColumnNames(queryInfo.getInputs());
        this.operatorStats = queryInfo.getQueryStats().getOperatorSummaries().stream()
                .collect(toImmutableListMultimap(OperatorStats::getPlanNodeId, operator -> operator));

        Optional<StagesInfo> stages = queryInfo.getStages();
        if (stages.isEmpty()) {
            this.rootNode = Optional.empty();
            this.rootByFragmentId = ImmutableMap.of();
            this.planNodesById = ImmutableMap.of();
            this.stats = ImmutableMap.of();
            this.statsEstimates = ImmutableMap.of();
            this.tables = ImmutableMap.of();
            return;
        }

        List<StageInfo> stageInfos = stages.get().getStages();
        Map<PlanFragmentId, PlanNode> rootByFragmentId = stageInfos.stream()
                .filter(stage -> stage.plan() != null)
                .collect(toImmutableMap(stage -> stage.plan().getId(), stage -> stage.plan().getRoot()));
        PlanNodes planNodes = computePlanNodes(stages.get(), rootByFragmentId);
        PlanNode root = requireNonNull(stages.get().getOutputStage().plan(), "planFragment is null").getRoot();

        this.rootByFragmentId = rootByFragmentId;
        this.planNodesById = planNodes.nodesById();
        this.stats = planNodes.statsByNode();
        this.rootNode = Optional.ofNullable(planNodes.nodesById().get(root.getId()));
        this.statsEstimates = computeStatsEstimates(stageInfos);
        this.tables = computeTables(stageInfos);
    }

    public String queryId()
    {
        return queryId;
    }

    public Optional<PlanNode> rootNode()
    {
        return rootNode;
    }

    public Collection<PlanNode> planNodes()
    {
        return planNodesById.values();
    }

    /**
     * Every plan node matching the predicate.
     */
    public List<PlanNode> planNodes(Predicate<PlanNode> predicate)
    {
        return planNodesById.values().stream()
                .filter(predicate)
                .collect(toImmutableList());
    }

    /**
     * Every plan node of the given type.
     */
    public <T extends PlanNode> List<T> planNodes(Class<T> type)
    {
        return planNodes(type::isInstance).stream()
                .map(type::cast)
                .collect(toImmutableList());
    }

    /**
     * Children of a node, stepping across fragment boundaries so the distributed plan reads as one tree.
     */
    public List<PlanNode> children(PlanNode node)
    {
        return childrenOf(node, rootByFragmentId);
    }

    /**
     * The table a scan reads, absent for any other node.
     */
    public Optional<TableRef> table(PlanNodeId planNodeId)
    {
        return Optional.ofNullable(tables.get(planNodeId));
    }

    /**
     * The column behind a symbol produced by a scan. Resolved at plan time and carried on
     * {@code QueryInfo}, because the query transaction is closed by the time analysis runs.
     */
    public Optional<String> columnName(PlanNodeId planNodeId, String symbolName)
    {
        return Optional.ofNullable(columnNamesBySymbolName.get(planNodeId))
                .map(columns -> columns.get(symbolName));
    }

    /**
     * The merged operator stats a plan node produced, one entry per operator the node compiled into.
     * Empty for a node that produced none, which also covers a node the engine never executed.
     */
    public List<OperatorStats> operatorStats(PlanNodeId planNodeId)
    {
        return operatorStats.get(planNodeId);
    }

    /**
     * Rows the node actually emitted. Zero when the node produced no stats, which also covers a node
     * the engine never executed.
     */
    public long outputRowCount(PlanNodeId planNodeId)
    {
        return Optional.ofNullable(stats.get(planNodeId))
                .map(PlanNodeStats::getPlanNodeOutputPositions)
                .orElse(0L);
    }

    /**
     * Rows the optimizer expected the node to emit, empty when it could not estimate.
     */
    public OptionalDouble estimatedRowCount(PlanNodeId planNodeId)
    {
        PlanNodeStatsEstimate estimate = statsEstimates.get(planNodeId);
        if (estimate == null) {
            return OptionalDouble.empty();
        }
        return known(estimate.getOutputRowCount());
    }

    /**
     * Bytes the optimizer expected the node to emit, empty when it could not estimate.
     */
    public OptionalDouble estimatedSizeInBytes(PlanNodeId planNodeId)
    {
        PlanNodeStatsEstimate estimate = statsEstimates.get(planNodeId);
        PlanNode planNode = planNodesById.get(planNodeId);
        if (estimate == null || planNode == null) {
            return OptionalDouble.empty();
        }
        return known(estimate.getOutputSizeInBytes(planNode.getOutputSymbols()));
    }

    private static OptionalDouble known(double value)
    {
        if (Double.isNaN(value)) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(value);
    }

    private static Map<PlanNodeId, Map<String, String>> computeColumnNames(Collection<Input> inputs)
    {
        return inputs.stream()
                .collect(toImmutableMap(
                        Input::planNodeId,
                        input -> input.columnsBySymbolName().entrySet().stream()
                                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().name()))));
    }

    private static Map<PlanNodeId, PlanNodeStatsEstimate> computeStatsEstimates(List<StageInfo> stageInfos)
    {
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> result = ImmutableMap.builder();
        for (StageInfo stageInfo : stageInfos) {
            if (stageInfo.plan() != null) {
                result.putAll(stageInfo.plan().getStatsAndCosts().getStats());
            }
        }
        return result.buildKeepingLast();
    }

    private static Map<PlanNodeId, TableRef> computeTables(List<StageInfo> stageInfos)
    {
        ImmutableMap.Builder<PlanNodeId, TableRef> result = ImmutableMap.builder();
        for (StageInfo stageInfo : stageInfos) {
            stageInfo.tables().forEach((planNodeId, tableInfo) -> result.put(planNodeId, toTableRef(tableInfo)));
        }
        return result.buildKeepingLast();
    }

    private static TableRef toTableRef(TableInfo tableInfo)
    {
        CatalogSchemaTableName tableName = tableInfo.tableName().asCatalogSchemaTableName();
        return new TableRef(
                tableName.getCatalogName(),
                tableName.getSchemaTableName().getSchemaName(),
                tableName.getSchemaTableName().getTableName());
    }

    private record PlanNodes(Map<PlanNodeId, PlanNode> nodesById, Map<PlanNodeId, PlanNodeStats> statsByNode)
    {
        private PlanNodes
        {
            requireNonNull(nodesById, "nodesById is null");
            requireNonNull(statsByNode, "statsByNode is null");
        }
    }

    private static PlanNodes computePlanNodes(StagesInfo stagesInfo, Map<PlanFragmentId, PlanNode> rootByFragmentId)
    {
        Map<PlanNodeId, PlanNodeStats> planNodeStats = PlanNodeStatsSummarizer.aggregateStageStats(stagesInfo.getStages());
        Map<PlanNodeId, PlanNode> nodesById = new HashMap<>();
        Map<PlanNodeId, PlanNodeStats> statsByNode = new HashMap<>();
        collectNodes(stagesInfo.getOutputStage().plan().getRoot(), rootByFragmentId, planNodeStats, nodesById, statsByNode);
        return new PlanNodes(ImmutableMap.copyOf(nodesById), ImmutableMap.copyOf(statsByNode));
    }

    private static void collectNodes(
            PlanNode planNode,
            Map<PlanFragmentId, PlanNode> rootByFragmentId,
            Map<PlanNodeId, PlanNodeStats> currentStats,
            Map<PlanNodeId, PlanNode> nodesById,
            Map<PlanNodeId, PlanNodeStats> statsByNode)
    {
        if (nodesById.containsKey(planNode.getId())) {
            return;
        }
        nodesById.put(planNode.getId(), planNode);
        PlanNodeStats stats = currentStats.get(planNode.getId());
        if (stats != null) {
            statsByNode.put(planNode.getId(), stats);
        }

        List<PlanNode> sources = childrenOf(planNode, rootByFragmentId);
        Map<PlanNodeId, PlanNodeStats> childStats = resolveChildStats(planNode, sources, currentStats);
        for (PlanNode source : sources) {
            collectNodes(source, rootByFragmentId, childStats, nodesById, statsByNode);
        }
    }

    private static List<PlanNode> childrenOf(PlanNode planNode, Map<PlanFragmentId, PlanNode> rootByFragmentId)
    {
        if (planNode instanceof RemoteSourceNode remoteSourceNode) {
            return remoteSourceNode.getSourceFragmentIds().stream()
                    .map(rootByFragmentId::get)
                    .filter(Objects::nonNull)
                    .collect(toImmutableList());
        }
        return ImmutableList.copyOf(planNode.getSources());
    }

    /**
     * A scan's operator stats are reported against the filter or projection sitting on top of it, so
     * hand them down to the scan itself; otherwise a scan would look like it emitted no rows.
     */
    private static Map<PlanNodeId, PlanNodeStats> resolveChildStats(
            PlanNode planNode,
            List<PlanNode> sources,
            Map<PlanNodeId, PlanNodeStats> planNodeStats)
    {
        if (!isTableScanStatsReattachmentCandidate(planNode, sources)) {
            return planNodeStats;
        }
        PlanNodeId tableScanId = sources.getFirst().getId();
        checkArgument(!planNodeStats.containsKey(tableScanId), "Unexpected stats for table scan found");
        return Optional.ofNullable(planNodeStats.get(planNode.getId()))
                .map(stats -> ImmutableMap.of(tableScanId, stats))
                .orElse(ImmutableMap.of());
    }

    private static boolean isTableScanStatsReattachmentCandidate(PlanNode planNode, List<PlanNode> sources)
    {
        if (sources.isEmpty()) {
            return false;
        }
        PlanNode source = sources.getFirst();
        if (planNode instanceof FilterNode) {
            return source instanceof TableScanNode;
        }
        if (planNode instanceof ProjectNode) {
            return source instanceof TableScanNode || isFilterOverTableScan(source);
        }
        return false;
    }

    private static boolean isFilterOverTableScan(PlanNode node)
    {
        return node instanceof FilterNode filterNode
                && !filterNode.getSources().isEmpty()
                && filterNode.getSources().getFirst() instanceof TableScanNode;
    }
}
