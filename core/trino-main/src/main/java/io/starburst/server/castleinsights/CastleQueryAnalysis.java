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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.execution.TableInfo;
import io.trino.operator.OperatorStats;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
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
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

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
    private final Map<PlanFragmentId, List<Symbol>> fragmentOutputLayouts;
    private final Set<DynamicFilterId> narrowingDynamicFilters;

    public CastleQueryAnalysis(QueryInfo queryInfo)
    {
        requireNonNull(queryInfo, "queryInfo is null");

        this.queryId = queryInfo.getQueryId().toString();
        this.columnNamesBySymbolName = computeColumnNames(queryInfo.getInputs());
        this.operatorStats = queryInfo.getQueryStats().getOperatorSummaries().stream()
                .collect(toImmutableListMultimap(OperatorStats::getPlanNodeId, identity()));
        this.narrowingDynamicFilters = computeNarrowingDynamicFilters(queryInfo);

        Optional<StagesInfo> stages = queryInfo.getStages();
        if (stages.isEmpty()) {
            this.rootNode = Optional.empty();
            this.rootByFragmentId = ImmutableMap.of();
            this.planNodesById = ImmutableMap.of();
            this.stats = ImmutableMap.of();
            this.statsEstimates = ImmutableMap.of();
            this.tables = ImmutableMap.of();
            this.fragmentOutputLayouts = ImmutableMap.of();
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
        this.fragmentOutputLayouts = stageInfos.stream()
                .filter(stage -> stage.plan() != null)
                .collect(toImmutableMap(stage -> stage.plan().getId(), stage -> stage.plan().getOutputPartitioningScheme().getOutputLayout()));
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

    public List<PlanNode> planNodes(Predicate<PlanNode> predicate)
    {
        return planNodesById.values().stream()
                .filter(predicate)
                .collect(toImmutableList());
    }

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
     * The dynamic filters whose collected domain constrained the probe key space, i.e. was anything other
     * than {@code ALL}. Such a filter may have removed probe rows before they reached the join that drove
     * it. It only <em>may</em> have: a domain covering every key the probe holds is still not {@code ALL}
     * yet removes nothing, so this over-reports rather than under-reports, which suits a caller that would
     * rather withhold a measurement than trust a biased one.
     */
    public Set<DynamicFilterId> narrowingDynamicFilters()
    {
        return narrowingDynamicFilters;
    }

    /**
     * The table column a symbol produced by {@code node} originates from, or empty when it comes from
     * a computation rather than a column, or cannot be traced to a single column.
     */
    public Optional<ColumnRef> traceColumn(PlanNode node, Symbol symbol)
    {
        return switch (node) {
            case TableScanNode scan when scan.getOutputSymbols().contains(symbol) -> table(scan.getId())
                    .flatMap(tableRef -> columnName(scan.getId(), symbol.name())
                            .map(column -> new ColumnRef(tableRef, column)));
            case ProjectNode project -> unwrapColumnSymbol(project.getAssignments().assignments().get(symbol))
                    .flatMap(source -> traceThroughChildren(node, source));
            case ExchangeNode exchange -> {
                List<PlanNode> sources = exchange.getSources();
                yield traceFanIn(
                        exchange.getOutputSymbols().indexOf(symbol),
                        sources.size(),
                        (source, index) -> traceColumn(sources.get(source), exchange.getInputs().get(source).get(index)));
            }
            case RemoteSourceNode remoteSource -> {
                List<PlanFragmentId> fragmentIds = remoteSource.getSourceFragmentIds();
                yield traceFanIn(
                        remoteSource.getOutputSymbols().indexOf(symbol),
                        fragmentIds.size(),
                        (source, index) -> traceProducedColumn(fragmentIds.get(source), index));
            }
            // Any other node (FilterNode and the like) passes the symbol through unchanged.
            default -> traceThroughChildren(node, symbol);
        };
    }

    /**
     * The one column every source agrees on at output {@code index}, or empty if they disagree or any
     * does not resolve, so a union of different columns is not attributed to an arbitrary one.
     */
    private static Optional<ColumnRef> traceFanIn(int index, int sourceCount, BiFunction<Integer, Integer, Optional<ColumnRef>> resolver)
    {
        if (index < 0) {
            return Optional.empty();
        }
        ImmutableSet.Builder<ColumnRef> columns = ImmutableSet.builder();
        for (int source = 0; source < sourceCount; source++) {
            Optional<ColumnRef> column = resolver.apply(source, index);
            if (column.isEmpty()) {
                return Optional.empty();
            }
            columns.add(column.get());
        }
        Set<ColumnRef> distinct = columns.build();
        return distinct.size() == 1 ? Optional.of(getOnlyElement(distinct)) : Optional.empty();
    }

    /**
     * Resolves the symbol a producing fragment emits at {@code index}. The fragment's output layout is
     * the ordering a remote source reads positionally; its root node's outputs are only a superset of
     * that layout, in no particular order, so the layout is what the index refers to.
     */
    private Optional<ColumnRef> traceProducedColumn(PlanFragmentId fragmentId, int index)
    {
        List<Symbol> outputLayout = fragmentOutputLayouts.get(fragmentId);
        PlanNode root = rootByFragmentId.get(fragmentId);
        if (outputLayout == null || root == null || index >= outputLayout.size()) {
            return Optional.empty();
        }
        return traceColumn(root, outputLayout.get(index));
    }

    private Optional<ColumnRef> traceThroughChildren(PlanNode node, Symbol symbol)
    {
        for (PlanNode child : children(node)) {
            if (child.getOutputSymbols().contains(symbol)) {
                return traceColumn(child, symbol);
            }
        }
        return Optional.empty();
    }

    private static Optional<Symbol> unwrapColumnSymbol(Expression expression)
    {
        // A cast keeps the value tied to the column it came from, so trace through casts, including
        // nested ones; anything else is a real computation and ends the trace.
        Expression unwrapped = expression;
        while (unwrapped instanceof Cast cast) {
            unwrapped = cast.expression();
        }
        if (unwrapped instanceof Reference reference) {
            return Optional.of(new Symbol(reference.type(), reference.name()));
        }
        return Optional.empty();
    }

    /**
     * A table column a plan symbol was traced back to.
     */
    public record ColumnRef(TableRef table, String column)
    {
        public ColumnRef
        {
            requireNonNull(table, "table is null");
            requireNonNull(column, "column is null");
        }
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

    // The string a Domain collapses to when it matches everything; see Domain.toString(int).
    private static final String ALL_DOMAIN = "ALL";

    private static Set<DynamicFilterId> computeNarrowingDynamicFilters(QueryInfo queryInfo)
    {
        return queryInfo.getQueryStats().getDynamicFiltersStats().getDynamicFilterDomainStats().stream()
                .filter(stats -> !stats.getSimplifiedDomain().equals(ALL_DOMAIN))
                .map(DynamicFilterDomainStats::getDynamicFilterId)
                .collect(toImmutableSet());
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
