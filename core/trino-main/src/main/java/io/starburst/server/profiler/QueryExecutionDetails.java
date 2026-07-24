/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.execution.TableInfo;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.planprinter.PlanNodeStats;
import io.trino.sql.planner.planprinter.PlanNodeStatsSummarizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class QueryExecutionDetails
{
    private final QueryStats queryStats;
    private final List<Input> inputs;
    private final Map<String, String> systemProperties;
    private final List<StageInfo> stageInfos;
    private final Map<PlanFragmentId, PlanNode> rootByFragmentId;
    private final Map<PlanNodeId, PlanNode> planNodesById;
    private final Map<PlanNodeId, PlanNodeStats> statsByPlanNode;
    private final Optional<PlanNode> rootNode;
    private final Map<PlanNodeId, PlanNodeStatsEstimate> estimates;
    private final Map<PlanNodeId, TableInfo> tables;
    private final Thresholds thresholds;

    public QueryExecutionDetails(QueryInfo queryInfo, QueryProfilerConfig config)
    {
        requireNonNull(queryInfo, "queryInfo is null");
        requireNonNull(config, "config is null");

        this.queryStats = queryInfo.getQueryStats();
        this.inputs = ImmutableList.copyOf(queryInfo.getInputs());
        this.systemProperties = queryInfo.getSession().getSystemProperties();
        this.thresholds = Thresholds.from(config);

        Optional<StagesInfo> stages = queryInfo.getStages();
        if (stages.isEmpty()) {
            this.stageInfos = ImmutableList.of();
            this.rootByFragmentId = ImmutableMap.of();
            this.planNodesById = ImmutableMap.of();
            this.statsByPlanNode = ImmutableMap.of();
            this.rootNode = Optional.empty();
            this.estimates = ImmutableMap.of();
            this.tables = ImmutableMap.of();
            return;
        }

        List<StageInfo> stageInfos = stages.get().getStages();
        Map<PlanFragmentId, PlanNode> rootByFragmentId = stageInfos.stream()
                .filter(stage -> stage.plan() != null)
                .collect(toImmutableMap(stage -> stage.plan().getId(), stage -> stage.plan().getRoot()));
        PlanNodes planNodes = computePlanNodes(stages.get(), rootByFragmentId);
        PlanNode root = requireNonNull(stages.get().getOutputStage().plan(), "planFragment is null").getRoot();

        this.stageInfos = ImmutableList.copyOf(stageInfos);
        this.rootByFragmentId = rootByFragmentId;
        this.planNodesById = planNodes.nodesById();
        this.statsByPlanNode = planNodes.statsByNode();
        this.rootNode = Optional.ofNullable(planNodes.nodesById().get(root.getId()));
        this.estimates = computeEstimates(stageInfos);
        this.tables = computeTables(stageInfos);
    }

    public QueryStats queryStats()
    {
        return queryStats;
    }

    public List<Input> inputs()
    {
        return inputs;
    }

    public Map<String, String> systemProperties()
    {
        return systemProperties;
    }

    public List<StageInfo> stageInfos()
    {
        return stageInfos;
    }

    public Map<PlanNodeId, PlanNode> planNodesById()
    {
        return planNodesById;
    }

    public Optional<PlanNode> rootNode()
    {
        return rootNode;
    }

    public List<PlanNode> children(PlanNode node)
    {
        return childrenOf(node, rootByFragmentId);
    }

    public Optional<PlanNodeStats> stats(PlanNodeId planNodeId)
    {
        return Optional.ofNullable(statsByPlanNode.get(planNodeId));
    }

    public long outputPositions(PlanNodeId planNodeId)
    {
        return stats(planNodeId).map(PlanNodeStats::getPlanNodeOutputPositions).orElse(0L);
    }

    public DataSize outputDataSize(PlanNodeId planNodeId)
    {
        return stats(planNodeId).map(PlanNodeStats::getPlanNodeOutputDataSize).orElse(DataSize.ofBytes(0));
    }

    public Duration cpuTime(PlanNodeId planNodeId)
    {
        return stats(planNodeId).map(PlanNodeStats::getPlanNodeCpuTime).orElse(new Duration(0, NANOSECONDS));
    }

    public Duration scheduledTime(PlanNodeId planNodeId)
    {
        return stats(planNodeId).map(PlanNodeStats::getPlanNodeScheduledTime).orElse(new Duration(0, NANOSECONDS));
    }

    public Map<PlanNodeId, PlanNodeStatsEstimate> estimates()
    {
        return estimates;
    }

    public Map<PlanNodeId, TableInfo> tables()
    {
        return tables;
    }

    public Thresholds thresholds()
    {
        return thresholds;
    }

    public Optional<String> tableName(PlanNodeId planNodeId)
    {
        return Optional.ofNullable(tables.get(planNodeId))
                .map(tableInfo -> tableInfo.tableName().toString());
    }

    public record Thresholds(
            double topOperatorsPercentage,
            int maxTopOperators,
            double topStagesPercentage,
            int maxTopStages)
    {
        public static Thresholds from(QueryProfilerConfig config)
        {
            return new Thresholds(
                    config.getTopOperatorsPercentage(),
                    config.getMaxTopOperators(),
                    config.getTopStagesPercentage(),
                    config.getMaxTopStages());
        }
    }

    private static <T> Map<PlanNodeId, T> aggregateByPlanNode(List<StageInfo> stageInfos, Function<StageInfo, Map<PlanNodeId, T>> extractor)
    {
        ImmutableMap.Builder<PlanNodeId, T> result = ImmutableMap.builder();
        for (StageInfo stageInfo : stageInfos) {
            result.putAll(extractor.apply(stageInfo));
        }
        return result.buildOrThrow();
    }

    private static Map<PlanNodeId, PlanNodeStatsEstimate> computeEstimates(List<StageInfo> stageInfos)
    {
        return aggregateByPlanNode(stageInfos, stageInfo -> stageInfo.plan() == null ? ImmutableMap.of() : stageInfo.plan().getStatsAndCosts().getStats());
    }

    private static Map<PlanNodeId, TableInfo> computeTables(List<StageInfo> stageInfos)
    {
        return aggregateByPlanNode(stageInfos, StageInfo::tables);
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
