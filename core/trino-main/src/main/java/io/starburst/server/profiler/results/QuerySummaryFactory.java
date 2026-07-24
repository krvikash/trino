/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.results;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.trino.execution.QueryStats;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageStats;
import io.trino.operator.OperatorStats;
import io.trino.operator.ResourceUsageTimeSeriesRecorder;
import io.trino.operator.ResourceUsageTimeSeriesRecorder.ResourceUsageTimeSeriesSnapshot;
import io.trino.plugin.base.metrics.DistributionSnapshot;
import io.trino.spi.metrics.Metric;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.PlanNodeStats;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.groupingBy;

/**
 * Extracted from trino-query-analyzer
 */
public final class QuerySummaryFactory
{
    private static final DataSize MAX_BUFFER_SIZE = DataSize.of(32, DataSize.Unit.MEGABYTE);

    private QuerySummaryFactory() {}

    public static QuerySummary from(QueryExecutionDetails details)
    {
        QueryStats queryStats = details.queryStats();
        QueryExecutionDetails.Thresholds thresholds = details.thresholds();

        List<StageSummary> stages = extractStages(details.stageInfos(), queryStats);
        List<OperatorStats> rawOperators = queryStats.getOperatorSummaries();
        List<OperatorStatsSummary> allOperators = extractOperators(rawOperators, queryStats);
        List<OperatorTypeStatsSummary> operatorTypeStats = computeOperatorTypeStats(rawOperators, queryStats);
        List<PlanNodeSummary> planNodes = extractPlanNodes(details, rawOperators);

        List<Integer> topOperatorsByCpuTime = topOperatorsByCpuTime(allOperators, queryStats, thresholds);
        List<Integer> topOperatorsByScheduledTime = topOperatorsByScheduledTime(allOperators, queryStats, thresholds);
        Set<Integer> topOperatorIds = ImmutableSet.<Integer>builder()
                .addAll(topOperatorsByCpuTime)
                .addAll(topOperatorsByScheduledTime)
                .build();
        List<OperatorStatsSummary> operators = allOperators.stream()
                .filter(operator -> topOperatorIds.contains(operator.id()))
                .collect(toImmutableList());

        DataSize queryNetwork = totalNetworkInputDataSize(queryStats.getInternalNetworkInputDataSize(), queryStats.getPhysicalInputDataSize());
        return new QuerySummary(
                stages,
                planNodes,
                operators,
                topOperatorTypeStats(operatorTypeStats, queryStats, thresholds),
                topOperatorsByCpuTime,
                topOperatorsByScheduledTime,
                topStagesByCpuTime(stages, queryStats, thresholds),
                topStagesByScheduledTime(stages, queryStats, thresholds),
                topStagesByPeakMemory(stages, queryStats, thresholds),
                topStagesByNetworkData(stages, queryNetwork, thresholds),
                extractResourceUsageTimeSeries(rawOperators));
    }

    private static List<StageSummary> extractStages(List<StageInfo> stageInfos, QueryStats queryStats)
    {
        Map<StageId, StageInfo> stagesById = stageInfos.stream().collect(toImmutableMap(StageInfo::stageId, stage -> stage));
        return stageInfos.stream()
                .map(stageInfo -> toQueryStage(stageInfo, queryStats, isPotentialBottleneck(stageInfo, stagesById)))
                .collect(toImmutableList());
    }

    private static List<OperatorStatsSummary> extractOperators(List<OperatorStats> rawOperators, QueryStats queryStats)
    {
        return IntStream.range(0, rawOperators.size())
                .mapToObj(i -> toQueryOperator(i + 1, rawOperators.get(i), queryStats))
                .collect(toImmutableList());
    }

    private static List<PlanNodeSummary> extractPlanNodes(QueryExecutionDetails details, List<OperatorStats> rawOperators)
    {
        Map<PlanNodeId, List<OperatorStats>> operatorsByPlanNode = rawOperators.stream()
                .collect(groupingBy(OperatorStats::getPlanNodeId));
        return details.planNodesById().values().stream()
                .map(node -> toQueryPlanNode(node, details, operatorsByPlanNode.getOrDefault(node.getId(), ImmutableList.of())))
                .collect(toImmutableList());
    }

    private static List<OperatorTypeStatsSummary> topOperatorTypeStats(List<OperatorTypeStatsSummary> operatorTypeStats, QueryStats queryStats, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(operatorTypeStats, stats -> stats.cpuTime().roundTo(NANOSECONDS), queryStats.getTotalCpuTime().roundTo(NANOSECONDS), thresholds.maxTopOperators(), thresholds.topOperatorsPercentage());
    }

    private static List<Integer> topOperatorsByCpuTime(List<OperatorStatsSummary> operators, QueryStats queryStats, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(operators, o -> o.cpuTime().roundTo(NANOSECONDS), queryStats.getTotalCpuTime().roundTo(NANOSECONDS), thresholds.maxTopOperators(), thresholds.topOperatorsPercentage())
                .stream().map(OperatorStatsSummary::id).collect(toImmutableList());
    }

    private static List<Integer> topOperatorsByScheduledTime(List<OperatorStatsSummary> operators, QueryStats queryStats, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(operators, o -> o.scheduledTime().roundTo(NANOSECONDS), queryStats.getTotalScheduledTime().roundTo(NANOSECONDS), thresholds.maxTopOperators(), thresholds.topOperatorsPercentage())
                .stream().map(OperatorStatsSummary::id).collect(toImmutableList());
    }

    private static List<Integer> topStagesByCpuTime(List<StageSummary> stages, QueryStats queryStats, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(stages, s -> s.totalCpuTime().roundTo(NANOSECONDS), queryStats.getTotalCpuTime().roundTo(NANOSECONDS), thresholds.maxTopStages(), thresholds.topStagesPercentage())
                .stream().map(StageSummary::stageId).collect(toImmutableList());
    }

    private static List<Integer> topStagesByScheduledTime(List<StageSummary> stages, QueryStats queryStats, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(stages, s -> s.scheduledTime().roundTo(NANOSECONDS), queryStats.getTotalScheduledTime().roundTo(NANOSECONDS), thresholds.maxTopStages(), thresholds.topStagesPercentage())
                .stream().map(StageSummary::stageId).collect(toImmutableList());
    }

    private static List<Integer> topStagesByPeakMemory(List<StageSummary> stages, QueryStats queryStats, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(stages, s -> s.peakMemory().toBytes(), queryStats.getPeakUserMemoryReservation().toBytes(), thresholds.maxTopStages(), thresholds.topStagesPercentage())
                .stream().map(StageSummary::stageId).collect(toImmutableList());
    }

    private static List<Integer> topStagesByNetworkData(List<StageSummary> stages, DataSize queryNetwork, QueryExecutionDetails.Thresholds thresholds)
    {
        return topByValue(stages, s -> s.totalNetworkInput().toBytes(), queryNetwork.toBytes(), thresholds.maxTopStages(), thresholds.topStagesPercentage())
                .stream().map(StageSummary::stageId).collect(toImmutableList());
    }

    private static StageSummary toQueryStage(StageInfo stageInfo, QueryStats queryStats, boolean isBottleneck)
    {
        StageStats stageStats = stageInfo.stageStats();
        DataSize stageNetwork = totalNetworkInputDataSize(stageStats.getInternalNetworkInputDataSize(), stageStats.getPhysicalInputDataSize());
        return new StageSummary(
                stageInfo.stageId().id(),
                stageStats.getTotalScheduledTime(),
                percentage(queryStats.getTotalScheduledTime(), stageStats.getTotalScheduledTime()),
                stageStats.getTotalCpuTime(),
                percentage(queryStats.getTotalCpuTime(), stageStats.getTotalCpuTime()),
                stageStats.getPeakUserMemoryReservation(),
                stageNetwork,
                stageStats.getPhysicalInputDataSize(),
                stageStats.getInternalNetworkInputDataSize(),
                stageStats.getOutputBufferUtilization().map(u -> u.p50()).orElse(0.0),
                stageStats.getTotalTasks(),
                isBottleneck);
    }

    private static boolean isPotentialBottleneck(StageInfo stageInfo, Map<StageId, StageInfo> stagesById)
    {
        if (stageInfo.stageStats().getOutputBufferUtilization().map(utilization -> utilization.p50() >= 0.2).orElse(true)) {
            return false;
        }
        if (stageInfo.subStages().isEmpty()) {
            return stageInfo.stageStats().getOutputBufferUtilization().orElseThrow().p90() <= 0.05
                    && stageInfo.stageStats().getOutputDataSize().toBytes() >= MAX_BUFFER_SIZE.toBytes() * stageInfo.tasks().size();
        }
        return stageInfo.subStages().stream()
                .map(stagesById::get)
                .anyMatch(subStage -> subStage.stageStats().getOutputBufferUtilization().map(utilization -> utilization.p50() >= 0.2).orElse(false));
    }

    private static OperatorStatsSummary toQueryOperator(int id, OperatorStats operator, QueryStats queryStats)
    {
        Duration cpuTime = new Duration(
                operator.getAddInputCpu().roundTo(NANOSECONDS) + operator.getGetOutputCpu().roundTo(NANOSECONDS) + operator.getFinishCpu().roundTo(NANOSECONDS),
                NANOSECONDS);
        Duration scheduledTime = new Duration(
                operator.getAddInputWall().roundTo(NANOSECONDS) + operator.getGetOutputWall().roundTo(NANOSECONDS) + operator.getFinishWall().roundTo(NANOSECONDS),
                NANOSECONDS);
        return new OperatorStatsSummary(
                id,
                operator.getAlternativeId(),
                operator.getStageId(),
                operator.getPipelineId(),
                operator.getOperatorId(),
                operator.getOperatorType(),
                operator.getPlanNodeId().toString(),
                operator.getInputPositions(),
                operator.getOutputPositions(),
                cpuTime,
                percentage(queryStats.getTotalCpuTime(), cpuTime),
                scheduledTime,
                percentage(queryStats.getTotalScheduledTime(), scheduledTime));
    }

    private static List<OperatorTypeStatsSummary> computeOperatorTypeStats(List<OperatorStats> operators, QueryStats queryStats)
    {
        Map<String, List<OperatorStats>> operatorsByType = operators.stream().collect(groupingBy(OperatorStats::getOperatorType));
        ImmutableList.Builder<OperatorTypeStatsSummary> result = ImmutableList.builder();
        for (Map.Entry<String, List<OperatorStats>> entry : operatorsByType.entrySet()) {
            List<OperatorStats> typeOperators = entry.getValue();
            long totalCpuNanos = typeOperators.stream().mapToLong(operator ->
                    operator.getAddInputCpu().roundTo(NANOSECONDS) + operator.getGetOutputCpu().roundTo(NANOSECONDS) + operator.getFinishCpu().roundTo(NANOSECONDS)).sum();
            long totalInputPositions = typeOperators.stream().mapToLong(OperatorStats::getInputPositions).sum();
            long totalOutputPositions = typeOperators.stream().mapToLong(OperatorStats::getOutputPositions).sum();
            Duration totalCpuTime = new Duration(totalCpuNanos, NANOSECONDS);
            result.add(new OperatorTypeStatsSummary(
                    entry.getKey(),
                    typeOperators.size(),
                    totalCpuTime,
                    percentage(queryStats.getTotalCpuTime(), totalCpuTime),
                    totalInputPositions,
                    totalOutputPositions));
        }
        return result.build();
    }

    private static ResourceUsageTimeSeries extractResourceUsageTimeSeries(List<OperatorStats> operators)
    {
        Map<String, List<ResourceUsageTimeSeriesSnapshot>> snapshotsByType = new HashMap<>();
        List<ResourceUsageTimeSeriesSnapshot> allSnapshots = new ArrayList<>();
        for (OperatorStats operator : operators) {
            for (Metric<?> metric : operator.getMetrics().getMetrics().values()) {
                if (metric instanceof ResourceUsageTimeSeriesSnapshot snapshot && !snapshot.isEmpty()) {
                    snapshotsByType.computeIfAbsent(operator.getOperatorType(), _ -> new ArrayList<>()).add(snapshot);
                    allSnapshots.add(snapshot);
                }
            }
        }

        if (allSnapshots.isEmpty()) {
            return ResourceUsageTimeSeries.empty();
        }

        ResourceUsageTimeSeriesSnapshot merged = ResourceUsageTimeSeriesRecorder.merge(allSnapshots);
        long startTimeEpochSeconds = merged.startTimeEpochSeconds();
        int bucketWidthSeconds = merged.bucketWidthSeconds();
        int bucketCount = merged.cpuNanosBuckets().length;
        ResourceUsageTimeSeriesSnapshot gridAnchor = ResourceUsageTimeSeriesSnapshot.create(
                startTimeEpochSeconds, bucketWidthSeconds, new long[bucketCount], new long[bucketCount]);

        List<String> operatorTypes = snapshotsByType.keySet().stream().sorted().collect(toImmutableList());
        ImmutableMap.Builder<String, double[]> cpuMillisByOperatorType = ImmutableMap.builder();
        ImmutableMap.Builder<String, double[]> wallMillisByOperatorType = ImmutableMap.builder();
        for (String operatorType : operatorTypes) {
            List<ResourceUsageTimeSeriesSnapshot> typeSnapshots = ImmutableList.<ResourceUsageTimeSeriesSnapshot>builder()
                    .add(gridAnchor)
                    .addAll(snapshotsByType.get(operatorType))
                    .build();
            ResourceUsageTimeSeriesSnapshot typeMerged = ResourceUsageTimeSeriesRecorder.merge(typeSnapshots);
            cpuMillisByOperatorType.put(operatorType, nanosToMillis(typeMerged.cpuNanosBuckets()));
            wallMillisByOperatorType.put(operatorType, nanosToMillis(typeMerged.wallNanosBuckets()));
        }

        return new ResourceUsageTimeSeries(
                startTimeEpochSeconds,
                bucketWidthSeconds,
                bucketCount,
                operatorTypes,
                cpuMillisByOperatorType.buildOrThrow(),
                wallMillisByOperatorType.buildOrThrow());
    }

    private static double[] nanosToMillis(long[] nanos)
    {
        double[] millis = new double[nanos.length];
        for (int i = 0; i < nanos.length; i++) {
            millis[i] = nanos[i] / 1_000_000.0;
        }
        return millis;
    }

    private static PlanNodeSummary toQueryPlanNode(PlanNode node, QueryExecutionDetails details, List<OperatorStats> operators)
    {
        long drivers = operators.stream().mapToLong(OperatorStats::getTotalDrivers).max().orElse(0L);
        Optional<PlanNodeStats> stats = details.stats(node.getId());
        return new PlanNodeSummary(
                node.getId().toString(),
                node.getClass().getSimpleName(),
                details.cpuTime(node.getId()),
                details.scheduledTime(node.getId()),
                stats.map(PlanNodeStats::getPlanNodeInputPositions).orElse(0L),
                stats.map(PlanNodeStats::getPlanNodeInputDataSize).orElse(DataSize.ofBytes(0)),
                stats.map(PlanNodeStats::getPlanNodeOutputDataSize).orElse(DataSize.ofBytes(0)),
                stats.map(PlanNodeStats::getPlanNodeOutputPositions).orElse(0L),
                stats.map(PlanNodeStats::getPlanNodePhysicalInputDataSize).orElse(DataSize.ofBytes(0)),
                drivers,
                getColumnIndexUsage(operators),
                node.getSources().stream().map(child -> child.getId().toString()).collect(toImmutableList()));
    }

    private static List<ColumnIndexUsage> getColumnIndexUsage(List<OperatorStats> operators)
    {
        long totalDrivers = operators.stream().mapToLong(OperatorStats::getTotalDrivers).sum();
        if (totalDrivers == 0) {
            return ImmutableList.of();
        }

        Map<String, Long> indexUsedByColumn = new HashMap<>();
        Set<String> filteredColumns = new HashSet<>();
        for (OperatorStats operatorStats : operators) {
            for (Map.Entry<String, Metric<?>> entry : operatorStats.getConnectorMetrics().getMetrics().entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("warp-match:")) {
                    String[] parts = key.split(":");
                    if (parts.length < 3) {
                        continue;
                    }
                    String column = parts[1];
                    String warmUpType = parts[2];
                    filteredColumns.add(column);
                    if ((warmUpType.equals("WARM_UP_TYPE_BASIC") || warmUpType.equals("WARM_UP_TYPE_LUCENE")) && entry.getValue() instanceof DistributionSnapshot distribution) {
                        indexUsedByColumn.merge(column, distribution.total(), Long::sum);
                    }
                }
                else if (key.startsWith("external-match:")) {
                    filteredColumns.add(key.substring("external-match:".length()));
                }
            }
        }

        if (filteredColumns.isEmpty()) {
            return ImmutableList.of();
        }

        return filteredColumns.stream()
                .sorted()
                .map(column -> {
                    long indexUsedCount = indexUsedByColumn.getOrDefault(column, 0L);
                    return new ColumnIndexUsage(column, indexUsedCount, totalDrivers, (double) indexUsedCount / totalDrivers);
                })
                .collect(toImmutableList());
    }

    private static DataSize totalNetworkInputDataSize(DataSize internalNetworkInputDataSize, DataSize physicalInputDataSize)
    {
        return DataSize.ofBytes(internalNetworkInputDataSize.toBytes() + physicalInputDataSize.toBytes());
    }

    private static double percentage(Duration total, Duration part)
    {
        double totalNanos = total.roundTo(NANOSECONDS);
        return totalNanos > 0 ? part.roundTo(NANOSECONDS) / totalNanos : 0;
    }

    private static <T> List<T> topByValue(List<T> items, ToLongFunction<T> value, long queryTotal, int max, double maxPercentage)
    {
        List<T> sorted = items.stream()
                .sorted(Comparator.comparingLong(value).reversed())
                .limit(max)
                .collect(toImmutableList());

        ImmutableList.Builder<T> result = ImmutableList.builder();
        double soFar = 0;
        for (T item : sorted) {
            soFar += value.applyAsLong(item);
            result.add(item);
            if (queryTotal > 0 && soFar / queryTotal > maxPercentage) {
                break;
            }
        }
        return result.build();
    }
}
