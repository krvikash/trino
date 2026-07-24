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

import java.util.List;

import static java.util.Objects.requireNonNull;

public record QuerySummary(
        List<StageSummary> stages,
        List<PlanNodeSummary> planNodes,
        List<OperatorStatsSummary> operators,
        List<OperatorTypeStatsSummary> operatorTypeStats,
        List<Integer> topOperatorsByCpuTime,
        List<Integer> topOperatorsByScheduledTime,
        List<Integer> topStagesByCpuTime,
        List<Integer> topStagesByScheduledTime,
        List<Integer> topStagesByPeakMemory,
        List<Integer> topStagesByNetworkData,
        ResourceUsageTimeSeries resourceUsageTimeSeriesByOperatorType)
{
    public QuerySummary
    {
        requireNonNull(stages, "stages is null");
        requireNonNull(planNodes, "planNodes is null");
        requireNonNull(operators, "operators is null");
        requireNonNull(operatorTypeStats, "operatorTypeStats is null");
        requireNonNull(topOperatorsByCpuTime, "topOperatorsByCpuTime is null");
        requireNonNull(topOperatorsByScheduledTime, "topOperatorsByScheduledTime is null");
        requireNonNull(topStagesByCpuTime, "topStagesByCpuTime is null");
        requireNonNull(topStagesByScheduledTime, "topStagesByScheduledTime is null");
        requireNonNull(topStagesByPeakMemory, "topStagesByPeakMemory is null");
        requireNonNull(topStagesByNetworkData, "topStagesByNetworkData is null");
        requireNonNull(resourceUsageTimeSeriesByOperatorType, "resourceUsageTimeSeriesByOperatorType is null");
    }
}
