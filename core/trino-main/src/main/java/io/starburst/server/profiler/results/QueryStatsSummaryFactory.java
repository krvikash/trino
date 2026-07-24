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

import io.airlift.units.DataSize;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;

import java.util.HashSet;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Extracted from trino-query-analyzer
 */
public final class QueryStatsSummaryFactory
{
    private static final double HIGH_FINISHING_TIME_RATIO = 0.1;

    private QueryStatsSummaryFactory() {}

    public static QueryStatsSummary from(QueryInfo queryInfo)
    {
        QueryStats stats = queryInfo.getQueryStats();
        int workerCount = countUniqueWorkers(queryInfo);

        long totalNetworkBytes = stats.getInternalNetworkInputDataSize().toBytes() + stats.getPhysicalInputDataSize().toBytes();
        DataSize totalNetworkDataSize = DataSize.ofBytes(totalNetworkBytes).succinct();

        return new QueryStatsSummary(
                stats.getElapsedTime(),
                stats.getQueuedTime(),
                stats.getAnalysisTime(),
                stats.getPlanningTime(),
                stats.getExecutionTime(),
                stats.getFinishingTime(),
                stats.getFinishingTime().toMillis() > stats.getElapsedTime().toMillis() * HIGH_FINISHING_TIME_RATIO,
                stats.getTotalCpuTime(),
                cpuLoadAveragePerWorker(stats, workerCount),
                stats.getTotalScheduledTime(),
                stats.getPeakUserMemoryReservation().succinct(),
                stats.getOutputDataSize().succinct(),
                stats.getInternalNetworkInputDataSize().succinct(),
                stats.getPhysicalInputDataSize().succinct(),
                totalNetworkDataSize,
                averageNetworkPerWorkerPerSecond(stats, workerCount, totalNetworkBytes),
                workerCount,
                stats.getTotalDrivers(),
                stats.getCreateTime(),
                stats.getEndTime());
    }

    private static double cpuLoadAveragePerWorker(QueryStats stats, int workerCount)
    {
        if (workerCount == 0) {
            return 0.0;
        }
        long executionTimeMillis = stats.getExecutionTime().toMillis();
        if (executionTimeMillis == 0) {
            return 0.0;
        }
        double maxPossibleCpuTimeMillis = (double) workerCount * executionTimeMillis;
        return stats.getTotalCpuTime().toMillis() / maxPossibleCpuTimeMillis;
    }

    private static DataSize averageNetworkPerWorkerPerSecond(QueryStats stats, int workerCount, long totalNetworkAndPhysicalBytes)
    {
        if (workerCount == 0) {
            return DataSize.ofBytes(0);
        }
        double executionTimeMillis = stats.getExecutionTime().roundTo(MILLISECONDS);
        if (executionTimeMillis == 0) {
            return DataSize.ofBytes(totalNetworkAndPhysicalBytes * 1000).succinct();
        }
        double averageBytesPerMillis = totalNetworkAndPhysicalBytes / (workerCount * executionTimeMillis);
        return DataSize.ofBytes((long) (averageBytesPerMillis * 1000)).succinct();
    }

    private static int countUniqueWorkers(QueryInfo queryInfo)
    {
        return queryInfo.getStages()
                .map(stagesInfo -> {
                    Set<String> uniqueNodeIds = new HashSet<>();
                    for (StageInfo stage : stagesInfo.getStages()) {
                        if (!stagesInfo.getOutputStageId().equals(stage.stageId())) {
                            for (TaskInfo task : stage.tasks()) {
                                uniqueNodeIds.add(task.taskStatus().nodeId());
                            }
                        }
                    }
                    return uniqueNodeIds.size();
                })
                .orElse(0);
    }
}
