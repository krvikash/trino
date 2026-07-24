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
import io.airlift.units.Duration;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public record QueryStatsSummary(
        Duration elapsedTime,
        Duration queuedTime,
        Duration analysisTime,
        Duration planningTime,
        Duration executionTime,
        Duration finishingTime,
        boolean finishingTimeExcessive,
        Duration totalCpuTime,
        double cpuLoadAveragePerWorker,
        Duration totalScheduledTime,
        DataSize peakMemory,
        DataSize outputDataSize,
        DataSize internalNetworkInputDataSize,
        DataSize physicalInputDataSize,
        DataSize totalNetworkDataSize,
        DataSize averageNetworkPerWorkerPerSecond,
        int workerCount,
        int totalDrivers,
        Instant createTime,
        Instant endTime)
{
    public QueryStatsSummary
    {
        requireNonNull(elapsedTime, "elapsedTime is null");
        requireNonNull(queuedTime, "queuedTime is null");
        requireNonNull(analysisTime, "analysisTime is null");
        requireNonNull(planningTime, "planningTime is null");
        requireNonNull(executionTime, "executionTime is null");
        requireNonNull(finishingTime, "finishingTime is null");
        requireNonNull(totalCpuTime, "totalCpuTime is null");
        requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        requireNonNull(peakMemory, "peakMemory is null");
        requireNonNull(outputDataSize, "outputDataSize is null");
        requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        requireNonNull(totalNetworkDataSize, "totalNetworkDataSize is null");
        requireNonNull(averageNetworkPerWorkerPerSecond, "averageNetworkPerWorkerPerSecond is null");
        requireNonNull(createTime, "createTime is null");
    }
}
