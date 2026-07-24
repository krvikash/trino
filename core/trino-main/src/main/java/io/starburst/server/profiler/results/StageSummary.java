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

import static java.util.Objects.requireNonNull;

public record StageSummary(
        int stageId,
        Duration scheduledTime,
        double queryScheduledTime,
        Duration totalCpuTime,
        double queryCpuTime,
        DataSize peakMemory,
        DataSize totalNetworkInput,
        DataSize physicalNetworkInput,
        DataSize internalNetworkInput,
        double outputBuffer,
        int tasks,
        boolean isBottleneck)
{
    public StageSummary
    {
        requireNonNull(scheduledTime, "scheduledTime is null");
        requireNonNull(totalCpuTime, "totalCpuTime is null");
        requireNonNull(peakMemory, "peakMemory is null");
        requireNonNull(totalNetworkInput, "totalNetworkInput is null");
        requireNonNull(physicalNetworkInput, "physicalNetworkInput is null");
        requireNonNull(internalNetworkInput, "internalNetworkInput is null");
    }
}
