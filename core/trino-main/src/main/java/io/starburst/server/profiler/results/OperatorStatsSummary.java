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

import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

public record OperatorStatsSummary(
        int id,
        int alternativeId,
        int stageId,
        int pipelineId,
        int operatorId,
        String operatorType,
        String planNodeId,
        long inputPositions,
        long outputPositions,
        Duration cpuTime,
        double totalCpuTime,
        Duration scheduledTime,
        double totalScheduledTime)
{
    public OperatorStatsSummary
    {
        requireNonNull(operatorType, "operatorType is null");
        requireNonNull(planNodeId, "planNodeId is null");
        requireNonNull(cpuTime, "cpuTime is null");
        requireNonNull(scheduledTime, "scheduledTime is null");
    }
}
