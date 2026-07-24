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

import java.util.List;

import static java.util.Objects.requireNonNull;

public record PlanNodeSummary(
        String planNodeId,
        String planNodeType,
        Duration cpu,
        Duration scheduledTime,
        long inputRows,
        DataSize inputSize,
        DataSize outputSize,
        long outputRows,
        DataSize physicalIn,
        long drivers,
        List<ColumnIndexUsage> columnIndexUsage,
        List<String> childIds)
{
    public PlanNodeSummary
    {
        requireNonNull(planNodeId, "planNodeId is null");
        requireNonNull(planNodeType, "planNodeType is null");
        requireNonNull(cpu, "cpu is null");
        requireNonNull(scheduledTime, "scheduledTime is null");
        requireNonNull(inputSize, "inputSize is null");
        requireNonNull(outputSize, "outputSize is null");
        requireNonNull(physicalIn, "physicalIn is null");
        requireNonNull(columnIndexUsage, "columnIndexUsage is null");
        requireNonNull(childIds, "childIds is null");
    }
}
