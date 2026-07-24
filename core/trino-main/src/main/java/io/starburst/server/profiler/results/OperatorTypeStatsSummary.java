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

public record OperatorTypeStatsSummary(
        String type,
        int count,
        Duration cpuTime,
        double queryCpuTime,
        long totalRowsIn,
        long totalRowsOut)
{
    public OperatorTypeStatsSummary
    {
        requireNonNull(type, "type is null");
        requireNonNull(cpuTime, "cpuTime is null");
    }
}
