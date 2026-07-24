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

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record ResourceUsageTimeSeries(
        long startTimeEpochSeconds,
        int bucketWidthSeconds,
        int bucketCount,
        List<String> operatorTypes,
        Map<String, double[]> cpuMillisByOperatorType,
        Map<String, double[]> wallMillisByOperatorType)
{
    public ResourceUsageTimeSeries
    {
        requireNonNull(operatorTypes, "operatorTypes is null");
        requireNonNull(cpuMillisByOperatorType, "cpuMillisByOperatorType is null");
        requireNonNull(wallMillisByOperatorType, "wallMillisByOperatorType is null");
    }

    public static ResourceUsageTimeSeries empty()
    {
        return new ResourceUsageTimeSeries(-1, 1, 0, ImmutableList.of(), ImmutableMap.of(), ImmutableMap.of());
    }
}
