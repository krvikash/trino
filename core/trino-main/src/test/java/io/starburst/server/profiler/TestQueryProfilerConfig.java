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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestQueryProfilerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(QueryProfilerConfig.class)
                .setTopOperatorsPercentage(0.8)
                .setMaxTopOperators(10)
                .setTopStagesPercentage(0.8)
                .setMaxTopStages(10)
                .setSlowJdbcTableMinScheduledTimeRatio(0.1)
                .setJoinMaxBroadcastTableSize(DataSize.of(100, MEGABYTE))
                .setSmallFilesMinNumberOfSplits(1000)
                .setSmallFilesPositionsPerSplitThreshold(10_000)
                .setMinBigOutputSize(DataSize.of(100, MEGABYTE))
                .setMinOverlappingScanPercentage(0.25)
                .setMinOverlappingColumns(0.8)
                .setCrossJoinScalingFactor(10)
                .setBigTableTextFileScanSize(DataSize.of(1, GIGABYTE))
                .setBigTableQueryCpuPercentage(0.3)
                .setSlowSplitDistributionPercentage(0.5)
                .setDynamicFilterJoinFilteringPercentage(0.3));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.profiler.top-operators-percentage", "0.9")
                .put("query.profiler.max-top-operators", "5")
                .put("query.profiler.top-stages-percentage", "0.7")
                .put("query.profiler.max-top-stages", "8")
                .put("query.profiler.slow-jdbc-table-min-scheduled-time-ratio", "0.2")
                .put("query.profiler.join-max-broadcast-table-size", "200MB")
                .put("query.profiler.small-files-min-number-of-splits", "500")
                .put("query.profiler.small-files-positions-per-split-threshold", "5000")
                .put("query.profiler.min-big-output-size", "250MB")
                .put("query.profiler.min-overlapping-scan-percentage", "0.5")
                .put("query.profiler.min-overlapping-columns", "0.6")
                .put("query.profiler.cross-join-scaling-factor", "20")
                .put("query.profiler.big-table-text-file-scan-size", "2GB")
                .put("query.profiler.big-table-query-cpu-percentage", "0.4")
                .put("query.profiler.slow-split-distribution-percentage", "0.6")
                .put("query.profiler.dynamic-filter-join-filtering-percentage", "0.4")
                .buildOrThrow();

        QueryProfilerConfig expected = new QueryProfilerConfig()
                .setTopOperatorsPercentage(0.9)
                .setMaxTopOperators(5)
                .setTopStagesPercentage(0.7)
                .setMaxTopStages(8)
                .setSlowJdbcTableMinScheduledTimeRatio(0.2)
                .setJoinMaxBroadcastTableSize(DataSize.of(200, MEGABYTE))
                .setSmallFilesMinNumberOfSplits(500)
                .setSmallFilesPositionsPerSplitThreshold(5000)
                .setMinBigOutputSize(DataSize.of(250, MEGABYTE))
                .setMinOverlappingScanPercentage(0.5)
                .setMinOverlappingColumns(0.6)
                .setCrossJoinScalingFactor(20)
                .setBigTableTextFileScanSize(DataSize.of(2, GIGABYTE))
                .setBigTableQueryCpuPercentage(0.4)
                .setSlowSplitDistributionPercentage(0.6)
                .setDynamicFilterJoinFilteringPercentage(0.4);

        assertFullMapping(properties, expected);
    }
}
