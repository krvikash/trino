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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class QueryProfilerConfig
{
    private double topOperatorsPercentage = 0.8;
    private int maxTopOperators = 10;
    private double topStagesPercentage = 0.8;
    private int maxTopStages = 10;
    private double slowJdbcTableMinScheduledTimeRatio = 0.1;
    private DataSize joinMaxBroadcastTableSize = DataSize.of(100, MEGABYTE);
    private int smallFilesMinNumberOfSplits = 1000;
    private int smallFilesPositionsPerSplitThreshold = 10_000;
    private DataSize minBigOutputSize = DataSize.of(100, MEGABYTE);
    private double minOverlappingScanPercentage = 0.25;
    private double minOverlappingColumns = 0.8;
    private double crossJoinScalingFactor = 10;
    private DataSize bigTableTextFileScanSize = DataSize.of(1, GIGABYTE);
    private double bigTableQueryCpuPercentage = 0.3;
    private double slowSplitDistributionPercentage = 0.5;
    private double dynamicFilterJoinFilteringPercentage = 0.3;

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getTopOperatorsPercentage()
    {
        return topOperatorsPercentage;
    }

    @Config("query.profiler.top-operators-percentage")
    @ConfigDescription("Cumulative fraction of query CPU time used to select the top operators reported in the analysis")
    public QueryProfilerConfig setTopOperatorsPercentage(double topOperatorsPercentage)
    {
        this.topOperatorsPercentage = topOperatorsPercentage;
        return this;
    }

    @Min(1)
    public int getMaxTopOperators()
    {
        return maxTopOperators;
    }

    @Config("query.profiler.max-top-operators")
    @ConfigDescription("Maximum number of top operators reported in the analysis")
    public QueryProfilerConfig setMaxTopOperators(int maxTopOperators)
    {
        this.maxTopOperators = maxTopOperators;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getTopStagesPercentage()
    {
        return topStagesPercentage;
    }

    @Config("query.profiler.top-stages-percentage")
    @ConfigDescription("Cumulative fraction of query resources used to select the top stages reported in the analysis")
    public QueryProfilerConfig setTopStagesPercentage(double topStagesPercentage)
    {
        this.topStagesPercentage = topStagesPercentage;
        return this;
    }

    @Min(1)
    public int getMaxTopStages()
    {
        return maxTopStages;
    }

    @Config("query.profiler.max-top-stages")
    @ConfigDescription("Maximum number of top stages reported in the analysis")
    public QueryProfilerConfig setMaxTopStages(int maxTopStages)
    {
        this.maxTopStages = maxTopStages;
        return this;
    }

    public double getSlowJdbcTableMinScheduledTimeRatio()
    {
        return slowJdbcTableMinScheduledTimeRatio;
    }

    @Config("query.profiler.slow-jdbc-table-min-scheduled-time-ratio")
    @ConfigDescription("Minimum fraction of total query scheduled time a JDBC table scan must take to be flagged as slow")
    public QueryProfilerConfig setSlowJdbcTableMinScheduledTimeRatio(double slowJdbcTableMinScheduledTimeRatio)
    {
        this.slowJdbcTableMinScheduledTimeRatio = slowJdbcTableMinScheduledTimeRatio;
        return this;
    }

    public DataSize getJoinMaxBroadcastTableSize()
    {
        return joinMaxBroadcastTableSize;
    }

    @Config("query.profiler.join-max-broadcast-table-size")
    @ConfigDescription("Maximum build-side size for which a broadcast join distribution is considered appropriate")
    public QueryProfilerConfig setJoinMaxBroadcastTableSize(DataSize joinMaxBroadcastTableSize)
    {
        this.joinMaxBroadcastTableSize = joinMaxBroadcastTableSize;
        return this;
    }

    public int getSmallFilesMinNumberOfSplits()
    {
        return smallFilesMinNumberOfSplits;
    }

    @Config("query.profiler.small-files-min-number-of-splits")
    @ConfigDescription("Minimum number of splits a table scan must have before it is checked for reading small files")
    public QueryProfilerConfig setSmallFilesMinNumberOfSplits(int smallFilesMinNumberOfSplits)
    {
        this.smallFilesMinNumberOfSplits = smallFilesMinNumberOfSplits;
        return this;
    }

    public int getSmallFilesPositionsPerSplitThreshold()
    {
        return smallFilesPositionsPerSplitThreshold;
    }

    @Config("query.profiler.small-files-positions-per-split-threshold")
    @ConfigDescription("Maximum average number of rows per split below which a table scan is flagged as reading small files")
    public QueryProfilerConfig setSmallFilesPositionsPerSplitThreshold(int smallFilesPositionsPerSplitThreshold)
    {
        this.smallFilesPositionsPerSplitThreshold = smallFilesPositionsPerSplitThreshold;
        return this;
    }

    public DataSize getMinBigOutputSize()
    {
        return minBigOutputSize;
    }

    @Config("query.profiler.min-big-output-size")
    @ConfigDescription("Minimum query output data size above which the output is flagged as large")
    public QueryProfilerConfig setMinBigOutputSize(DataSize minBigOutputSize)
    {
        this.minBigOutputSize = minBigOutputSize;
        return this;
    }

    public double getMinOverlappingScanPercentage()
    {
        return minOverlappingScanPercentage;
    }

    @Config("query.profiler.min-overlapping-scan-percentage")
    @ConfigDescription("Minimum fraction of query resources a repeated table scan must consume to be flagged as redundant")
    public QueryProfilerConfig setMinOverlappingScanPercentage(double minOverlappingScanPercentage)
    {
        this.minOverlappingScanPercentage = minOverlappingScanPercentage;
        return this;
    }

    public double getMinOverlappingColumns()
    {
        return minOverlappingColumns;
    }

    @Config("query.profiler.min-overlapping-columns")
    @ConfigDescription("Minimum fraction of overlapping columns for two scans of the same table to be considered redundant")
    public QueryProfilerConfig setMinOverlappingColumns(double minOverlappingColumns)
    {
        this.minOverlappingColumns = minOverlappingColumns;
        return this;
    }

    public double getCrossJoinScalingFactor()
    {
        return crossJoinScalingFactor;
    }

    @Config("query.profiler.cross-join-scaling-factor")
    @ConfigDescription("Factor by which a join's output rows may exceed its input rows before it is flagged as expanding")
    public QueryProfilerConfig setCrossJoinScalingFactor(double crossJoinScalingFactor)
    {
        this.crossJoinScalingFactor = crossJoinScalingFactor;
        return this;
    }

    public DataSize getBigTableTextFileScanSize()
    {
        return bigTableTextFileScanSize;
    }

    @Config("query.profiler.big-table-text-file-scan-size")
    @ConfigDescription("Minimum scanned data size for a text-format table scan to be flagged as large")
    public QueryProfilerConfig setBigTableTextFileScanSize(DataSize bigTableTextFileScanSize)
    {
        this.bigTableTextFileScanSize = bigTableTextFileScanSize;
        return this;
    }

    public double getBigTableQueryCpuPercentage()
    {
        return bigTableQueryCpuPercentage;
    }

    @Config("query.profiler.big-table-query-cpu-percentage")
    @ConfigDescription("Minimum fraction of query CPU time a large text-file table scan must consume to be flagged")
    public QueryProfilerConfig setBigTableQueryCpuPercentage(double bigTableQueryCpuPercentage)
    {
        this.bigTableQueryCpuPercentage = bigTableQueryCpuPercentage;
        return this;
    }

    public double getSlowSplitDistributionPercentage()
    {
        return slowSplitDistributionPercentage;
    }

    @Config("query.profiler.slow-split-distribution-percentage")
    @ConfigDescription("Minimum fraction of query execution time spent generating splits for a stage to be flagged as slow")
    public QueryProfilerConfig setSlowSplitDistributionPercentage(double slowSplitDistributionPercentage)
    {
        this.slowSplitDistributionPercentage = slowSplitDistributionPercentage;
        return this;
    }

    public double getDynamicFilterJoinFilteringPercentage()
    {
        return dynamicFilterJoinFilteringPercentage;
    }

    @Config("query.profiler.dynamic-filter-join-filtering-percentage")
    @ConfigDescription("Minimum join input-row filtering ratio expected before a missing or ineffective dynamic filter is flagged")
    public QueryProfilerConfig setDynamicFilterJoinFilteringPercentage(double dynamicFilterJoinFilteringPercentage)
    {
        this.dynamicFilterJoinFilteringPercentage = dynamicFilterJoinFilteringPercentage;
        return this;
    }
}
