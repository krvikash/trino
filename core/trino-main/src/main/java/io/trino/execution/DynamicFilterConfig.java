/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "dynamic-filtering-max-per-driver-row-count",
        "dynamic-filtering-max-per-driver-size",
        "dynamic-filtering-range-row-limit-per-driver",
        "dynamic-filtering.service-thread-count",
        "experimental.dynamic-filtering-max-per-driver-row-count",
        "experimental.dynamic-filtering-max-per-driver-size",
        "experimental.dynamic-filtering-refresh-interval",
        "experimental.enable-dynamic-filtering",
        "enable-coordinator-dynamic-filters-distribution",
        "dynamic-row-filtering.wait-timeout",
        "dynamic-filtering.large-partitioned.range-row-limit-per-driver",
        "dynamic-filtering.large-broadcast.range-row-limit-per-driver",
        "dynamic-filtering.small.max-distinct-values-per-driver",
        "dynamic-filtering.small.max-size-per-driver",
        "dynamic-filtering.small.range-row-limit-per-driver",
        "dynamic-filtering.small.max-size-per-operator",
        "dynamic-filtering.small.max-size-per-filter",
        "dynamic-filtering.small-broadcast.max-distinct-values-per-driver",
        "dynamic-filtering.small-broadcast.max-size-per-driver",
        "dynamic-filtering.small-broadcast.range-row-limit-per-driver",
        "dynamic-filtering.small-broadcast.max-size-per-operator",
        "dynamic-filtering.small-partitioned.max-distinct-values-per-driver",
        "dynamic-filtering.small-partitioned.max-size-per-driver",
        "dynamic-filtering.small-partitioned.range-row-limit-per-driver",
        "dynamic-filtering.small-partitioned.max-size-per-operator",
        "enable-large-dynamic-filters",
})
public class DynamicFilterConfig
{
    private boolean enableDynamicFiltering = true;
    private boolean enableDynamicRowFiltering = true;
    private double dynamicRowFilterSelectivityThreshold = 0.7;

    private Duration smallDynamicFilterWaitTimeout = new Duration(20, SECONDS);
    private long smallDynamicFilterMaxRowCount = 100_000;
    private long smallDynamicFilterMaxNdvCount = 500;
    /*
     * dynamic-filtering.large.* limits are applied when
     * collected over a not pre-partitioned source (when join distribution type is
     * REPLICATED or when FTE is enabled).
     *
     * dynamic-filtering.large-partitioned.*
     * limits are applied when collected over a pre-partitioned source (when join
     * distribution type is PARTITIONED and FTE is disabled).
     *
     * When FTE is enabled dynamic filters are always collected over non partitioned data,
     * hence the dynamic-filtering.large.* limits applied.
     */
    private int largeMaxDistinctValuesPerDriver = 50_000;
    private DataSize largeMaxSizePerDriver = DataSize.of(4, MEGABYTE);
    private DataSize largeMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private int largePartitionedMaxDistinctValuesPerDriver = 20_000;
    private DataSize largePartitionedMaxSizePerDriver = DataSize.of(200, KILOBYTE);
    private DataSize largePartitionedMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private DataSize largeMaxSizePerFilter = DataSize.of(10, MEGABYTE);
    private int bloomFilterMaxDistinctValuesPerDriver = 100_000;
    private int partitionedBloomFilterMaxDistinctValuesPerDriver = 25_000;

    public boolean isEnableDynamicFiltering()
    {
        return enableDynamicFiltering;
    }

    @Config("enable-dynamic-filtering")
    public DynamicFilterConfig setEnableDynamicFiltering(boolean enableDynamicFiltering)
    {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    public boolean isEnableDynamicRowFiltering()
    {
        return enableDynamicRowFiltering;
    }

    @Config("enable-dynamic-row-filtering")
    @LegacyConfig("dynamic-row-filtering.enabled")
    @ConfigDescription("Enable fine-grained filtering of rows in the scan operator using dynamic filters")
    public DynamicFilterConfig setEnableDynamicRowFiltering(boolean enableDynamicRowFiltering)
    {
        this.enableDynamicRowFiltering = enableDynamicRowFiltering;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getDynamicRowFilterSelectivityThreshold()
    {
        return dynamicRowFilterSelectivityThreshold;
    }

    @Config("dynamic-row-filtering.selectivity-threshold")
    @ConfigDescription("Avoid using dynamic row filters when fraction of rows selected is above threshold")
    public DynamicFilterConfig setDynamicRowFilterSelectivityThreshold(double dynamicRowFilterSelectivityThreshold)
    {
        this.dynamicRowFilterSelectivityThreshold = dynamicRowFilterSelectivityThreshold;
        return this;
    }

    @MinDuration("0ms")
    public Duration getSmallDynamicFilterWaitTimeout()
    {
        return smallDynamicFilterWaitTimeout;
    }

    @Config("small-dynamic-filter.wait-timeout")
    @ConfigDescription("Maximum time to wait for small dynamic filter before table scan is started")
    public DynamicFilterConfig setSmallDynamicFilterWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.smallDynamicFilterWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    @Min(0)
    public long getSmallDynamicFilterMaxRowCount()
    {
        return smallDynamicFilterMaxRowCount;
    }

    @Config("small-dynamic-filter.max-row-count")
    @ConfigDescription("Maximum number of rows for dynamic filter to be considered small")
    public DynamicFilterConfig setSmallDynamicFilterMaxRowCount(long smallDynamicFilterMaxRowCount)
    {
        this.smallDynamicFilterMaxRowCount = smallDynamicFilterMaxRowCount;
        return this;
    }

    @Min(0)
    public long getSmallDynamicFilterMaxNdvCount()
    {
        return smallDynamicFilterMaxNdvCount;
    }

    @Config("small-dynamic-filter.max-ndv-count")
    @ConfigDescription("Maximum number of distinct values for dynamic filter to be considered small")
    public DynamicFilterConfig setSmallDynamicFilterMaxNdvCount(long smallDynamicFilterMaxNdvCount)
    {
        this.smallDynamicFilterMaxNdvCount = smallDynamicFilterMaxNdvCount;
        return this;
    }

    @Min(0)
    public int getLargeMaxDistinctValuesPerDriver()
    {
        return largeMaxDistinctValuesPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-distinct-values-per-driver")
    @Config("dynamic-filtering.large.max-distinct-values-per-driver")
    public DynamicFilterConfig setLargeMaxDistinctValuesPerDriver(int largeMaxDistinctValuesPerDriver)
    {
        this.largeMaxDistinctValuesPerDriver = largeMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargeMaxSizePerDriver()
    {
        return largeMaxSizePerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-size-per-driver")
    @Config("dynamic-filtering.large.max-size-per-driver")
    public DynamicFilterConfig setLargeMaxSizePerDriver(DataSize largeMaxSizePerDriver)
    {
        this.largeMaxSizePerDriver = largeMaxSizePerDriver;
        return this;
    }

    @MaxDataSize("100MB")
    public DataSize getLargeMaxSizePerOperator()
    {
        return largeMaxSizePerOperator;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-size-per-operator")
    @Config("dynamic-filtering.large.max-size-per-operator")
    public DynamicFilterConfig setLargeMaxSizePerOperator(DataSize largeMaxSizePerOperator)
    {
        this.largeMaxSizePerOperator = largeMaxSizePerOperator;
        return this;
    }

    @Min(0)
    public int getLargePartitionedMaxDistinctValuesPerDriver()
    {
        return largePartitionedMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.large-partitioned.max-distinct-values-per-driver")
    public DynamicFilterConfig setLargePartitionedMaxDistinctValuesPerDriver(int largePartitionedMaxDistinctValuesPerDriver)
    {
        this.largePartitionedMaxDistinctValuesPerDriver = largePartitionedMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargePartitionedMaxSizePerDriver()
    {
        return largePartitionedMaxSizePerDriver;
    }

    @Config("dynamic-filtering.large-partitioned.max-size-per-driver")
    public DynamicFilterConfig setLargePartitionedMaxSizePerDriver(DataSize largePartitionedMaxSizePerDriver)
    {
        this.largePartitionedMaxSizePerDriver = largePartitionedMaxSizePerDriver;
        return this;
    }

    @MaxDataSize("50MB")
    public DataSize getLargePartitionedMaxSizePerOperator()
    {
        return largePartitionedMaxSizePerOperator;
    }

    @Config("dynamic-filtering.large-partitioned.max-size-per-operator")
    public DynamicFilterConfig setLargePartitionedMaxSizePerOperator(DataSize largePartitionedMaxSizePerOperator)
    {
        this.largePartitionedMaxSizePerOperator = largePartitionedMaxSizePerOperator;
        return this;
    }

    @NotNull
    @MaxDataSize("15MB")
    public DataSize getLargeMaxSizePerFilter()
    {
        return largeMaxSizePerFilter;
    }

    @Config("dynamic-filtering.large.max-size-per-filter")
    public DynamicFilterConfig setLargeMaxSizePerFilter(DataSize largeMaxSizePerFilter)
    {
        this.largeMaxSizePerFilter = largeMaxSizePerFilter;
        return this;
    }

    public int getBloomFilterMaxDistinctValuesPerDriver()
    {
        return bloomFilterMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.bloom-filter.max-distinct-values-per-driver")
    public DynamicFilterConfig setBloomFilterMaxDistinctValuesPerDriver(int bloomFilterMaxDistinctValuesPerDriver)
    {
        this.bloomFilterMaxDistinctValuesPerDriver = bloomFilterMaxDistinctValuesPerDriver;
        return this;
    }

    public int getPartitionedBloomFilterMaxDistinctValuesPerDriver()
    {
        return partitionedBloomFilterMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.partitioned-bloom-filter.max-distinct-values-per-driver")
    public DynamicFilterConfig setPartitionedBloomFilterMaxDistinctValuesPerDriver(int partitionedBloomFilterMaxDistinctValuesPerDriver)
    {
        this.partitionedBloomFilterMaxDistinctValuesPerDriver = partitionedBloomFilterMaxDistinctValuesPerDriver;
        return this;
    }
}
