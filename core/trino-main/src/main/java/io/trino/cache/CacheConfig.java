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
package io.trino.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;

@DefunctConfig({
        "cache.common-subqueries.enabled",
        "cache.min-worker-split-separation",
        "cache.subqueries.enabled",
})
public class CacheConfig
{
    private boolean enabled;
    private double revokingThreshold = 0.9;
    private double revokingTarget = 0.7;
    private boolean cacheAggregationsEnabled = true;
    private boolean cacheProjectionsEnabled = true;
    private DataSize maxSplitSize = DataSize.of(256, DataSize.Unit.MEGABYTE);
    private double dataReductionThreshold = 100f;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("subquery-cache.enabled")
    @ConfigDescription("Enables pipeline level cache")
    public CacheConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.enabled", replacedBy = "subquery-cache.enabled")
    public CacheConfig setLegacyEnabled(boolean enabled)
    {
        return setEnabled(enabled);
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getRevokingThreshold()
    {
        return revokingThreshold;
    }

    @Config("subquery-cache.revoking-threshold")
    @ConfigDescription("Revoke cache memory when memory pool is filled over threshold")
    public CacheConfig setRevokingThreshold(double revokingThreshold)
    {
        this.revokingThreshold = revokingThreshold;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.revoking-threshold", replacedBy = "subquery-cache.revoking-threshold")
    public CacheConfig setLegacyRevokingThreshold(double revokingThreshold)
    {
        return setRevokingThreshold(revokingThreshold);
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getRevokingTarget()
    {
        return revokingTarget;
    }

    @Config("subquery-cache.revoking-target")
    @ConfigDescription("When revoking cache memory, revoke so much that cache memory reservation is below target at the end")
    public CacheConfig setRevokingTarget(double revokingTarget)
    {
        this.revokingTarget = revokingTarget;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.revoking-target", replacedBy = "subquery-cache.revoking-target")
    public CacheConfig setLegacyRevokingTarget(double revokingTarget)
    {
        return setRevokingTarget(revokingTarget);
    }

    public boolean isCacheAggregationsEnabled()
    {
        return cacheAggregationsEnabled;
    }

    @Config("subquery-cache.aggregations.enabled")
    @ConfigDescription("Enables caching of aggregations")
    public CacheConfig setCacheAggregationsEnabled(boolean cacheAggregationsEnabled)
    {
        this.cacheAggregationsEnabled = cacheAggregationsEnabled;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.aggregations.enabled", replacedBy = "subquery-cache.aggregations.enabled")
    public CacheConfig setLegacyCacheAggregationsEnabled(boolean cacheAggregationsEnabled)
    {
        return setCacheAggregationsEnabled(cacheAggregationsEnabled);
    }

    public boolean isCacheProjectionsEnabled()
    {
        return cacheProjectionsEnabled;
    }

    @Config("subquery-cache.projections.enabled")
    @ConfigDescription("Enables caching of projections")
    public CacheConfig setCacheProjectionsEnabled(boolean cacheProjectionsEnabled)
    {
        this.cacheProjectionsEnabled = cacheProjectionsEnabled;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.projections.enabled", replacedBy = "subquery-cache.projections.enabled")
    public CacheConfig setLegacyCacheProjectionsEnabled(boolean cacheProjectionsEnabled)
    {
        return setCacheProjectionsEnabled(cacheProjectionsEnabled);
    }

    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("subquery-cache.max-split-size")
    @ConfigDescription("Upper bound for size of cached split")
    public CacheConfig setMaxSplitSize(DataSize cacheSubqueriesSize)
    {
        this.maxSplitSize = cacheSubqueriesSize;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.max-split-size", replacedBy = "subquery-cache.max-split-size")
    public CacheConfig setLegacyMaxSplitSize(DataSize maxSplitSize)
    {
        return setMaxSplitSize(maxSplitSize);
    }

    @DecimalMin("0.0")
    public double getDataReductionThreshold()
    {
        return dataReductionThreshold;
    }

    @Config("subquery-cache.data-reduction-threshold")
    @ConfigDescription("Minimum factor of data reduction of cached split (values >1 represent data expansion)")
    public CacheConfig setDataReductionThreshold(double dataReductionThreshold)
    {
        this.dataReductionThreshold = dataReductionThreshold;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "cache.data-reduction-threshold", replacedBy = "subquery-cache.data-reduction-threshold")
    public CacheConfig setLegacyDataReductionThreshold(double dataReductionThreshold)
    {
        return setDataReductionThreshold(dataReductionThreshold);
    }
}
