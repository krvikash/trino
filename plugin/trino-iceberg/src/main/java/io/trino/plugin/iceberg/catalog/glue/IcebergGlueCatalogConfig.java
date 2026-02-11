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
package io.trino.plugin.iceberg.catalog.glue;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergGlueCatalogConfig
{
    private boolean cacheTableMetadata = true;
    private Duration metastoreCacheTtl = new Duration(0, SECONDS);
    private Optional<Duration> metastoreCacheRefreshInterval = Optional.empty();
    private long metastoreCacheMaximumSize = 20_000;
    private int metastoreCacheMaxRefreshThreads = 10;

    public boolean isCacheTableMetadata()
    {
        return cacheTableMetadata;
    }

    @Config("iceberg.glue.cache-table-metadata")
    public IcebergGlueCatalogConfig setCacheTableMetadata(boolean cacheTableMetadata)
    {
        this.cacheTableMetadata = cacheTableMetadata;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @Config("iceberg.glue.metastore-cache.ttl")
    @ConfigDescription("Duration for which Glue metastore entries are cached. Caching is disabled when set to 0.")
    public IcebergGlueCatalogConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    public Optional<@MinDuration("1ms") Duration> getMetastoreCacheRefreshInterval()
    {
        return metastoreCacheRefreshInterval;
    }

    @Config("iceberg.glue.metastore-cache.refresh-interval")
    @ConfigDescription("Interval at which cached Glue metastore entries are asynchronously refreshed. If not set, entries are not refreshed until expired.")
    public IcebergGlueCatalogConfig setMetastoreCacheRefreshInterval(Duration metastoreCacheRefreshInterval)
    {
        this.metastoreCacheRefreshInterval = Optional.ofNullable(metastoreCacheRefreshInterval);
        return this;
    }

    @Min(1)
    public long getMetastoreCacheMaximumSize()
    {
        return metastoreCacheMaximumSize;
    }

    @Config("iceberg.glue.metastore-cache.maximum-size")
    @ConfigDescription("Maximum number of Glue metastore entries to cache")
    public IcebergGlueCatalogConfig setMetastoreCacheMaximumSize(long metastoreCacheMaximumSize)
    {
        this.metastoreCacheMaximumSize = metastoreCacheMaximumSize;
        return this;
    }

    @Min(1)
    public int getMetastoreCacheMaxRefreshThreads()
    {
        return metastoreCacheMaxRefreshThreads;
    }

    @Config("iceberg.glue.metastore-cache.max-refresh-threads")
    @ConfigDescription("Maximum number of threads used to refresh cached Glue metastore entries")
    public IcebergGlueCatalogConfig setMetastoreCacheMaxRefreshThreads(int metastoreCacheMaxRefreshThreads)
    {
        this.metastoreCacheMaxRefreshThreads = metastoreCacheMaxRefreshThreads;
        return this;
    }
}
