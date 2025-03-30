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

import io.trino.operator.OperatorContext;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.InternalDynamicFilter;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record CacheDriverContext(
        Optional<ConnectorPageSource> pageSource,
        Optional<ConnectorPageSink> pageSink,
        InternalDynamicFilter dynamicFilter,
        CacheSplitId cacheSplitId,
        PlanSignature planSignature,
        CacheMetrics cacheMetrics,
        CacheStats cacheStats,
        CachePerformanceTracker cachePerformanceTracker,
        Metrics metrics)
{
    public CacheDriverContext(
            Optional<ConnectorPageSource> pageSource,
            Optional<ConnectorPageSink> pageSink,
            InternalDynamicFilter dynamicFilter,
            CacheSplitId cacheSplitId,
            PlanSignature planSignature,
            CacheMetrics cacheMetrics,
            CacheStats cacheStats,
            CachePerformanceTracker cachePerformanceTracker,
            Metrics metrics)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.cacheSplitId = requireNonNull(cacheSplitId, "cacheSplitId is null");
        this.planSignature = requireNonNull(planSignature, "planSignature is null");
        this.cacheMetrics = requireNonNull(cacheMetrics, "cacheMetrics is null");
        this.cacheStats = requireNonNull(cacheStats, "cacheStats is null");
        this.cachePerformanceTracker = requireNonNull(cachePerformanceTracker, "cachePerformanceTracker is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    public CacheDriverContext withMetrics(Metrics metrics)
    {
        return new CacheDriverContext(pageSource, pageSink, dynamicFilter, cacheSplitId, planSignature, cacheMetrics, cacheStats, cachePerformanceTracker, metrics);
    }

    public static InternalDynamicFilter getDynamicFilter(OperatorContext context, InternalDynamicFilter originalDynamicFilter)
    {
        return context.getDriverContext().getCacheDriverContext()
                .map(CacheDriverContext::dynamicFilter)
                .orElse(originalDynamicFilter);
    }

    public void recordPotentialGain(long cpuNanos)
    {
        cachePerformanceTracker.put(cacheSplitId, planSignature, cpuNanos);
    }

    public Optional<Long> getSparedCpu()
    {
        return Optional.ofNullable(cachePerformanceTracker.get(cacheSplitId, planSignature));
    }
}
