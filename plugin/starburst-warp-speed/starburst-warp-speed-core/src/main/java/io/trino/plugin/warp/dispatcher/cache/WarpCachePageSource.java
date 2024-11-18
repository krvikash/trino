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
package io.trino.plugin.warp.dispatcher.cache;

import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.warp.dispatcher.DispatcherPageSource;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

record WarpCachePageSource(StorageEngineTxService txService, DispatcherPageSource dispatcherPageSource, CustomStatsContext customStatsContext)
        implements ConnectorPageSource
{
    public WarpCachePageSource(StorageEngineTxService txService, DispatcherPageSource dispatcherPageSource, CustomStatsContext customStatsContext)
    {
        this.txService = requireNonNull(txService);
        this.dispatcherPageSource = requireNonNull(dispatcherPageSource);
        this.customStatsContext = requireNonNull(customStatsContext);
        txService.updateRunningPageSourcesCount(true);
    }

    @Override
    public long getCompletedBytes()
    {
        return dispatcherPageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dispatcherPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return dispatcherPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return dispatcherPageSource.getNextPage();
    }

    @Override
    public long getMemoryUsage()
    {
        return dispatcherPageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            dispatcherPageSource.close();
        }
        finally {
            customStatsContext.copyStatsToGlobalMetricsManager();
            txService.updateRunningPageSourcesCount(false);
        }
    }

    @Override
    public Metrics getMetrics()
    {
        Metrics.Accumulator result = Metrics.accumulator();
        Map<String, Long> statsMap = new TreeMap<>();
        customStatsContext.getRegisteredStats().forEach((key, value) -> statsMap.putAll(value.statsCounterMapper()));
        statsMap.putAll(customStatsContext.getFixedStats());

        Map<String, Metric<?>> metricsMap = statsMap.entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> new LongCount(entry.getValue())));
        result.add(new Metrics(metricsMap));
        result.add(dispatcherPageSource.getMetrics());
        return result.get();
    }
}
