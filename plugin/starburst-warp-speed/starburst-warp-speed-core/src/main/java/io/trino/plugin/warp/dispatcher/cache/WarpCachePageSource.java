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
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

record WarpCachePageSource(StorageEngineTxService txService, ConnectorPageSource pageSource, CustomStatsContext customStatsContext)
        implements ConnectorPageSource
{
    public WarpCachePageSource(StorageEngineTxService txService, ConnectorPageSource pageSource, CustomStatsContext customStatsContext)
    {
        this.txService = requireNonNull(txService);
        this.pageSource = requireNonNull(pageSource);
        this.customStatsContext = requireNonNull(customStatsContext);
        txService.updateRunningPageSourcesCount(true);
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        return pageSource.getNextSourcePage();
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            pageSource.close();
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
        result.add(pageSource.getMetrics());
        return result.get();
    }
}
