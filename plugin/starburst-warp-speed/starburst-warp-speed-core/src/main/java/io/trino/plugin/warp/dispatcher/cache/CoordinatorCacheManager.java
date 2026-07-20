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

import com.google.inject.Inject;
import io.trino.plugin.warp.gen.stats.CachePredicatesStats;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.gen.stats.WarmupExportServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.gen.stats.WorkerTaskExecutorServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.PlanSignature;

public class CoordinatorCacheManager
        implements CacheManager
{
    @Inject
    public CoordinatorCacheManager(MetricsManager metricsManager)
    {
        metricsManager.registerMetric(DispatcherPageSourceStats.create());
        metricsManager.registerMetric(WarmingServiceStats.create());
        metricsManager.registerMetric(WarmupDemoterStats.create());
        metricsManager.registerMetric(WarmupImportServiceStats.create());
        metricsManager.registerMetric(WarmupExportServiceStats.create());
        metricsManager.registerMetric(WorkerTaskExecutorServiceStats.create());
        metricsManager.registerMetric(DictionaryStats.create());
        metricsManager.registerMetric(CachePredicatesStats.create());
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        throw new UnsupportedOperationException("getSplitCache should read only in worker");
    }

    @Override
    public long revokeMemory(long bytesToRevoke)
    {
        return 0;
    }
}
