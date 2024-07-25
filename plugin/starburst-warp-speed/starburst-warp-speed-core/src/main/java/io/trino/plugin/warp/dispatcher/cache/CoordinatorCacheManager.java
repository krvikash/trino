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
import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.PlanSignature;

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.STATS_DISPATCHER_KEY;
import static io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService.WORKER_TASK_EXECUTOR_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.WeGroupWarmer.WARMUP_IMPORTER_STAT_GROUP;
import static io.trino.plugin.warp.juffer.PredicatesCacheService.STATS_CACHE_PREDICATE_KEY;

public class CoordinatorCacheManager
        implements CacheManager
{
    @Inject
    public CoordinatorCacheManager(MetricsManager metricsManager)
    {
        metricsManager.registerMetric(DispatcherPageSourceStats.create(STATS_DISPATCHER_KEY));
        metricsManager.registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
        metricsManager.registerMetric(WarmupDemoterStats.create(WARMUP_DEMOTER_STAT_GROUP));
        metricsManager.registerMetric(WarmupImportServiceStats.create(WARMUP_IMPORTER_STAT_GROUP));
        metricsManager.registerMetric(WarmupExportServiceStats.create(WARMUP_EXPORTER_STAT_GROUP));
        metricsManager.registerMetric(WorkerTaskExecutorServiceStats.create(WORKER_TASK_EXECUTOR_STAT_GROUP));
        metricsManager.registerMetric(DictionaryStats.create(DICTIONARY_STAT_GROUP));
        metricsManager.registerMetric(CachePredicatesStats.create(STATS_CACHE_PREDICATE_KEY));
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        throw new UnsupportedOperationException("getSplitCache should read only in worker");
    }

    @SuppressWarnings("deprecation")
    @Override
    public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
    {
        return new WarpPreferredAddressProvider(nodeManager);
    }

    @Override
    public long revokeMemory(long bytesToRevoke)
    {
        return 0;
    }
}
