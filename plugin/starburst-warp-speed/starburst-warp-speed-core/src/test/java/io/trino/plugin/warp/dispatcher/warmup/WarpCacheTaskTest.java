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
package io.trino.plugin.warp.dispatcher.warmup;

import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.cache.AbortAction;
import io.trino.plugin.warp.dispatcher.cache.AbortOnInitAction;
import io.trino.plugin.warp.dispatcher.cache.CacheAction;
import io.trino.plugin.warp.dispatcher.cache.EmptyPageAction;
import io.trino.plugin.warp.dispatcher.cache.FinishAction;
import io.trino.plugin.warp.dispatcher.cache.ParallelWarmUpLimiter;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.write.WarpCacheFilesMerger;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class WarpCacheTaskTest
{
    @Test
    public void testAbortWithoutAddPage()
            throws InterruptedException
    {
        GlobalConfig globalConfig = new GlobalConfig();
        RowGroupKey rowGroupKey = mock(RowGroupKey.class);
        RowGroupDataService rowGroupDataService = mock(RowGroupDataService.class);
        StorageWarmerService storageWarmerService = mock(StorageWarmerService.class);
        WarpCacheFilesMerger warpCacheFilesMerger = mock(WarpCacheFilesMerger.class);
        MetricsManager metricsManager = mock(MetricsManager.class);
        WarmingManager warmingManager = mock(WarmingManager.class);

        FinishAction finishAction = spy(new FinishAction(rowGroupDataService, storageWarmerService, warpCacheFilesMerger));
        EmptyPageAction emptyPageAction = spy(new EmptyPageAction(metricsManager, warmingManager, rowGroupDataService));
        AbortOnInitAction abortOnInitAction = spy(new AbortOnInitAction(storageWarmerService, metricsManager));
        AbortAction abortAction = spy(new AbortAction(rowGroupDataService, storageWarmerService, warpCacheFilesMerger));
        Map<CacheWarmState, CacheAction> cacheActions = Map.of(
                CacheWarmState.FINISHING, finishAction,
                CacheWarmState.EMPTY_PAGE, emptyPageAction,
                CacheWarmState.ABORT_ON_INIT_PROCESS, abortOnInitAction,
                CacheWarmState.ABORTING, abortAction);

        WarpCacheTask warpCacheTask = new WarpCacheTask(
                globalConfig,
                cacheActions,
                mock(WorkerTaskExecutorService.class),
                storageWarmerService,
                mock(ParallelWarmUpLimiter.class),
                mock(CacheWarmer.class),
                mock(WarmingServiceStats.class),
                emptyList(),
                emptyMap(),
                rowGroupKey,
                false);

        Thread thread = new Thread(warpCacheTask);
        thread.start();
        warpCacheTask.abort();
        thread.join();

        for (CacheAction cacheAction : cacheActions.values()) {
            int expectedTimes = cacheAction instanceof AbortOnInitAction ? 1 : 0;
            verify(cacheAction, times(expectedTimes)).act(any(), anyInt(), eq(rowGroupKey));
            verify(cacheAction, times(expectedTimes)).close(anyList(), eq(rowGroupKey), anyLong(), any());
        }
    }
}
