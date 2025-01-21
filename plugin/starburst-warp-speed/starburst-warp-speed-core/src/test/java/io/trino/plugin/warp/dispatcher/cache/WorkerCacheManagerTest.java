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

import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerCacheManagerTest
{
    private CacheManagerConfig cacheManagerConfig;
    private RowGroupDataService rowGroupDataService;
    private CacheWarmer cacheWarmer;
    private StorageWarmerService storageWarmerService;
    private MemoryContextService memoryContextService;
    private CacheMgrWarmupRuleService warmupRuleService;
    private WorkerCacheManager workerCacheManager;
    private NativeStorageStateHandler nativeStorageStateHandler;

    @BeforeEach
    public void beforeEach()
    {
        cacheManagerConfig = new CacheManagerConfig();
        WarpCachePageSourceFactory warpCachePageSourceFactory = mock(WarpCachePageSourceFactory.class);
        WorkerTaskExecutorService workerTaskExecutorService = mock(WorkerTaskExecutorService.class);
        rowGroupDataService = mock(RowGroupDataService.class);

        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any(WarmingServiceStats.class)))
                .thenReturn(WarmingServiceStats.create());
        when(metricsManager.registerMetric(any(DispatcherPageSourceStats.class)))
                .thenReturn(DispatcherPageSourceStats.create());

        cacheWarmer = mock(CacheWarmer.class);
        storageWarmerService = mock(StorageWarmerService.class);
        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        CatalogNameProvider catalogNameProvider = mock(CatalogNameProvider.class);
        Map<CacheWarmState, CacheAction> cacheActions = Map.of();
        memoryContextService = mock(MemoryContextService.class);
        PredicateHashCalculator predicateHashCalculator = mock(PredicateHashCalculator.class);
        warmupRuleService = new CacheMgrWarmupRuleService(cacheManagerConfig);

        nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(true);

        workerCacheManager = new WorkerCacheManager(
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()),
                cacheManagerConfig,
                warpCachePageSourceFactory,
                workerTaskExecutorService,
                rowGroupDataService,
                metricsManager,
                cacheWarmer,
                storageWarmerService,
                storageEngineConstants,
                catalogNameProvider,
                cacheActions,
                memoryContextService,
                predicateHashCalculator,
                warmupRuleService,
                nativeStorageStateHandler);
    }

    @Test
    public void testSuccessStore()
    {
        cacheManagerConfig.setRulesEnabled(false);

        when(memoryContextService.revokeIsRunning()).thenReturn(false);

        RowGroupKey rowGroupKey = new RowGroupKey("s", "t", "fp", 0, 0, 0, "", "");
        when(rowGroupDataService.get(any())).thenReturn(RowGroupData.builder()
                .warmUpElements(List.of())
                .rowGroupKey(rowGroupKey)
                .isEmpty(true)
                .build());
        when(storageWarmerService.tryAllocateNativeResourceForWarmup()).thenReturn(true);

        PlanSignature planSignature = new PlanSignature(
                new SignatureKey("key"),
                Optional.empty(),
                List.of(new CacheColumnId("1")),
                List.of(BooleanType.BOOLEAN));

        List<WarmupElementWriteMetadata> toWarm = List.of(mock(WarmupElementWriteMetadata.class));
        when(cacheWarmer.getWarmupElementWriteMetadatasToWarm(
                any(),
                any(),
                any(),
                anyBoolean(),
                anyBoolean()))
                .thenReturn(toWarm);
        when(memoryContextService.add(any())).thenReturn(true);

        CacheManager.SplitCache splitCache = workerCacheManager.getSplitCache(planSignature);
        assertThat(splitCache.storePages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isNotEmpty();
    }

    @Test
    public void testSuccessStoreWithRules()
    {
        cacheManagerConfig.setRulesEnabled(true);
        warmupRuleService.replaceAll(List.of(
                new CacheManagerRule("key", 1L, Duration.ZERO)));

        when(memoryContextService.revokeIsRunning()).thenReturn(false);

        RowGroupKey rowGroupKey = new RowGroupKey("s", "t", "fp", 0, 0, 0, "", "");
        when(rowGroupDataService.get(any())).thenReturn(RowGroupData.builder()
                .warmUpElements(List.of())
                .rowGroupKey(rowGroupKey)
                .isEmpty(true)
                .build());
        when(storageWarmerService.tryAllocateNativeResourceForWarmup()).thenReturn(true);

        PlanSignature planSignature = new PlanSignature(
                new SignatureKey("key"),
                Optional.empty(),
                List.of(new CacheColumnId("1")),
                List.of(BooleanType.BOOLEAN));

        List<WarmupElementWriteMetadata> toWarm = List.of(mock(WarmupElementWriteMetadata.class));
        when(cacheWarmer.getWarmupElementWriteMetadatasToWarm(
                any(),
                any(),
                any(),
                anyBoolean(),
                anyBoolean()))
                .thenReturn(toWarm);
        when(memoryContextService.add(any())).thenReturn(true);

        CacheManager.SplitCache splitCache = workerCacheManager.getSplitCache(planSignature);
        assertThat(splitCache.storePages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isNotEmpty();
    }

    @Test
    public void testNoStoreFlow()
    {
        PlanSignature planSignature = new PlanSignature(
                new SignatureKey("key"),
                Optional.empty(),
                List.of(),
                List.of());

        cacheManagerConfig.setRulesEnabled(true);
        warmupRuleService.replaceAll(
                List.of(new CacheManagerRule(planSignature.getKey().toString(), 1L, Duration.ZERO)));

        CacheManager.SplitCache splitCache = workerCacheManager.getSplitCache(planSignature);
        assertThat(splitCache.storePages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isEmpty();

        planSignature = new PlanSignature(
                new SignatureKey("key"),
                Optional.empty(),
                List.of(new CacheColumnId("1")),
                List.of(BooleanType.BOOLEAN));
        cacheManagerConfig.setRulesEnabled(false);
        warmupRuleService.replaceAll(
                List.of(new CacheManagerRule("key", 1L, Duration.ZERO)));

        splitCache = workerCacheManager.getSplitCache(planSignature);
        assertThat(splitCache.storePages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isEmpty();

        cacheManagerConfig.setRulesEnabled(true);
        warmupRuleService.replaceAll(
                List.of(new CacheManagerRule("noMatch", 1L, Duration.ZERO)));

        splitCache = workerCacheManager.getSplitCache(planSignature);
        assertThat(splitCache.storePages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isEmpty();
    }

    @Test
    public void testSkipWhenStorageNotAvailable()
    {
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(false);

        PlanSignature planSignature = new PlanSignature(
                new SignatureKey("key"),
                Optional.empty(),
                List.of(new CacheColumnId("1")),
                List.of(BooleanType.BOOLEAN));

        CacheManager.SplitCache splitCache = workerCacheManager.getSplitCache(planSignature);
        assertThat(splitCache.loadPages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isEmpty();
        assertThat(splitCache.storePages(mock(CacheSplitId.class), TupleDomain.all(), TupleDomain.all()))
                .isEmpty();
    }
}
