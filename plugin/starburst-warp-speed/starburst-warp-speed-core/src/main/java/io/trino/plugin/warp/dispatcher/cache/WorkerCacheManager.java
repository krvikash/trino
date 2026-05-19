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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.warp.dispatcher.warmup.WarpCacheTask;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.write.WarmupCacheData;
import io.trino.plugin.warp.storage.write.WarpCachePageSink;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerCacheManager
        implements CacheManager
{
    private static final Logger logger = Logger.get(WorkerCacheManager.class);

    private final ShapingLoggerFactory shapingLoggerFactory;
    private final CacheManagerConfig cacheManagerConfig;
    private final WarpCachePageSourceFactory warpCachePageSourceFactory;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final RowGroupDataService rowGroupDataService;
    private final CacheWarmer cacheWarmer;
    private final StorageWarmerService storageWarmerService;
    private final CatalogNameProvider catalogNameProvider;
    private final Map<CacheWarmState, CacheAction> cacheActions;
    private final MemoryContextService memoryContextService;
    private final PredicateHashCalculator predicateHashCalculator;
    private final CacheMgrWarmupRuleService warmupRuleService;
    private final NativeStorageStateHandler nativeStorageStateHandler;

    private final ShapingLogger shapingLogger;
    private final int chunkSize;
    private final WarmingServiceStats statsWarmingService;
    private final DispatcherPageSourceStats statsPageSource;
    private final HashFunction hashFunction;

    @Inject
    public WorkerCacheManager(
            ShapingLoggerFactory shapingLoggerFactory,
            CacheManagerConfig cacheManagerConfig,
            WarpCachePageSourceFactory warpCachePageSourceFactory,
            WorkerTaskExecutorService workerTaskExecutorService,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            CacheWarmer cacheWarmer,
            StorageWarmerService storageWarmerService,
            StorageEngineConstants storageEngineConstants,
            CatalogNameProvider catalogNameProvider,
            @Named("CacheActions") Map<CacheWarmState, CacheAction> cacheActions,
            MemoryContextService memoryContextService,
            PredicateHashCalculator predicateHashCalculator,
            CacheMgrWarmupRuleService warmupRuleService,
            NativeStorageStateHandler nativeStorageStateHandler)
    {
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.cacheManagerConfig = requireNonNull(cacheManagerConfig);
        this.warpCachePageSourceFactory = requireNonNull(warpCachePageSourceFactory);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.cacheActions = requireNonNull(cacheActions);
        this.memoryContextService = requireNonNull(memoryContextService);
        this.predicateHashCalculator = requireNonNull(predicateHashCalculator);
        this.warmupRuleService = requireNonNull(warmupRuleService);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);

        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
        this.chunkSize = 1 << requireNonNull(storageEngineConstants).getChunkSizeShift();
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create());
        this.statsPageSource = metricsManager.registerMetric(DispatcherPageSourceStats.create());
        this.hashFunction = Hashing.farmHashFingerprint64();
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return new WarpSplitCache(signature);
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
        return memoryContextService.revoke(bytesToRevoke);
    }

    private class WarpSplitCache
            implements SplitCache
    {
        private final PlanSignature planSignature;
        private final CommonStoreIdFinder commonStoreIdFinder;

        WarpSplitCache(PlanSignature planSignature)
        {
            // this has a smaller memory footprint
            this.planSignature = new PlanSignature(
                    new SignatureKey(warmupRuleService.hash(planSignature.getKey().toString())),
                    planSignature.getGroupByColumns(),
                    planSignature.getColumns(),
                    planSignature.getColumnsTypes());
            commonStoreIdFinder = new CommonStoreIdFinder(rowGroupDataService, planSignature);
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            if (isSkipped()) {
                return Optional.empty();
            }

            Optional<ConnectorPageSource> result = Optional.empty();
            try {
                // if we stored the data after filtering, we don't need to filter again
                RowGroupKey rowGroupKey = getRowGroupKey(splitId, predicate, unenforcedPredicate);
                Optional<UUID> queryStoreId = commonStoreIdFinder.findAndCache(rowGroupKey);
                result = warpCachePageSourceFactory.createConnectorPageSource(rowGroupKey, planSignature, queryStoreId, TupleDomain.all(), TupleDomain.all());

                if (cacheManagerConfig.isBasicIndexEnabled() && result.isEmpty() && !(predicate.isAll() && unenforcedPredicate.isAll())) {
                    rowGroupKey = getRowGroupKey(splitId, TupleDomain.all(), TupleDomain.all());
                    queryStoreId = commonStoreIdFinder.findAndCache(rowGroupKey);
                    result = warpCachePageSourceFactory.createConnectorPageSource(rowGroupKey, planSignature, queryStoreId, predicate, unenforcedPredicate);
                }
            }
            catch (Throwable e) {
                shapingLogger.error(e, "failed to load pages splitId=%s, planSignature=%s", splitId, planSignature);
            }
            if (result.isPresent()) {
                statsPageSource.incwarp_cache_manager();
            }
            else {
                statsPageSource.incskip_warp_cache_manager();
            }
            return result;
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            if (isSkipped()) {
                statsWarmingService.incwarm_warp_cache_skip_zero_columns();
                return Optional.empty();
            }

            if (memoryContextService.revokeIsRunning()) {
                return Optional.empty();
            }

            try {
                RowGroupKey rowGroupKey = getRowGroupKey(splitId, predicate, unenforcedPredicate);
                Optional<UUID> storeId = commonStoreIdFinder.getFromCache(rowGroupKey);  // read directly from cache because we assume loadPages have already added it (if exists)
                if (storeId.isPresent()) {
                    logger.debug("Skipping warming as they are already warmed");
                    return Optional.empty();
                }
                boolean txMemoryReserved = storageWarmerService.tryAllocateNativeResourceForWarmup();
                if (!txMemoryReserved) {
                    shapingLogger.info("nativeResourceForWarmup is not available");
                    return Optional.empty();
                }

                boolean isOrderDeterministic = isOrderDeterministic(predicate, unenforcedPredicate);
                List<WarmupElementWriteMetadata> toWarm = cacheWarmer.getWarmupElementWriteMetadatasToWarm(
                        planSignature.getColumns(), planSignature.getColumnsTypes(), rowGroupKey, isOrderDeterministic, cacheManagerConfig.isBasicIndexEnabled());
                if (toWarm.isEmpty()) {
                    logger.debug("nothing to warm for %s", planSignature);
                    return Optional.empty();
                }
                Map<Integer, List<CacheWarmupElementArgs>> connectorIndexToWarmColumns = toWarm.stream()
                        .collect(Collectors.groupingBy(
                                WarmupElementWriteMetadata::connectorBlockIndex,
                                Collectors.mapping(
                                        writeMetadata -> new CacheWarmupElementArgs(writeMetadata, new WarmupElementBlocks(chunkSize)),
                                        Collectors.toList())));
                WarpCacheTask warpCacheTask = new WarpCacheTask(
                        cacheActions,
                        workerTaskExecutorService,
                        storageWarmerService,
                        new WarmupCacheData(connectorIndexToWarmColumns, shapingLoggerFactory),
                        cacheWarmer,
                        statsWarmingService,
                        rowGroupKey,
                        memoryContextService,
                        shapingLoggerFactory);
                boolean taskAdded = memoryContextService.add(warpCacheTask);
                if (taskAdded) {
                    return Optional.of(new WarpCachePageSink(warpCacheTask, workerTaskExecutorService));
                }
                else {
                    logger.debug("Skipping warming since a similar warming is already running. warpCacheTask =%s. runningSize()=%s", warpCacheTask, memoryContextService.getRunningSize());
                }
            }
            catch (Throwable e) {
                shapingLogger.error(e, "failed to store data from WarpCacheManager splitId=%s, planSignature=%s", splitId, planSignature);
            }
            return Optional.empty();
        }

        @Override
        public void close() {}

        private boolean isSkipped()
        {
            return !nativeStorageStateHandler.isStorageAvailable() || planSignature.getColumns().isEmpty();
        }

        private boolean isOrderDeterministic(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            return predicate.isAll() &&
                    unenforcedPredicate.isAll() &&
                    (planSignature.getGroupByColumns().isEmpty() || planSignature.getGroupByColumns().get().isEmpty());
        }

        private RowGroupKey getRowGroupKey(
                CacheSplitId splitId,
                TupleDomain<CacheColumnId> predicate,
                TupleDomain<CacheColumnId> unenforcedPredicate)
                throws Throwable
        {
            long predicateMap = predicateHashCalculator.getHash(normalizeTupleDomain(predicate));
            long unenforcedPredicateMap = predicateHashCalculator.getHash(normalizeTupleDomain(unenforcedPredicate));

            String key = splitId.toString() + "_" +
                    predicateMap + "_" +
                    unenforcedPredicateMap;
            if (planSignature.getGroupByColumns().isPresent()) {
                key = key + "_" + planSignature.getGroupByColumns().get();
            }
            String uniqueKey = hashFunction.hashString(key, StandardCharsets.UTF_8).toString();
            String schema = "WarpCache";
            return new RowGroupKey(
                    schema,
                    planSignature.getKey().toString(),
                    uniqueKey,
                    0,
                    0,
                    0,
                    "",
                    catalogNameProvider.get());
        }
    }
}
