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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
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
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.WarmupCacheData;
import io.trino.plugin.warp.storage.write.WarpCachePageSink;
import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.STATS_DISPATCHER_KEY;
import static io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerCacheManager
        implements CacheManager
{
    private static final Logger logger = Logger.get(WorkerCacheManager.class);
    private final ShapingLogger shapingLogger;
    private final GlobalConfig globalConfig;
    private final ConnectorSync connectorSync;
    private final Map<CacheWarmState, CacheAction> cacheActions;
    private final MemoryContextService memoryContextService;
    private final DispatcherPageSourceFactory dispatcherPageSourceFactory;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final RowGroupDataService rowGroupDataService;
    private final WarmingServiceStats statsWarmingService;

    private final CacheWarmer cacheWarmer;
    private final ObjectMapper objectMapper;
    private final StorageWarmerService storageWarmerService;
    private final int chunkSize;
    private final DispatcherPageSourceStats statsPageSource;

    @Inject
    public WorkerCacheManager(DispatcherPageSourceFactory dispatcherPageSourceFactory,
            WorkerTaskExecutorService workerTaskExecutorService,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            CacheWarmer cacheWarmer,
            ObjectMapperProvider objectMapper,
            StorageWarmerService storageWarmerService,
            StorageEngineConstants storageEngineConstants,
            GlobalConfig globalConfig,
            ConnectorSync connectorSync,
            @Named("CacheActions") Map<CacheWarmState, CacheAction> cacheActions,
            MemoryContextService memoryContextService)
    {
        this.dispatcherPageSourceFactory = requireNonNull(dispatcherPageSourceFactory);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
        this.statsPageSource = metricsManager.registerMetric(DispatcherPageSourceStats.create(STATS_DISPATCHER_KEY));
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.objectMapper = objectMapper.get();
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.chunkSize = 1 << requireNonNull(storageEngineConstants).getChunkSizeShift();
        this.globalConfig = requireNonNull(globalConfig);
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        this.connectorSync = requireNonNull(connectorSync);
        this.cacheActions = requireNonNull(cacheActions);
        this.memoryContextService = requireNonNull(memoryContextService);
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
        long revokedBytes = memoryContextService.revoke(bytesToRevoke);
        shapingLogger.info("revoked %s bytesToRevoke=%s", revokedBytes, bytesToRevoke);
        return revokedBytes;
    }

    private class WarpSplitCache
            implements SplitCache
    {
        private final PlanSignature planSignature;
        private final CommonStoreIdFinder commonStoreIdFinder;

        public WarpSplitCache(PlanSignature planSignature)
        {
            this.planSignature = planSignature;
            commonStoreIdFinder = new CommonStoreIdFinder(rowGroupDataService, planSignature);
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            Optional<ConnectorPageSource> result = Optional.empty();
            try {
                RowGroupKey rowGroupKey = getRowGroupKey(splitId, predicate, unenforcedPredicate);
                Optional<UUID> queryStoreId = commonStoreIdFinder.findAndCache(rowGroupKey);
                result = dispatcherPageSourceFactory.createConnectorPageSource(rowGroupKey, planSignature, queryStoreId);
            }
            catch (Exception e) {
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
            Optional<ConnectorPageSink> res = Optional.empty();
            List<WarmupElementWriteMetadata> toWarm;
            try {
                if (memoryContextService.revokeIsRunning()) {
                    return Optional.empty();
                }
                RowGroupKey rowGroupKey = getRowGroupKey(splitId, predicate, unenforcedPredicate);
                Optional<UUID> storeId = commonStoreIdFinder.getFromCache(rowGroupKey);  // read directly from cache because we assume loadPages have already added it (if exists)
                if (storeId.isPresent()) {
                    shapingLogger.debug("Skipping warming as they are already warmed");
                    return res;
                }
                boolean txMemoryReserved = storageWarmerService.tryAllocateNativeResourceForWarmup();
                if (!txMemoryReserved) {
                    logger.info("nativeResourceForWarmup is not available");
                    return res;
                }

                toWarm = cacheWarmer.getWarmupElementWriteMetadatasToWarm(
                        planSignature.getColumns(), planSignature.getColumnsTypes(), rowGroupKey);
                if (toWarm.isEmpty()) {
                    statsWarmingService.incwarm_warp_cache_skip_zero_columns();
                    logger.debug("nothing to warm for %s", planSignature);
                    return res;
                }
                List<WarmupElementBlocks> warmupElementBlocksList = new ArrayList<>(toWarm.stream().map(x -> new WarmupElementBlocks(x, chunkSize)).toList());
                WarpCacheTask warpCacheTask = new WarpCacheTask(
                        globalConfig,
                        cacheActions,
                        workerTaskExecutorService,
                        storageWarmerService,
                        new WarmupCacheData(warmupElementBlocksList),
                        cacheWarmer,
                        statsWarmingService,
                        toWarm,
                        rowGroupKey,
                        memoryContextService);
                boolean taskAdded = memoryContextService.add(warpCacheTask);
                if (taskAdded) {
                    res = Optional.of(new WarpCachePageSink(warpCacheTask, workerTaskExecutorService));
                }
                else {
                    shapingLogger.debug("Skipping warming since a similar warming is already running. warpCacheTask =%s. runningSize()=%s", warpCacheTask, memoryContextService.getRunningSize());
                }
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to store data from WarpCacheManager splitId=%s, planSignature=%s", splitId, planSignature);
                res = Optional.empty();
            }
            return res;
        }

        @Override
        public void close()
        {
        }

        private RowGroupKey getRowGroupKey(
                CacheSplitId splitId,
                TupleDomain<CacheColumnId> predicate,
                TupleDomain<CacheColumnId> unenforcedPredicate)
                throws JsonProcessingException
        {
            String key = splitId.toString() + "_" +
                    planSignature.getKey().toString() + "_" +
                    objectMapper.writeValueAsString(predicate + "_" +
                            objectMapper.writeValueAsString(unenforcedPredicate));
            if (planSignature.getGroupByColumns().isPresent()) {
                key = key + "_" + planSignature.getGroupByColumns().get();
            }
            String uniqueKey = Hashing.sha256().hashUnencodedChars(key).toString();
            return new RowGroupKey("WarpCache",
                    uniqueKey,
                    "",
                    0,
                    0,
                    0,
                    "",
                    connectorSync.getCatalogName());
        }
    }
}
