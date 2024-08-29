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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.DispatcherPageSource;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.dispatcher.PageSourceDecision;
import io.trino.plugin.warp.dispatcher.ReadErrorHandler;
import io.trino.plugin.warp.dispatcher.RowGroupCloseHandler;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.query.classifier.WarpCacheColumnHandle;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.read.ChunksQueueService;
import io.trino.plugin.warp.storage.read.LazyCollectorService;
import io.trino.plugin.warp.storage.read.QueryParams;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.storage.read.WarpPageSource;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.EmptyPageSource;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.STATS_LUCENE_PAGE_CACHE_KEY;
import static io.trino.plugin.warp.storage.read.QueryParamsConverter.createQueryParams;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarpCachePageSourceFactory
{
    private static final Logger logger = Logger.get(WarpCachePageSourceFactory.class);

    private final ReadErrorHandler readErrorHandler;
    private final ChunksQueueService chunksQueueService;
    private final StorageCollectorService storageCollectorService;
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final RowGroupDataService rowGroupDataService;
    private final MetricsManager metricsManager;
    private final PredicatesCacheService predicatesCacheService;
    private final QueryClassifier queryClassifier;
    private final DictionaryCacheService dictionaryCacheService;

    private final GlobalConfig globalConfig;
    private final DispatcherPageSourceStats statsDispatcherPageSource;
    private final LazyCollectorService lazyCollectorService;

    @Inject
    public WarpCachePageSourceFactory(StorageEngine storageEngine,
                                      StorageEngineConstants storageEngineConstants,
                                      BufferAllocator bufferAllocator,
                                      RowGroupDataService rowGroupDataService,
                                      MetricsManager metricsManager,
                                      PredicatesCacheService predicatesCacheService,
                                      QueryClassifier queryClassifier,
                                      DictionaryCacheService dictionaryCacheService,
                                      GlobalConfig globalConfig,
                                      ReadErrorHandler readErrorHandler,
                                      ChunksQueueService chunksQueueService,
                                      StorageCollectorService storageCollectorService,
                                      LazyCollectorService lazyCollectorService)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.metricsManager = requireNonNull(metricsManager);
        this.predicatesCacheService = requireNonNull(predicatesCacheService);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.globalConfig = requireNonNull(globalConfig);
        this.readErrorHandler = requireNonNull(readErrorHandler);
        this.chunksQueueService = requireNonNull(chunksQueueService);
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.lazyCollectorService = requireNonNull(lazyCollectorService);
        this.statsDispatcherPageSource = metricsManager.registerMetric(DispatcherPageSourceStats.create(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));
    }

    public Optional<ConnectorPageSource> createConnectorPageSource(RowGroupKey rowGroupKey, PlanSignature planSignature, Optional<UUID> queryStoreId)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        if (rowGroupData == null) {
            return Optional.empty();
        }
        if (rowGroupData.isEmpty()) {
            statsDispatcherPageSource.incempty_page_source();
            return Optional.of(new EmptyPageSource());
        }

        if (queryStoreId.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
        for (int i = 0; i < planSignature.getColumns().size(); i++) {
            columns.add(new WarpCacheColumnHandle(
                    planSignature.getColumns().get(i).toString().toLowerCase(Locale.ROOT),
                    planSignature.getColumnsTypes().get(i)));
        }
        QueryContext queryContext = queryClassifier.classifyCache(columns.build(), queryStoreId, rowGroupData);
        if (!queryContext.getRemainingCollectColumnByBlockIndex().isEmpty()) {
            // might happen if there is not enough memory, see NativeCollectClassifier
            logger.debug("RemainingCollectColumnByBlockIndex is not empty - exiting");
            return Optional.empty();
        }
        CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager, List.of());
        initializeCustomStats(customStatsContext);

        String filePath = rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath());
        long fileModTime = rowGroupData.getRowGroupKey().fileModifiedTime();
        // Caching manager always returns tight results, so lazy collect is set to false.
        QueryParams queryParams = createQueryParams(queryContext, filePath, fileModTime, false);
        WarpPageSource warpPageSource = new WarpPageSource(storageEngine,
                storageEngineConstants,
                Long.MAX_VALUE,
                bufferAllocator,
                queryParams,
                false,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfig,
                chunksQueueService,
                storageCollectorService,
                lazyCollectorService);
        RowGroupCloseHandler closeHandler = new RowGroupCloseHandler();
        try {
            PageSourceDecision pageSourceDecision = PageSourceDecision.WARP;
            DispatcherPageSource dispatcherPageSource = new DispatcherPageSource(EmptyPageSource::new,
                    queryClassifier,
                    Collections.emptyList(), // no proxied in case of cache
                    warpPageSource,
                    queryContext,
                    rowGroupData,
                    pageSourceDecision,
                    statsDispatcherPageSource,
                    closeHandler,
                    null, //used for debug for mixed case, unused in CM
                    null,
                    0, // no proxied in case of cache
                    readErrorHandler,
                    globalConfig);
            statsDispatcherPageSource.addwarp_collect_columns(planSignature.getColumns().size());
            return Optional.of(dispatcherPageSource);
        }
        catch (Exception e) {
            RowGroupData afterLockRowGroupData = rowGroupDataService.get(rowGroupKey);
            if (afterLockRowGroupData != null) {
                closeHandler.accept(afterLockRowGroupData);
            }
            throw e;
        }
    }

    private void initializeCustomStats(CustomStatsContext customStatsContext)
    {
        customStatsContext.getOrRegister(new DispatcherPageSourceStats(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));
        customStatsContext.getOrRegister(new DictionaryStats(DictionaryCacheService.DICTIONARY_STAT_GROUP));
        customStatsContext.getOrRegister(LucenePageCacheStats.create(STATS_LUCENE_PAGE_CACHE_KEY));
    }
}
