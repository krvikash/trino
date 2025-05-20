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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherPageSource;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandleBuilderProvider;
import io.trino.plugin.warp.dispatcher.PageSourceDecision;
import io.trino.plugin.warp.dispatcher.ReadErrorHandler;
import io.trino.plugin.warp.dispatcher.RowGroupCloseHandler;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.ClassificationType;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.query.classifier.WarpCacheColumnHandle;
import io.trino.plugin.warp.dispatcher.query.classifier.WarpCacheTableHandle;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.MatchService;
import io.trino.plugin.warp.storage.read.PrefilledPageSource;
import io.trino.plugin.warp.storage.read.QueryParams;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.storage.read.WarpPageSource;
import io.trino.plugin.warp.util.DefaultFakeConnectorSession;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.storage.read.QueryParamsConverter.createQueryParams;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarpCachePageSourceFactory
        extends DispatcherPageSourceFactory
{
    private final MetricsManager metricsManager;
    private final GlobalConfig globalConfig;
    private final StorageEngineTxService txService;
    private final WorkerMemoryManager workerMemoryManager;
    private final DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;

    @Inject
    public WarpCachePageSourceFactory(
            StorageEngineConstants storageEngineConstants,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            PredicatesCacheService predicatesCacheService,
            QueryClassifier queryClassifier,
            GlobalConfig globalConfig,
            ReadErrorHandler readErrorHandler,
            CollectTxService collectTxService,
            StorageCollectorService storageCollectorService,
            MatchService matchService,
            StorageEngineTxService txService,
            WorkerMemoryManager workerMemoryManager,
            DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        super(storageEngineConstants,
                rowGroupDataService,
                metricsManager,
                dispatcherProxiedConnectorTransformer,
                predicatesCacheService,
                queryClassifier,
                shapingLoggerFactory,
                readErrorHandler,
                collectTxService,
                storageCollectorService,
                matchService);

        this.metricsManager = requireNonNull(metricsManager);
        this.globalConfig = requireNonNull(globalConfig);
        this.txService = requireNonNull(txService);
        this.workerMemoryManager = requireNonNull(workerMemoryManager);
        this.dispatcherTableHandleBuilderProvider = requireNonNull(dispatcherTableHandleBuilderProvider);
    }

    public Optional<ConnectorPageSource> createConnectorPageSource(
            RowGroupKey rowGroupKey,
            PlanSignature planSignature,
            Optional<UUID> queryStoreId,
            TupleDomain<CacheColumnId> predicate,
            TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager, List.of());
        initializeCustomStats(customStatsContext);
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());

        if (queryStoreId.isEmpty()) {
            return Optional.empty();
        }

        Map<CacheColumnId, ColumnHandle> columnIdToHandle = new HashMap<>(planSignature.getColumns().size());
        ImmutableList.Builder<ColumnHandle> columnsBuilder = ImmutableList.builder();
        for (int i = 0; i < planSignature.getColumns().size(); i++) {
            WarpCacheColumnHandle columnHandle = new WarpCacheColumnHandle(
                    planSignature.getColumns().get(i).toString().toLowerCase(Locale.ROOT),
                    planSignature.getColumnsTypes().get(i));
            columnIdToHandle.put(planSignature.getColumns().get(i), columnHandle);
            columnsBuilder.add(columnHandle);
        }
        List<ColumnHandle> columns = columnsBuilder.build();

        DispatcherTableHandle dispatcherTableHandle = createDispatcherTableHandle(predicate, unenforcedPredicate, columnIdToHandle);

        RowGroupData rowGroupData = rowGroupDataService.getIfPresent(rowGroupKey);
        // This method also locks the rowGroup (when necessary)
        PageSourceDecision pageSourceDecision = getBasicPageSourceDecision(
                rowGroupData,
                dispatcherTableHandle,
                columns,
                DynamicFilter.EMPTY,
                dispatcherPageSourceStats);
        if (PageSourceDecision.EMPTY.equals(pageSourceDecision)) {
            dispatcherPageSourceStats.incempty_page_source();
            return Optional.of(new EmptyPageSource());
        }
        if (PageSourceDecision.PREFILL.equals(pageSourceDecision) && columns.isEmpty()) {
            int totalRecords = getTotalRecords(rowGroupData);
            dispatcherPageSourceStats.incempty_collect_columns();
            PrefilledPageSource prefilledPageSource = new PrefilledPageSource(emptyMap(), dispatcherPageSourceStats, rowGroupData, totalRecords, Optional.empty());
            return Optional.of(new WarpCachePageSource(txService, prefilledPageSource, customStatsContext));
        }
        if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
            return Optional.empty();
        }

        RowGroupCloseHandler closeHandler = new RowGroupCloseHandler();
        final RowGroupData afterLockRowGroupData = rowGroupDataService.getIfPresent(rowGroupKey); // re-fetch the row group since it might have been changed while this flow was in read-lock
        QueryContext queryContext = null;
        try {
            ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
            QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(columns, dispatcherTableHandle, DynamicFilter.EMPTY, session);

            queryContext = queryClassifier.classify(
                    basicQueryContext,
                    afterLockRowGroupData,
                    dispatcherTableHandle,
                    Optional.of(session),
                    queryStoreId,
                    ClassificationType.QUERY);
            if (!queryContext.getRemainingCollectColumnByBlockIndex().isEmpty() || // might happen if there is not enough memory, see NativeCollectClassifier
                    !queryContext.getPredicateContextData().getRemainingColumns().isEmpty() ||
                    !queryContext.isCanBeTight()) {
                closeResources(closeHandler, afterLockRowGroupData, queryContext, "no memory");
                return Optional.empty();
            }

            pageSourceDecision = getPageSourceDecision(queryContext);
            if (PageSourceDecision.EMPTY.equals(pageSourceDecision)) {
                closeResources(closeHandler, afterLockRowGroupData, queryContext, "EMPTY");
                addStatsOnFilteredByPredicate(columns, customStatsContext, dispatcherPageSourceStats, basicQueryContext);
                return Optional.of(new EmptyPageSource());
            }

            addColumnStats(customStatsContext, queryContext);

            checkArgument(queryContext.getTotalRecords() != QueryClassifier.INVALID_TOTAL_RECORDS, "invalid totalRecords, %s", queryContext);
            if (PageSourceDecision.PREFILL.equals(pageSourceDecision)) {
                dispatcherPageSourceStats.addwarp_prefilled_collect_columns(columns.size());
                queryClassifier.close(queryContext);
                //in prefill queryContext doesn't hold any WE
                int totalRecords = getTotalRecords(rowGroupData);
                PrefilledPageSource prefilledPageSource = new PrefilledPageSource(
                        queryContext.getPrefilledQueryCollectDataByBlockIndex(),
                        dispatcherPageSourceStats,
                        afterLockRowGroupData,
                        totalRecords,
                        Optional.of(closeHandler));
                return Optional.of(new WarpCachePageSource(txService, prefilledPageSource, customStatsContext));
            }

            if (pageSourceDecision != PageSourceDecision.WARP) {
                closeResources(closeHandler, afterLockRowGroupData, queryContext, "PROXY");
                return Optional.empty();
            }

            increaseMixedCounters(dispatcherPageSourceStats, queryContext);

            String filePath = afterLockRowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath());
            long fileModTime = afterLockRowGroupData.getRowGroupKey().fileModifiedTime();
            QueryParams queryParams = createQueryParams(workerMemoryManager, queryContext, filePath, fileModTime, false, session.getQueryId());
            WarpPageSource warpPageSource = new WarpPageSource(
                    storageEngineConstants,
                    Long.MAX_VALUE,
                    queryParams,
                    predicatesCacheService,
                    customStatsContext,
                    shapingLoggerFactory,
                    storageCollectorService,
                    matchService,
                    workerMemoryManager);
            DispatcherPageSource dispatcherPageSource = new DispatcherPageSource(EmptyPageSource::new,
                    queryClassifier,
                    Collections.emptyList(), // no proxied in case of cache
                    warpPageSource,
                    queryContext,
                    afterLockRowGroupData,
                    pageSourceDecision,
                    dispatcherPageSourceStats,
                    closeHandler,
                    null, //used for debug for mixed case, unused in CM
                    null,
                    0, // no proxied in case of cache
                    readErrorHandler,
                    globalConfig,
                    shapingLoggerFactory);
            return Optional.of(new WarpCachePageSource(txService, dispatcherPageSource, customStatsContext));
        }
        catch (Exception e) {
            closeResources(closeHandler, afterLockRowGroupData, queryContext, "EXCEPTION");
            throw new RuntimeException(format("Failed to create page source. queryStoreId=%s, rowGroupData=%s, queryContext=%s, dispatcherTableHandle=%s",
                    queryStoreId, afterLockRowGroupData, queryContext, dispatcherTableHandle), e);
        }
    }

    private DispatcherTableHandle createDispatcherTableHandle(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate, Map<CacheColumnId, ColumnHandle> columnIdToHandle)
    {
        int predicateThreshold = globalConfig.getPredicateSimplifyThreshold(); // read from global config directly since there is no session
        ConnectorTableHandle connectorTableHandle = new WarpCacheTableHandle();

        // TODO: Currently, the classification process doesn't support a distinction
        //  between a predicate that requires tightness (subsumedPredicates=true) and a predicate that doesn't.
        //  For the sake of simplicity, we currently intersect the predicates, but in the future,
        //  we should consider adding such support.
        TupleDomain<ColumnHandle> fullPredicate = predicate.intersect(unenforcedPredicate).transformKeys(columnIdToHandle::get);

        return dispatcherTableHandleBuilderProvider
                .builder(predicateThreshold, connectorTableHandle)
                .fullPredicate(fullPredicate)
                .subsumedPredicates(true)
                .build();
    }

    private void closeResources(RowGroupCloseHandler closeHandler, RowGroupData afterLockRowGroupData, QueryContext queryContext, String caller)
    {
        try {
            closeHandler.accept(afterLockRowGroupData, caller, shapingLogger);
        }
        finally {
            if (queryContext != null) {
                try {
                    List<PredicateCacheData> predicateCacheData = queryContext.getMatchLeavesDFS().stream()
                            .map(QueryMatchData::getPredicateCacheData)
                            .toList();
                    predicatesCacheService.markFinished(predicateCacheData);
                }
                finally {
                    queryClassifier.close(queryContext);
                }
            }
        }
    }
}
