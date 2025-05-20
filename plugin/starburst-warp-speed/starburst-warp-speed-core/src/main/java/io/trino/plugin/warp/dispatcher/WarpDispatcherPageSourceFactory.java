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
package io.trino.plugin.warp.dispatcher;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.cache.WarpCachePageSourceFactory;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.query.data.QueryColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.MatchService;
import io.trino.plugin.warp.storage.read.PrefilledPageSource;
import io.trino.plugin.warp.storage.read.QueryParams;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.storage.read.WarpPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.warp.storage.read.QueryParamsConverter.createQueryParams;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarpDispatcherPageSourceFactory
        extends DispatcherPageSourceFactory
{
    private static final Logger logger = Logger.get(WarpCachePageSourceFactory.class);
    private final GlobalConfig globalConfig;
    private final WorkerWarmingService workerWarmingService;
    private final WorkerMemoryManager workerMemoryManager;
    private final NativeStorageStateHandler nativeStorageStateHandler;

    @Inject
    public WarpDispatcherPageSourceFactory(
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
            WorkerWarmingService workerWarmingService,
            WorkerMemoryManager workerMemoryManager,
            NativeStorageStateHandler nativeStorageStateHandler,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        super(
                storageEngineConstants,
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

        this.globalConfig = requireNonNull(globalConfig);
        this.workerWarmingService = requireNonNull(workerWarmingService);
        this.workerMemoryManager = requireNonNull(workerMemoryManager);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
    }

    public ConnectorPageSource createConnectorPageSource(
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            CustomStatsContext customStatsContext)
    {
        initializeCustomStats(customStatsContext);
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());

        if ((!nativeStorageStateHandler.isStorageAvailable() && dispatcherTableHandle.isSubsumedPredicates())) {
            throw new TrinoException(WarpErrorCode.WARP_NATIVE_ERROR,
                    "storage is not available");
        }

        if (!nativeStorageStateHandler.isStorageAvailable() ||
                !dispatcherProxiedConnectorTransformer.isValidForAcceleration(dispatcherTableHandle) ||
                WarpSessionProperties.isBypassEnabled(session)) {
            logger.debug("Query is not valid for acceleration, reading from proxy connector without warmup. dispatcherTableHandle=%s", dispatcherTableHandle);
            QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(columns,
                    dispatcherTableHandle,
                    dynamicFilter,
                    session);
            addProxiedColumnStats(dispatcherPageSourceStats,
                    customStatsContext,
                    columns,
                    basicQueryContext);
            return connectorPageSourceProvider.createPageSource(
                    transactionHandle,
                    session,
                    dispatcherSplit.getProxyConnectorSplit(),
                    dispatcherTableHandle.getProxyConnectorTableHandle(),
                    columns,
                    dynamicFilter);
        }

        ConnectorPageSource connectorPageSource = getConnectorPageSource(
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                customStatsContext);

        workerWarmingService.warm(
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                1);

        return connectorPageSource;
    }

    ConnectorPageSource getConnectorPageSource(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            CustomStatsContext customStatsContext)
    {
        // nice tweak to make load a bit faster in POCs and tests (from the old varada days)
        if (WarpSessionProperties.isEmptyQuery(session)) {
            return new EmptyPageSource();
        }

        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());

        RowGroupKey rowGroupKey = rowGroupDataService.createRowGroupKey(dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                dispatcherSplit.getDeletedFilesHash());

        RowGroupData rowGroupData = rowGroupDataService.getIfPresent(rowGroupKey);
        PageSourceDecision pageSourceDecision = getBasicPageSourceDecision(
                rowGroupData,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                dispatcherPageSourceStats);

        if (PageSourceDecision.EMPTY.equals(pageSourceDecision)) {
            dispatcherPageSourceStats.incempty_page_source();
            return new EmptyPageSource();
        }
        if (PageSourceDecision.PREFILL.equals(pageSourceDecision) && columns.isEmpty()) {
            int totalRecords = getTotalRecords(rowGroupData);
            dispatcherPageSourceStats.incempty_collect_columns();
            return new PrefilledPageSource(emptyMap(), dispatcherPageSourceStats, rowGroupData, totalRecords, Optional.empty());
        }

        RowGroupCloseHandler closeHandler = new RowGroupCloseHandler();
        try {
            QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(columns,
                    dispatcherTableHandle,
                    dynamicFilter,
                    session);

            if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
                addProxiedColumnStats(dispatcherPageSourceStats,
                        customStatsContext,
                        columns,
                        basicQueryContext);

                return createProxiedConnectorPageSource(
                        Optional.empty(),
                        connectorPageSourceProvider,
                        transactionHandle,
                        session,
                        dispatcherSplit,
                        dispatcherTableHandle,
                        columns,
                        dynamicFilter,
                        Optional.empty(),
                        closeHandler,
                        pageSourceDecision);
            }

            final RowGroupData afterLockRowGroupData = rowGroupDataService.getIfPresent(rowGroupKey); // re-fetch the row group since it might have been changed while this flow was in read-lock

            if (logger.isDebugEnabled()) {
                logger.debug("Intersected fullPredicate: %s, dynamicFilter: %s -> into tupleDomain: %s",
                        dispatcherTableHandle.getFullPredicate().toString(session),
                        dynamicFilter.getCurrentPredicate().toString(session),
                        basicQueryContext.getPredicateContextData());
            }

            QueryContext queryContext = queryClassifier.classify(
                    basicQueryContext,
                    afterLockRowGroupData,
                    dispatcherTableHandle,
                    Optional.of(session));

            pageSourceDecision = getPageSourceDecision(queryContext);
            if (PageSourceDecision.EMPTY.equals(pageSourceDecision)) {
                closeHandler.accept(afterLockRowGroupData, "EMPTY-factory", shapingLogger);
                queryClassifier.close(queryContext);
                addStatsOnFilteredByPredicate(columns, customStatsContext, dispatcherPageSourceStats, basicQueryContext);
                return new EmptyPageSource();
            }

            addColumnStats(customStatsContext, queryContext);

            if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
                addProxiedColumnStats(dispatcherPageSourceStats,
                        customStatsContext,
                        columns,
                        queryContext);

                return createProxiedConnectorPageSource(
                        Optional.of(afterLockRowGroupData),
                        connectorPageSourceProvider,
                        transactionHandle,
                        session,
                        dispatcherSplit,
                        dispatcherTableHandle,
                        columns,
                        dynamicFilter,
                        Optional.of(queryContext),
                        closeHandler,
                        PageSourceDecision.PROXY);
            }

            checkArgument(queryContext.getTotalRecords() != QueryClassifier.INVALID_TOTAL_RECORDS, "invalid totalRecords, %s", queryContext);
            if (PageSourceDecision.PREFILL.equals(pageSourceDecision)) {
                dispatcherPageSourceStats.addwarp_prefilled_collect_columns(columns.size());
                queryClassifier.close(queryContext);
                //in prefill queryContext doesn't hold any WE
                int totalRecords = getTotalRecords(rowGroupData);
                return new PrefilledPageSource(queryContext.getPrefilledQueryCollectDataByBlockIndex(),
                        dispatcherPageSourceStats,
                        afterLockRowGroupData,
                        totalRecords,
                        Optional.of(closeHandler));
            }

            if (PageSourceDecision.MIXED.equals(pageSourceDecision) ||
                    PageSourceDecision.WARP.equals(pageSourceDecision)) {
                try {
                    increaseMixedCounters(dispatcherPageSourceStats, queryContext);

                    return createMixedPageSource(connectorPageSourceProvider,
                            queryClassifier,
                            queryContext,
                            transactionHandle,
                            dispatcherTableHandle,
                            session,
                            dispatcherSplit,
                            afterLockRowGroupData,
                            pageSourceDecision,
                            customStatsContext,
                            closeHandler);
                }
                catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        closeHandler.accept(afterLockRowGroupData, "WARP_TX_ALLOCATION_INTERRUPTED", shapingLogger);
                        throw new TrinoException(WarpErrorCode.WARP_TX_ALLOCATION_INTERRUPTED,
                                "interrupted while trying to create page source");
                    }
                    shapingLogger.warn(e, "Failed to create a mixed page source, returning proxied connector page source. rowGroupData=%s, queryContext=%s",
                            afterLockRowGroupData, queryContext);
                    dispatcherPageSourceStats.incexternal_collect_columns();
                }
            }

            return createProxiedConnectorPageSource(
                    Optional.of(afterLockRowGroupData),
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherSplit,
                    dispatcherTableHandle,
                    columns,
                    dynamicFilter,
                    Optional.of(queryContext),
                    closeHandler,
                    PageSourceDecision.PROXY);
        }
        catch (Exception e) {
            RowGroupData afterLockRowGroupData = rowGroupDataService.getIfPresent(rowGroupKey);
            if (afterLockRowGroupData != null) {
                closeHandler.accept(afterLockRowGroupData, "exception-factory", shapingLogger);
            }
            throw e;
        }
    }

    private ConnectorPageSource createProxiedConnectorPageSource(
            Optional<RowGroupData> rowGroupDataOpt,
            ConnectorPageSourceProvider proxiedConnectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            Optional<QueryContext> queryContext,
            RowGroupCloseHandler closeHandler,
            PageSourceDecision pageSourceDecision)
    {
        rowGroupDataOpt.ifPresent(rowGroupData -> closeHandler.accept(rowGroupData, "createProxiedConnectorPageSource", shapingLogger));
        queryContext.ifPresent(queryClassifier::close);

        ConnectorTableHandle connectorTableHandle;
        ConnectorSplit proxiedSplit;
        if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
            connectorTableHandle = dispatcherTableHandle.getProxyConnectorTableHandle();
            proxiedSplit = dispatcherSplit.getProxyConnectorSplit();
        }
        else {
            connectorTableHandle = dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(dispatcherTableHandle);
            proxiedSplit = dispatcherProxiedConnectorTransformer.createProxiedConnectorNonFilteredSplit(dispatcherSplit.getProxyConnectorSplit());
        }
        return proxiedConnectorPageSourceProvider.createPageSource(transactionHandle,
                session,
                proxiedSplit,
                connectorTableHandle,
                columns,
                dynamicFilter);
    }

    private ConnectorPageSource createMixedPageSource(
            ConnectorPageSourceProvider connectorPageSourceProvider,
            QueryClassifier queryClassifier,
            QueryContext queryContext,
            ConnectorTransactionHandle transactionHandle,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            RowGroupData rowGroupData,
            PageSourceDecision pageSourceDecision,
            CustomStatsContext customStatsContext,
            RowGroupCloseHandler closeHandler)
    {
        Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider = null;

        if (PageSourceDecision.WARP.equals(pageSourceDecision)) {
            proxiedConnectorPageSourceProvider = EmptyPageSource::new;
        }
        else if (PageSourceDecision.MIXED.equals(pageSourceDecision)) {
            proxiedConnectorPageSourceProvider = () -> createProxiedConnectorPageSource(
                    Optional.empty(),
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherSplit,
                    dispatcherTableHandle,
                    ImmutableList.copyOf(queryContext.getRemainingCollectColumns()),
                    DynamicFilter.EMPTY,
                    Optional.empty(),
                    closeHandler,
                    PageSourceDecision.MIXED);
        }

        boolean isMixedQuery = PageSourceDecision.MIXED.equals(pageSourceDecision);
        String filePath = rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath());
        long fileModTime = rowGroupData.getRowGroupKey().fileModifiedTime();
        QueryParams queryParams = createQueryParams(workerMemoryManager, queryContext, filePath, fileModTime, isMixedQuery, session.getQueryId());
        WarpPageSource warpPageSource = new WarpPageSource(
                storageEngineConstants,
                dispatcherTableHandle.getLimit().orElse(Long.MAX_VALUE),
                queryParams,
                predicatesCacheService,
                customStatsContext,
                shapingLoggerFactory,
                storageCollectorService,
                matchService,
                workerMemoryManager);

        DispatcherPageSourceStats pageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        List<Type> warpWithoutPrefilledAndProxiedCollectTypes = Stream.concat(
                        queryContext.getRemainingCollectColumns().stream().map(dispatcherProxiedConnectorTransformer::getColumnType),
                        queryContext.getNativeQueryCollectDataList().stream().map(QueryColumn::getType))
                .collect(toImmutableList());
        long deletedRowsCount = dispatcherProxiedConnectorTransformer.getDeletedRowsCount(dispatcherSplit.getProxyConnectorSplit());
        return new DispatcherPageSource(proxiedConnectorPageSourceProvider,
                queryClassifier,
                warpWithoutPrefilledAndProxiedCollectTypes,
                warpPageSource,
                queryContext,
                rowGroupData,
                pageSourceDecision,
                pageSourceStats,
                closeHandler,
                dispatcherSplit,
                dispatcherTableHandle,
                deletedRowsCount,
                readErrorHandler,
                globalConfig,
                shapingLoggerFactory);
    }

    private void addProxiedColumnStats(DispatcherPageSourceStats globalPageSourceStats,
            CustomStatsContext customStatsContext,
            List<ColumnHandle> columns,
            QueryContext queryContext)
    {
        long externalMatchSize = queryContext.getPredicateContextData()
                .getRemainingColumns()
                .stream()
                .flatMap(x -> Stream.of(x.getName()))
                .distinct()
                .count();
        if (columns.isEmpty()) { //couldn't find any representative column to get from warp
            globalPageSourceStats.incexternal_collect_columns();
        }
        else {
            globalPageSourceStats.addexternal_collect_columns(columns.size());
            globalPageSourceStats.addexternal_match_columns(externalMatchSize);
        }
        customStatsContext.addFixedStat(EXTERNAL_MATCH, externalMatchSize);
        queryContext.getPredicateContextData()
                .getRemainingColumns()
                .forEach(warpColumn -> customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_MATCH, warpColumn.getName()), 1));
        ImmutableList<ColumnHandle> remainingCollectColumns = queryContext.getRemainingCollectColumns();
        if (remainingCollectColumns != null) {
            remainingCollectColumns.forEach(columnHandle ->
                    customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_COLLECT, dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandle).getName()), 1));
        }
    }
}
