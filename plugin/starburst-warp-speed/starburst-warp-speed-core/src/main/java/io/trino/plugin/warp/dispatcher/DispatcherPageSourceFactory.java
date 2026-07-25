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

import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.query.data.QueryColumn;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.MatchService;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class DispatcherPageSourceFactory
{
    public static final String WARP_COLLECT = "warp-collect";
    public static final String WARP_MATCH = "warp-match";
    public static final String EXTERNAL_COLLECT = "external-collect";
    public static final String EXTERNAL_MATCH = "external-match";
    public static final String PREFILLED = "prefilled";

    private static final Logger logger = Logger.get(DispatcherPageSourceFactory.class);

    protected final ShapingLoggerFactory shapingLoggerFactory;
    protected final ShapingLogger shapingLogger;
    protected final ReadErrorHandler readErrorHandler;
    protected final CollectTxService collectTxService;
    protected final StorageCollectorService storageCollectorService;
    protected final MatchService matchService;
    protected final StorageEngineConstants storageEngineConstants;
    protected final RowGroupDataService rowGroupDataService;
    protected final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    protected final PredicatesCacheService predicatesCacheService;
    protected final QueryClassifier queryClassifier;

    public DispatcherPageSourceFactory(
            StorageEngineConstants storageEngineConstants,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            PredicatesCacheService predicatesCacheService,
            QueryClassifier queryClassifier,
            ShapingLoggerFactory shapingLoggerFactory,
            ReadErrorHandler readErrorHandler,
            CollectTxService collectTxService,
            StorageCollectorService storageCollectorService,
            MatchService matchService)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.predicatesCacheService = requireNonNull(predicatesCacheService);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.readErrorHandler = requireNonNull(readErrorHandler);
        this.collectTxService = requireNonNull(collectTxService);
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.matchService = requireNonNull(matchService);

        shapingLogger = shapingLoggerFactory.getInstance(DispatcherPageSourceFactory.class);
        metricsManager.registerMetric(DispatcherPageSourceStats.create());
        metricsManager.registerMetric(LucenePageCacheStats.create());
    }

    public static String createFixedStatKey(Object... parts)
    {
        StringJoiner sj = new StringJoiner(":");
        for (Object part : parts) {
            sj.add(part.toString());
        }
        return sj.toString();
    }

    private void updateUsageForEmptyRowGroupData(RowGroupData rowGroupData)
    {
        long lastUsedTimestamp = Instant.now().toEpochMilli();
        rowGroupData.getWarmUpElements().forEach(warmUpElement -> warmUpElement.setUsedTimestamp(lastUsedTimestamp));
    }

    // it is assumed that in case all match are proxied,
    // the warp collect is empty. this is done by the parse API
    protected PageSourceDecision getBasicPageSourceDecision(
            RowGroupData rowGroupData,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            Optional<FilteringStats> filteringStats,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        if (filteringStats.isPresent() && !filteringStats.get().isEfficientFiltering(dispatcherPageSourceStats)) {
            logger.debug("Inefficient filtering - go to proxy");
            dispatcherPageSourceStats.inccached_proxied_files();
            return PageSourceDecision.PROXY;
        }

        if (Objects.isNull(rowGroupData) ||
                rowGroupData.getValidWarmUpElements().isEmpty()) {
            logger.debug("file isn't warmed, reading from connector");
            dispatcherPageSourceStats.inccached_proxied_files();
            return PageSourceDecision.PROXY;
        }

        TupleDomain<ColumnHandle> fullPredicate = dispatcherTableHandle.getFullPredicate();
        if (fullPredicate.isNone()) {
            updateUsageForEmptyRowGroupData(rowGroupData);
            // Optimization: if the predicate is false, we don't read the split at all.
            // It can help dynamic filtering of inner-joins, if there are no build-side values.
            return PageSourceDecision.EMPTY;
        }

        if (rowGroupData.isEmpty()) {
            dispatcherPageSourceStats.incempty_row_group();
            updateUsageForEmptyRowGroupData(rowGroupData);
            return PageSourceDecision.EMPTY;
        }

        // now we mean business, check if related to warp
        dispatcherPageSourceStats.inccached_files();
        if (!dynamicFilter.getCurrentPredicate().isAll()) {
            dispatcherPageSourceStats.incdf_splits();
        }

        if (columns.isEmpty() &&
                dispatcherTableHandle.getFullPredicate().isAll() &&
                dispatcherTableHandle.getWarpExpression().map(expression -> expression.warpExpressionDataLeaves().isEmpty()).orElse(true) &&
                dynamicFilter.getCurrentPredicate().isAll()) {
            // covers the case of 'SELECT count(*) FROM t'
            return PageSourceDecision.PREFILL;
        }

        // important to keep this lock here after basic decisions are made
        // if we can't get a read lock, fallback to proxy
        if (!rowGroupData.getLock().readLock()) {
            return PageSourceDecision.PROXY;
        }

        return PageSourceDecision.UNKNOWN;
    }

    protected PageSourceDecision getPageSourceDecision(QueryContext queryContext)
    {
        PageSourceDecision pageSourceDecision = PageSourceDecision.MIXED;
        if (queryContext.getMatchLeavesDFS().stream().anyMatch(x -> x.getWarmUpElement().getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA)) {
            shapingLogger.error("match column contains WARM_UP_TYPE_DATA, invalid state. use proxy connector. %s", queryContext);
            pageSourceDecision = PageSourceDecision.PROXY;
        }
        else if (queryContext.isPrefilledOnly()) {
            logger.debug("all columns are prefilled -> only prefilled. %s", queryContext);
            pageSourceDecision = PageSourceDecision.PREFILL;
        }
        else if (queryContext.isProxyOnly()) {
            logger.debug("non of the required columns are warm -> only proxy. %s", queryContext);
            pageSourceDecision = PageSourceDecision.PROXY;
        }
        else if (queryContext.isNoneOnly()) {
            pageSourceDecision = PageSourceDecision.EMPTY;
        }
        else if (queryContext.isWarpOnly()) {
            logger.debug("all columns are warmed -> only warp. %s", queryContext);
            pageSourceDecision = PageSourceDecision.WARP;
        }

        logger.debug("pageSourceDecision=%s for queryContext=%s", pageSourceDecision, queryContext);
        return pageSourceDecision;
    }

    protected void increaseMixedCounters(
            DispatcherPageSourceStats stats,
            QueryContext queryContext)
    {
        stats.addexternal_collect_columns(queryContext.getRemainingCollectColumnByBlockIndex().size());
        Set<String> warpMatchColumns = queryContext
                .getMatchLeavesDFS()
                .stream()
                .flatMap(x -> Stream.of(x.getWarpColumn().getName()))
                .collect(Collectors.toSet());
        Set<String> externalMatchColumns = queryContext
                .getPredicateContextData()
                .getRemainingColumns()
                .stream()
                .flatMap(x -> Stream.of(x.getName()))
                .filter(x -> !warpMatchColumns.contains(x))
                .collect(Collectors.toSet());
        long externalMatchColumnsCount = externalMatchColumns.size();
        stats.addexternal_match_columns(externalMatchColumnsCount);
        long transformedColumns = queryContext.getMatchLeavesDFS().stream().filter(x -> x.getWarmUpElement().getWarpColumn().isTransformedColumn()).map(QueryColumn::getWarpColumn).distinct().count();
        stats.addtransformed_column(transformedColumns);
        stats.addwarp_collect_columns(queryContext.getNativeQueryCollectDataList().size());
        stats.addwarp_match_collect_columns(queryContext.getNativeQueryCollectDataList().stream()
                .filter(nativeQueryCollectData -> !MatchCollectType.DISABLED.equals(nativeQueryCollectData.getMatchCollectType()))
                .count());
        long mappedMatchCollect = queryContext.getNativeQueryCollectDataList().stream()
                .filter(nativeQueryCollectData -> MatchCollectType.MAPPED.equals(nativeQueryCollectData.getMatchCollectType()))
                .count();
        stats.addwarp_mapped_match_collect_columns(mappedMatchCollect);
        stats.addwarp_prefilled_collect_columns(queryContext.getPrefilledQueryCollectDataByBlockIndex().size());
        stats.addwarp_match_columns(warpMatchColumns.size());
        stats.addwarp_match_on_simplified_domain(sumColumns(queryContext.getMatchLeavesDFS().stream().filter(QueryMatchData::isSimplifiedDomain)));

        stats.addcached_total_rows(queryContext.getTotalRecords());
    }

    private int sumColumns(Stream<? extends QueryMatchData> matchLeavesStream)
    {
        return (int) matchLeavesStream.count();
    }

    protected void addColumnStats(CustomStatsContext customStatsContext, QueryContext queryContext)
    {
        queryContext.getNativeQueryCollectDataList().forEach(collectData -> customStatsContext.addFixedStat(createFixedStatKey(WARP_COLLECT, collectData.getWarpColumn().getName(), collectData.getWarmUpElement().getWarmUpType()), 1));
        queryContext.getPrefilledQueryCollectDataByBlockIndex().values().forEach(prefilledData -> customStatsContext.addFixedStat(createFixedStatKey(PREFILLED, prefilledData.getWarpColumn().getName()), 1));
        queryContext.getMatchLeavesDFS().forEach(matchData -> customStatsContext.addFixedStat(createFixedStatKey(WARP_MATCH, matchData.getWarpColumn().getName(), matchData.getWarmUpElement().getWarmUpType()), 1));
        queryContext.getRemainingCollectColumnByBlockIndex().values().forEach(collectColumnHandle -> customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_COLLECT, dispatcherProxiedConnectorTransformer.getWarpRegularColumn(collectColumnHandle).getName()), 1));
        queryContext.getPredicateContextData()
                .getRemainingColumns()
                .stream()
                .flatMap(x -> Stream.of(x.getName()))
                .distinct()
                .forEach(columnName -> customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_MATCH, columnName), 1));
    }

    protected void initializeCustomStats(CustomStatsContext customStatsContext)
    {
        customStatsContext.getOrRegister(new DispatcherPageSourceStats());
        customStatsContext.getOrRegister(LucenePageCacheStats.create());
        customStatsContext.getOrRegister(NativeStats.create());
    }

    /**
     * case we return emptyPageSource due to predicate filter we mark all collect and match columns as warp
     */
    protected void addStatsOnFilteredByPredicate(List<ColumnHandle> columns, CustomStatsContext customStatsContext, DispatcherPageSourceStats dispatcherPageSourceStats, QueryContext basicQueryContext)
    {
        dispatcherPageSourceStats.incfiltered_by_predicate();
        columns.forEach(columnHandle -> customStatsContext.addFixedStat(createFixedStatKey(WARP_COLLECT, dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandle).getName(), WarmUpType.WARM_UP_TYPE_DATA), 1));
        Set<RegularColumn> warpMatchColumns = basicQueryContext.getPredicateContextData().getRemainingColumns();
        warpMatchColumns.forEach(regularColumn -> customStatsContext.addFixedStat(createFixedStatKey(WARP_MATCH, regularColumn.getName(), WarmUpType.WARM_UP_TYPE_BASIC), 1));
        dispatcherPageSourceStats.addwarp_collect_columns(columns.size());
        dispatcherPageSourceStats.addwarp_match_columns(warpMatchColumns.size());
    }

    /**
     * in prefill queryContext doesn't hold any WE. we take totalRecords from RG and validate its all the same
     */
    protected int getTotalRecords(RowGroupData rowGroupData)
    {
        long count = rowGroupData.getValidWarmUpElements().stream().map(WarmUpElement::getTotalRecords).distinct().count();
        checkArgument(count == 1, "total records must be distinct");
        return rowGroupData.getValidWarmUpElements().getFirst().getTotalRecords();
    }
}
