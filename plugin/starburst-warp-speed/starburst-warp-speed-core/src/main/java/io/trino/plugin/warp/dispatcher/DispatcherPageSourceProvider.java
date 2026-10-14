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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.expression.rewrite.WarpExpression;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.IoExecutor;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DispatcherPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger logger = Logger.get(DispatcherPageSourceProvider.class);

    private final ConnectorPageSourceProvider connectorPageSourceProvider;
    private final WarpDispatcherPageSourceFactory pageSourceFactory;
    private final StorageEngineTxService txService;
    private final MetricsManager metricsManager;
    private final GlobalConfig globalConfig;
    private final String catalogName;
    private final Cache<ScanFilteringKey, FilteringStats> filteringStatsCache;

    public DispatcherPageSourceProvider(
            ConnectorPageSourceProvider connectorPageSourceProvider,
            WarpDispatcherPageSourceFactory pageSourceFactory,
            StorageEngineTxService txService,
            MetricsManager metricsManager,
            GlobalConfig globalConfig,
            String catalogName,
            Cache<ScanFilteringKey, FilteringStats> filteringStatsCache)
    {
        this.connectorPageSourceProvider = requireNonNull(connectorPageSourceProvider);
        this.pageSourceFactory = requireNonNull(pageSourceFactory);
        this.txService = requireNonNull(txService);
        this.metricsManager = requireNonNull(metricsManager);
        this.globalConfig = requireNonNull(globalConfig);
        this.catalogName = requireNonNull(catalogName);
        this.filteringStatsCache = requireNonNull(filteringStatsCache);
    }

    @Override
    public Optional<ConnectorGpuPageSource> createGpuPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            ConnectorGpuMemoryContext memoryContext,
            IoExecutor ioExecutor)
    {
        if (split instanceof DispatcherSplit dispatcherSplit && table instanceof DispatcherTableHandle dispatcherTableHandle) {
            // Warp's index and warmup are not GPU-aware, so the proxied connector serves the scan
            return connectorPageSourceProvider.createGpuPageSource(
                    transactionHandle,
                    session,
                    dispatcherSplit.proxyConnectorSplit(),
                    dispatcherTableHandle.getProxyConnectorTableHandle(),
                    tableCredentials,
                    columns,
                    dynamicFilter,
                    memoryContext,
                    ioExecutor);
        }
        return connectorPageSourceProvider.createGpuPageSource(
                transactionHandle,
                session,
                split,
                table,
                tableCredentials,
                columns,
                dynamicFilter,
                memoryContext,
                ioExecutor);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        try (WarpMDCContext _ = new WarpMDCContext(catalogName, Optional.of(session.getQueryId()))) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "createPageSource: handle=%s, split=%s, table=%s, columns=%s, dynamicFilter=%s",
                        transactionHandle,
                        split,
                        table,
                        columns,
                        dynamicFilter.getCurrentPredicate().toString());
            }

            if (!(split instanceof DispatcherSplit) && !(table instanceof DispatcherTableHandle)) {
                return connectorPageSourceProvider.createPageSource(
                        transactionHandle,
                        session,
                        split,
                        table,
                        tableCredentials,
                        columns,
                        dynamicFilter,
                        memoryContext);
            }

            DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) table;

            CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager);
            customStatsContext.getOrRegister(new DispatcherPageSourceStats());

            return new DispatcherWrapperPageSource(
                    connectorPageSourceProvider,
                    pageSourceFactory,
                    txService,
                    customStatsContext,
                    transactionHandle,
                    session,
                    (DispatcherSplit) split,
                    dispatcherTableHandle,
                    tableCredentials,
                    columns,
                    dynamicFilter,
                    memoryContext,
                    () -> getFilteringStats(session, dispatcherTableHandle, dynamicFilter.getCurrentPredicate()),
                    catalogName);
        }
    }

    /**
     * Returns the gate of this scan, or empty when the scan has nothing for the warp index to filter on.
     * Scans of the same shape share a gate, so a repeated query starts from the verdict of the last one.
     */
    private Optional<FilteringStats> getFilteringStats(
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            TupleDomain<ColumnHandle> dynamicFilterPredicate)
    {
        if (dispatcherTableHandle.getFullPredicate().isAll() &&
                dynamicFilterPredicate.isAll() &&
                dispatcherTableHandle.getWarpExpression().map(warpExpression -> warpExpression.warpExpressionDataLeaves().isEmpty()).orElse(true)) {
            return Optional.empty();
        }
        double lowSelectivityThreshold = WarpSessionProperties.getFilteringLowSelectivityThreshold(session, globalConfig);
        double chunkSkipThreshold = WarpSessionProperties.getFilteringChunkSkipThreshold(session, globalConfig);
        Set<ColumnHandle> dynamicFilterColumns = dynamicFilterPredicate.getDomains()
                .map(Map::keySet)
                .map(ImmutableSet::copyOf)
                .orElseGet(ImmutableSet::of);
        ScanFilteringKey scanFilteringKey = new ScanFilteringKey(
                dispatcherTableHandle.getSchemaTableName(),
                dispatcherTableHandle.getFullPredicate(),
                dispatcherTableHandle.getWarpExpression(),
                dynamicFilterColumns,
                lowSelectivityThreshold,
                chunkSkipThreshold);
        return Optional.of(filteringStatsCache.asMap().computeIfAbsent(
                scanFilteringKey,
                _ -> new FilteringStats(lowSelectivityThreshold, chunkSkipThreshold)));
    }

    /**
     * Identifies the scans that filter on the same thing. The dynamic filter is taken by its columns
     * rather than its values, which vary between runs of the same query.
     */
    public record ScanFilteringKey(
            SchemaTableName schemaTableName,
            TupleDomain<ColumnHandle> fullPredicate,
            Optional<WarpExpression> warpExpression,
            Set<ColumnHandle> dynamicFilterColumns,
            double lowSelectivityThreshold,
            double chunkSkipThreshold)
    {
        public ScanFilteringKey
        {
            requireNonNull(schemaTableName);
            requireNonNull(fullPredicate);
            requireNonNull(warpExpression);
            dynamicFilterColumns = ImmutableSet.copyOf(dynamicFilterColumns);
        }
    }

    @Override
    public TupleDomain<ColumnHandle> getUnenforcedPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) table;
        ConnectorSplit connectorSplit = ((DispatcherSplit) split).proxyConnectorSplit();
        ConnectorTableHandle connectorTableHandle = dispatcherTableHandle.getProxyConnectorTableHandle();
        TupleDomain<ColumnHandle> unenforcedPredicate = connectorPageSourceProvider.getUnenforcedPredicate(session, connectorSplit, connectorTableHandle, dynamicFilter);
        if (unenforcedPredicate.isNone()) {
            // split is fully filtered out
            return TupleDomain.none();
        }

        // Warp speed can apply very large predicates. At this point we also don't know who will
        // serve the split, warp or proxy. Therefore, for correctness we can assume both fullPredicate
        // and dynamic filter are not simplified. However, we can still prune columns which are ineffective
        // in filtering split data.
        return unenforcedPredicate.intersect(connectorPageSourceProvider.prunePredicate(
                session,
                connectorSplit,
                connectorTableHandle,
                dispatcherTableHandle.getFullPredicate().intersect(dynamicFilter)));
    }

    @Override
    public TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        return connectorPageSourceProvider.prunePredicate(
                session,
                ((DispatcherSplit) split).proxyConnectorSplit(),
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle(),
                predicate);
    }
}
