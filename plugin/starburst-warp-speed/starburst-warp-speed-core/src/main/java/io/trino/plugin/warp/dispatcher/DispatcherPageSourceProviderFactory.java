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
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceProvider.ScanFilteringKey;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.MemoryContext;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherPageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private final ConnectorPageSourceProviderFactory connectorPageSourceProviderFactory;
    private final WarpDispatcherPageSourceFactory pageSourceFactory;
    private final StorageEngineTxService txService;
    private final MetricsManager metricsManager;
    private final GlobalConfig globalConfig;
    private final String catalogName;
    private final Cache<ScanFilteringKey, FilteringStats> filteringStatsCache = buildFilteringStatsCache();

    @Inject
    public DispatcherPageSourceProviderFactory(
            @ForWarp ConnectorPageSourceProviderFactory connectorPageSourceProviderFactory,
            WarpDispatcherPageSourceFactory pageSourceFactory,
            StorageEngineTxService txService,
            MetricsManager metricsManager,
            GlobalConfig globalConfig,
            CatalogName catalogName)
    {
        this.connectorPageSourceProviderFactory = requireNonNull(connectorPageSourceProviderFactory);
        this.pageSourceFactory = requireNonNull(pageSourceFactory);
        this.txService = requireNonNull(txService);
        this.metricsManager = requireNonNull(metricsManager);
        this.globalConfig = requireNonNull(globalConfig);
        this.catalogName = requireNonNull(catalogName).toString();
    }

    @Override
    public ConnectorPageSourceProvider createPageSourceProvider(MemoryContext memoryContext)
    {
        // one proxied provider per scan so cross-split memoization in the proxied connector spans the scan
        ConnectorPageSourceProvider proxiedPageSourceProvider = connectorPageSourceProviderFactory.createPageSourceProvider(memoryContext);
        return new DispatcherPageSourceProvider(
                proxiedPageSourceProvider,
                pageSourceFactory,
                txService,
                metricsManager,
                globalConfig,
                catalogName,
                filteringStatsCache);
    }

    @SuppressModernizer // entries are never invalidated, so the eviction races that EvictableCacheBuilder guards against cannot happen
    private static Cache<ScanFilteringKey, FilteringStats> buildFilteringStatsCache()
    {
        return CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(Duration.ofMinutes(30))
                .build();
    }
}
