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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherPageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private final ConnectorPageSourceProviderFactory connectorPageSourceProviderFactory;
    private final WarpDispatcherPageSourceFactory pageSourceFactory;
    private final StorageEngineTxService txService;
    private final MetricsManager metricsManager;
    private final String catalogName;

    @Inject
    public DispatcherPageSourceProviderFactory(
            @ForWarp ConnectorPageSourceProviderFactory connectorPageSourceProviderFactory,
            WarpDispatcherPageSourceFactory pageSourceFactory,
            StorageEngineTxService txService,
            MetricsManager metricsManager,
            CatalogName catalogName)
    {
        this.connectorPageSourceProviderFactory = requireNonNull(connectorPageSourceProviderFactory);
        this.pageSourceFactory = requireNonNull(pageSourceFactory);
        this.txService = requireNonNull(txService);
        this.metricsManager = requireNonNull(metricsManager);
        this.catalogName = requireNonNull(catalogName).toString();
    }

    @Override
    public ConnectorPageSourceProvider createPageSourceProvider()
    {
        // one proxied provider per scan so cross-split memoization in the proxied connector spans the scan
        ConnectorPageSourceProvider proxiedPageSourceProvider = connectorPageSourceProviderFactory.createPageSourceProvider();
        return new DispatcherPageSourceProvider(
                proxiedPageSourceProvider,
                pageSourceFactory,
                txService,
                metricsManager,
                catalogName);
    }
}
