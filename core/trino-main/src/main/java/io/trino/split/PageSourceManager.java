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
package io.trino.split;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.gpu.AttributingIoExecutor;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.memory.DefaultConnectorGpuMemoryContext;
import io.trino.operator.gpu.scan.ConnectorGpuPageSourceAdapter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.EmptyGpuPageSource;
import io.trino.spi.gpu.IoExecutor;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static java.util.Objects.requireNonNull;

public class PageSourceManager
        implements PageSourceProviderFactory
{
    private static final Logger log = Logger.get(PageSourceManager.class);

    private final CatalogServiceProvider<ConnectorPageSourceProviderFactory> pageSourceProviderFactory;
    private final IoExecutor ioExecutor;

    @Inject
    public PageSourceManager(CatalogServiceProvider<ConnectorPageSourceProviderFactory> pageSourceProviderFactory, IoExecutor ioExecutor)
    {
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.ioExecutor = requireNonNull(ioExecutor, "ioExecutor is null");
    }

    public boolean supportsConnectorGpuPageSource(CatalogHandle catalogHandle, ConnectorTableHandle connectorTableHandle, List<ColumnHandle> columns)
    {
        ConnectorPageSourceProviderFactory provider = pageSourceProviderFactory.getService(catalogHandle);
        return provider.supportsConnectorGpuPageSource(connectorTableHandle, columns);
    }

    @Override
    public PageSourceProvider createPageSourceProvider(CatalogHandle catalogHandle)
    {
        ConnectorPageSourceProviderFactory provider = pageSourceProviderFactory.getService(catalogHandle);
        return new PageSourceProviderInstance(provider.createPageSourceProvider(), ioExecutor);
    }

    @VisibleForTesting
    public record PageSourceProviderInstance(ConnectorPageSourceProvider pageSourceProvider, IoExecutor ioExecutor)
            implements PageSourceProvider
    {
        public PageSourceProviderInstance
        {
            requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            requireNonNull(ioExecutor, "ioExecutor is null");
        }

        @Override
        public ConnectorGpuPageSource createGpuPageSource(
                GpuOperation.Context gpuOperationContext,
                Session session,
                Split split,
                TableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                List<Type> columnTypes,
                DynamicFilter dynamicFilter)
        {
            checkArgument(columns.size() == columnTypes.size(), "columns and columnTypes have different sizes");
            checkArgument(split.getCatalogHandle().equals(table.catalogHandle()), "mismatched split and table");

            TupleDomain<ColumnHandle> constraint = dynamicFilter.getCurrentPredicate();
            if (constraint.isNone()) {
                return new EmptyGpuPageSource();
            }
            if (!isAllowPushdownIntoConnectors(session)) {
                dynamicFilter = DynamicFilter.EMPTY;
            }
            DynamicFilter finalDynamicFilter = dynamicFilter;
            ConnectorSession connectorSession = session.toConnectorSession(table.catalogHandle());
            // Attribute reads offloaded to the shared executor to the operator issuing them.
            IoExecutor attributingIoExecutor = new AttributingIoExecutor(ioExecutor, gpuOperationContext.operatorContext());
            return pageSourceProvider.createGpuPageSource(
                            table.transaction(),
                            connectorSession,
                            split.getConnectorSplit(),
                            table.connectorHandle(),
                            tableCredentials,
                            columns,
                            finalDynamicFilter,
                            new DefaultConnectorGpuMemoryContext(gpuOperationContext.taskMemoryContext(), "ConnectorGpuPageSource"),
                            attributingIoExecutor)
                    .orElseGet(() -> {
                        log.debug("GPU page source was requested but not provided, falling back to CPU scan with adaptation for %s", table.connectorHandle());
                        return new ConnectorGpuPageSourceAdapter(
                                gpuOperationContext,
                                pageSourceProvider.createPageSource(
                                        table.transaction(),
                                        connectorSession,
                                        split.getConnectorSplit(),
                                        table.connectorHandle(),
                                        tableCredentials,
                                        columns,
                                        finalDynamicFilter),
                                columnTypes);
                    });
        }

        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            requireNonNull(columns, "columns is null");
            checkArgument(split.getCatalogHandle().equals(table.catalogHandle()), "mismatched split and table");

            TupleDomain<ColumnHandle> constraint = dynamicFilter.getCurrentPredicate();
            if (constraint.isNone()) {
                return new EmptyPageSource();
            }
            if (!isAllowPushdownIntoConnectors(session)) {
                dynamicFilter = DynamicFilter.EMPTY;
            }
            return pageSourceProvider.createPageSource(
                    table.transaction(),
                    session.toConnectorSession(table.catalogHandle()),
                    split.getConnectorSplit(),
                    table.connectorHandle(),
                    tableCredentials,
                    columns,
                    dynamicFilter);
        }

        @Override
        public TupleDomain<ColumnHandle> getUnenforcedPredicate(
                Session session,
                Split split,
                TableHandle table,
                TupleDomain<ColumnHandle> dynamicFilter)
        {
            CatalogHandle catalogHandle = split.getCatalogHandle();
            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return pageSourceProvider.getUnenforcedPredicate(connectorSession, split.getConnectorSplit(), table.connectorHandle(), dynamicFilter);
        }

        @Override
        public TupleDomain<ColumnHandle> prunePredicate(
                Session session,
                Split split,
                TableHandle table,
                TupleDomain<ColumnHandle> predicate)
        {
            CatalogHandle catalogHandle = split.getCatalogHandle();
            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return pageSourceProvider.prunePredicate(connectorSession, split.getConnectorSplit(), table.connectorHandle(), predicate);
        }

        @Override
        public long getMemoryUsage()
        {
            return pageSourceProvider.getMemoryUsage();
        }
    }
}
