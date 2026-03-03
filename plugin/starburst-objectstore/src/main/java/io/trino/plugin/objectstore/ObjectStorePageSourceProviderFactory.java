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
package io.trino.plugin.objectstore;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.system.files.FilesTableSplit;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.Objects.requireNonNull;

public class ObjectStorePageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private final ConnectorPageSourceProvider hivePageSourceProvider;
    private final ConnectorPageSourceProviderFactory icebergPageSourceProviderFactory;
    private final ConnectorPageSourceProvider deltaPageSourceProvider;
    private final ConnectorPageSourceProvider hudiPageSourceProvider;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public ObjectStorePageSourceProviderFactory(
            @ForHive ConnectorPageSourceProvider hivePageSourceProvider,
            @ForIceberg ConnectorPageSourceProviderFactory icebergPageSourceProviderFactory,
            @ForDelta ConnectorPageSourceProvider deltaPageSourceProvider,
            @ForHudi ConnectorPageSourceProvider hudiPageSourceProvider,
            ObjectStoreSessionProperties sessionProperties)
    {
        this.icebergPageSourceProviderFactory = requireNonNull(icebergPageSourceProviderFactory, "icebergPageSourceProviderFactory is null");
        this.hivePageSourceProvider = requireNonNull(hivePageSourceProvider, "hivePageSourceProvider is null");
        this.deltaPageSourceProvider = requireNonNull(deltaPageSourceProvider, "deltaPageSourceProvider is null");
        this.hudiPageSourceProvider = requireNonNull(hudiPageSourceProvider, "hudiPageSourceProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorPageSourceProvider createPageSourceProvider()
    {
        // createPageSourceProvider is called for each scan within a query
        // we hold on to ConnectorPageSourceProvider instance to allow IcebergPageSourceProvider to reuse equality deletes between splits of the same scan
        return new ConnectorPageSourceProvider()
        {
            private volatile PageSourceProvider delegate;

            @Override
            public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, Optional<ConnectorTableCredentials> tableCredentials, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
            {
                PageSourceProvider pageSourceProvider = getPageSourceProvider(split, table);
                return pageSourceProvider.pageSourceProvider()
                        .createPageSource(transaction, unwrap(pageSourceProvider.tableType(), session), split, table, tableCredentials, columns, dynamicFilter);
            }

            @Override
            public TupleDomain<ColumnHandle> getUnenforcedPredicate(
                    ConnectorSession session,
                    ConnectorSplit split,
                    ConnectorTableHandle table,
                    TupleDomain<ColumnHandle> dynamicFilter)
            {
                PageSourceProvider pageSourceProvider = getPageSourceProvider(split, table);
                return pageSourceProvider.pageSourceProvider()
                        .getUnenforcedPredicate(unwrap(pageSourceProvider.tableType(), session), split, table, dynamicFilter);
            }

            @Override
            public TupleDomain<ColumnHandle> prunePredicate(
                    ConnectorSession session,
                    ConnectorSplit split,
                    ConnectorTableHandle table,
                    TupleDomain<ColumnHandle> predicate)
            {
                PageSourceProvider pageSourceProvider = getPageSourceProvider(split, table);
                return pageSourceProvider.pageSourceProvider()
                        .prunePredicate(unwrap(pageSourceProvider.tableType(), session), split, table, predicate);
            }

            @Override
            public long getMemoryUsage()
            {
                PageSourceProvider provider = delegate;
                if (provider == null) {
                    // No page source was created, so no memory is used
                    return 0;
                }
                return provider.pageSourceProvider().getMemoryUsage();
            }

            private PageSourceProvider getPageSourceProvider(ConnectorSplit split, ConnectorTableHandle table)
            {
                PageSourceProvider result = delegate;
                if (result == null) {
                    synchronized (this) {
                        result = delegate;
                        if (result == null) {
                            result = forHandle(split, table);
                            delegate = result;
                        }
                    }
                }
                return result;
            }
        };
    }

    private PageSourceProvider forHandle(ConnectorSplit split, ConnectorTableHandle handle)
    {
        if (split instanceof FilesTableSplit) {
            return new PageSourceProvider(ICEBERG, icebergPageSourceProviderFactory.createPageSourceProvider());
        }

        return switch (handle) {
            case HiveTableHandle _ -> new PageSourceProvider(HIVE, hivePageSourceProvider);
            case IcebergTableHandle _ -> new PageSourceProvider(ICEBERG, icebergPageSourceProviderFactory.createPageSourceProvider());
            case DeltaLakeTableHandle _ -> new PageSourceProvider(DELTA, deltaPageSourceProvider);
            case HudiTableHandle _ -> new PageSourceProvider(HUDI, hudiPageSourceProvider);
            default -> throw new UnsupportedOperationException("Unsupported table handle " + handle.getClass() + " with split " + split.getClass());
        };
    }

    private ConnectorSession unwrap(TableType tableType, ConnectorSession session)
    {
        return sessionProperties.unwrap(tableType, session);
    }

    private record PageSourceProvider(TableType tableType, ConnectorPageSourceProvider pageSourceProvider) {}
}
