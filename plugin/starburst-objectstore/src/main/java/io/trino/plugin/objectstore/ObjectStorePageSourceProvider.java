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

import com.google.common.base.VerifyException;
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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.Objects.requireNonNull;

public class ObjectStorePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProviderFactory hivePageSourceProviderFactory;
    private final ConnectorPageSourceProviderFactory icebergPageSourceProviderFactory;
    private final ConnectorPageSourceProviderFactory deltaPageSourceProviderFactory;
    private final ConnectorPageSourceProviderFactory hudiPageSourceProviderFactory;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public ObjectStorePageSourceProvider(
            @ForHive ConnectorPageSourceProviderFactory hivePageSourceProviderFactory,
            @ForIceberg ConnectorPageSourceProviderFactory icebergPageSourceProviderFactory,
            @ForDelta ConnectorPageSourceProviderFactory deltaPageSourceProviderFactory,
            @ForHudi ConnectorPageSourceProviderFactory hudiPageSourceProviderFactory,
            ObjectStoreSessionProperties sessionProperties)
    {
        this.hivePageSourceProviderFactory = requireNonNull(hivePageSourceProviderFactory, "hivePageSourceProviderFactory is null");
        this.icebergPageSourceProviderFactory = requireNonNull(icebergPageSourceProviderFactory, "icebergPageSourceProviderFactory is null");
        this.deltaPageSourceProviderFactory = requireNonNull(deltaPageSourceProviderFactory, "deltaPageSourceProviderFactory is null");
        this.hudiPageSourceProviderFactory = requireNonNull(hudiPageSourceProviderFactory, "hudiPageSourceProviderFactory is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        if (table instanceof HiveTableHandle) {
            return hivePageSourceProviderFactory.createPageSourceProvider().createPageSource(transaction.getHiveHandle(), unwrap(HIVE, session), split, table, columns, dynamicFilter);
        }
        if (table instanceof IcebergTableHandle || split instanceof FilesTableSplit) {
            return icebergPageSourceProviderFactory.createPageSourceProvider().createPageSource(transaction.getIcebergHandle(), unwrap(ICEBERG, session), split, table, columns, dynamicFilter);
        }
        if (table instanceof DeltaLakeTableHandle) {
            return deltaPageSourceProviderFactory.createPageSourceProvider().createPageSource(transaction.getDeltaHandle(), unwrap(DELTA, session), split, table, columns, dynamicFilter);
        }
        if (table instanceof HudiTableHandle) {
            return hudiPageSourceProviderFactory.createPageSourceProvider().createPageSource(transaction.getHudiHandle(), unwrap(HUDI, session), split, table, columns, dynamicFilter);
        }
        throw new VerifyException("Unhandled class: " + table.getClass().getName());
    }

    @Override
    public TupleDomain<ColumnHandle> getUnenforcedPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        if (table instanceof HiveTableHandle) {
            return hivePageSourceProviderFactory.createPageSourceProvider().getUnenforcedPredicate(unwrap(HIVE, session), split, table, dynamicFilter);
        }
        if (table instanceof IcebergTableHandle) {
            return icebergPageSourceProviderFactory.createPageSourceProvider().getUnenforcedPredicate(unwrap(ICEBERG, session), split, table, dynamicFilter);
        }
        if (table instanceof DeltaLakeTableHandle) {
            return deltaPageSourceProviderFactory.createPageSourceProvider().getUnenforcedPredicate(unwrap(DELTA, session), split, table, dynamicFilter);
        }
        if (table instanceof HudiTableHandle) {
            return hudiPageSourceProviderFactory.createPageSourceProvider().getUnenforcedPredicate(unwrap(HUDI, session), split, table, dynamicFilter);
        }
        throw new VerifyException("Unhandled class: " + table.getClass().getName());
    }

    @Override
    public TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        if (table instanceof HiveTableHandle) {
            return hivePageSourceProviderFactory.createPageSourceProvider().prunePredicate(unwrap(HIVE, session), split, table, predicate);
        }
        if (table instanceof IcebergTableHandle) {
            return icebergPageSourceProviderFactory.createPageSourceProvider().prunePredicate(unwrap(ICEBERG, session), split, table, predicate);
        }
        if (table instanceof DeltaLakeTableHandle) {
            return deltaPageSourceProviderFactory.createPageSourceProvider().prunePredicate(unwrap(DELTA, session), split, table, predicate);
        }
        if (table instanceof HudiTableHandle) {
            return hudiPageSourceProviderFactory.createPageSourceProvider().prunePredicate(unwrap(HUDI, session), split, table, predicate);
        }
        throw new VerifyException("Unhandled class: " + table.getClass().getName());
    }

    private ConnectorSession unwrap(TableType tableType, ConnectorSession session)
    {
        return sessionProperties.unwrap(tableType, session);
    }
}
