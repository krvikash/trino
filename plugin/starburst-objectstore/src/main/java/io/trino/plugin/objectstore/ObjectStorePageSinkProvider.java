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
import io.trino.plugin.deltalake.DeltaLakeInsertTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMergeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeOutputTableHandle;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HiveTableExecuteHandle;
import io.trino.plugin.iceberg.IcebergMergeTableHandle;
import io.trino.plugin.iceberg.IcebergWritableTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableCredentials;

import java.util.Optional;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.Objects.requireNonNull;

public class ObjectStorePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final ConnectorPageSinkProvider hivePageSinkProvider;
    private final ConnectorPageSinkProvider icebergPageSinkProvider;
    private final ConnectorPageSinkProvider deltaPageSinkProvider;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public ObjectStorePageSinkProvider(
            @ForHive ConnectorPageSinkProvider hivePageSinkProvider,
            @ForIceberg ConnectorPageSinkProvider icebergPageSinkProvider,
            @ForDelta ConnectorPageSinkProvider deltaPageSinkProvider,
            ObjectStoreSessionProperties sessionProperties)
    {
        this.hivePageSinkProvider = requireNonNull(hivePageSinkProvider, "hivePageSinkProvider is null");
        this.icebergPageSinkProvider = requireNonNull(icebergPageSinkProvider, "icebergPageSinkProvider is null");
        this.deltaPageSinkProvider = requireNonNull(deltaPageSinkProvider, "deltaPageSinkProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle table, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        return switch (table) {
            case HiveOutputTableHandle _ -> hivePageSinkProvider.createPageSink(transaction.getHiveHandle(), unwrap(HIVE, session), table, tableCredentials, pageSinkId);
            case IcebergWritableTableHandle _ -> icebergPageSinkProvider.createPageSink(transaction.getIcebergHandle(), unwrap(ICEBERG, session), table, tableCredentials, pageSinkId);
            case DeltaLakeOutputTableHandle _ -> deltaPageSinkProvider.createPageSink(transaction.getDeltaHandle(), unwrap(DELTA, session), table, tableCredentials, pageSinkId);
            default -> throw new VerifyException("Unhandled class: " + table.getClass().getName());
        };
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle table, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        return switch (table) {
            case HiveInsertTableHandle _ -> hivePageSinkProvider.createPageSink(transaction.getHiveHandle(), unwrap(HIVE, session), table, tableCredentials, pageSinkId);
            case IcebergWritableTableHandle _ -> icebergPageSinkProvider.createPageSink(transaction.getIcebergHandle(), unwrap(ICEBERG, session), table, tableCredentials, pageSinkId);
            case DeltaLakeInsertTableHandle _ -> deltaPageSinkProvider.createPageSink(transaction.getDeltaHandle(), unwrap(DELTA, session), table, tableCredentials, pageSinkId);
            default -> throw new VerifyException("Unhandled class: " + table.getClass().getName());
        };
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle table, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        return switch (table) {
            case HiveTableExecuteHandle _ -> hivePageSinkProvider.createPageSink(transaction.getHiveHandle(), unwrap(HIVE, session), table, tableCredentials, pageSinkId);
            case IcebergTableExecuteHandle _ -> icebergPageSinkProvider.createPageSink(transaction.getIcebergHandle(), unwrap(ICEBERG, session), table, tableCredentials, pageSinkId);
            case DeltaLakeTableExecuteHandle _ -> deltaPageSinkProvider.createPageSink(transaction.getDeltaHandle(), unwrap(DELTA, session), table, tableCredentials, pageSinkId);
            default -> throw new VerifyException("Unhandled class: " + table.getClass().getName());
        };
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle table, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        return switch (table) {
            case IcebergMergeTableHandle _ -> icebergPageSinkProvider.createMergeSink(transaction.getIcebergHandle(), unwrap(ICEBERG, session), table, tableCredentials, pageSinkId);
            case DeltaLakeMergeTableHandle _ -> deltaPageSinkProvider.createMergeSink(transaction.getDeltaHandle(), unwrap(DELTA, session), table, tableCredentials, pageSinkId);
            default -> throw new VerifyException("Unhandled class: " + table.getClass().getName());
        };
    }

    private ConnectorSession unwrap(TableType tableType, ConnectorSession session)
    {
        return sessionProperties.unwrap(tableType, session);
    }
}
