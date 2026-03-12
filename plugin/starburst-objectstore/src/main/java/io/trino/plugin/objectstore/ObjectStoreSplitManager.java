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
import io.trino.plugin.deltalake.DeltaLakeSplit;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesTableFunctionHandle;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.Objects.requireNonNull;

public class ObjectStoreSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager hiveSplitManager;
    private final ConnectorSplitManager icebergSplitManager;
    private final ConnectorSplitManager deltaSplitManager;
    private final ConnectorSplitManager hudiSplitManager;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public ObjectStoreSplitManager(
            @ForHive ConnectorSplitManager hiveSplitManager,
            @ForIceberg ConnectorSplitManager icebergSplitManager,
            @ForDelta ConnectorSplitManager deltaSplitManager,
            @ForHudi ConnectorSplitManager hudiSplitManager,
            ObjectStoreSessionProperties sessionProperties)
    {
        this.hiveSplitManager = requireNonNull(hiveSplitManager, "hiveSplitManager is null");
        this.icebergSplitManager = requireNonNull(icebergSplitManager, "icebergSplitManager is null");
        this.deltaSplitManager = requireNonNull(deltaSplitManager, "deltaSplitManager is null");
        this.hudiSplitManager = requireNonNull(hudiSplitManager, "hudiSplitManager is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        return switch (table) {
            case HiveTableHandle _ -> hiveSplitManager.getSplits(transaction.getHiveHandle(), unwrap(HIVE, session), table, dynamicFilter, constraint);
            case IcebergTableHandle _ -> icebergSplitManager.getSplits(transaction.getIcebergHandle(), unwrap(ICEBERG, session), table, dynamicFilter, constraint);
            case DeltaLakeTableHandle _ -> deltaSplitManager.getSplits(transaction.getDeltaHandle(), unwrap(DELTA, session), table, dynamicFilter, constraint);
            case HudiTableHandle _ -> hudiSplitManager.getSplits(transaction.getHudiHandle(), unwrap(HUDI, session), table, dynamicFilter, constraint);
            default -> throw new VerifyException("Unhandled class: " + table.getClass().getName());
        };
    }

    @Override
    public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        return switch (split) {
            case HiveSplit _ -> hiveSplitManager.getCacheSplitId(split);
            case IcebergSplit _ -> icebergSplitManager.getCacheSplitId(split);
            case DeltaLakeSplit _ -> deltaSplitManager.getCacheSplitId(split);
            case HudiSplit _ -> hudiSplitManager.getCacheSplitId(split);
            default -> throw new VerifyException("Unhandled class: " + split.getClass().getName());
        };
    }

    private ConnectorSession unwrap(TableType tableType, ConnectorSession session)
    {
        return sessionProperties.unwrap(tableType, session);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        ObjectStoreTransactionHandle transactionHandle = (ObjectStoreTransactionHandle) transaction;
        return switch (function) {
            case TableChangesTableFunctionHandle _ -> deltaSplitManager.getSplits(transactionHandle.getDeltaHandle(), sessionProperties.unwrap(DELTA, session), function);
            case io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle _ -> icebergSplitManager.getSplits(transactionHandle.getIcebergHandle(), sessionProperties.unwrap(ICEBERG, session), function);
            default -> throw new VerifyException("Unhandled class: " + function.getClass().getName());
        };
    }
}
