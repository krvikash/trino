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
        return getSplits(transactionHandle, session, table, dynamicFilter, false, constraint);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, boolean preferDeterministicSplits, Constraint constraint)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        if (table instanceof HiveTableHandle) {
            return hiveSplitManager.getSplits(transaction.getHiveHandle(), unwrap(HIVE, session), table, dynamicFilter, preferDeterministicSplits, constraint);
        }
        if (table instanceof IcebergTableHandle) {
            return icebergSplitManager.getSplits(transaction.getIcebergHandle(), unwrap(ICEBERG, session), table, dynamicFilter, preferDeterministicSplits, constraint);
        }
        if (table instanceof DeltaLakeTableHandle) {
            return deltaSplitManager.getSplits(transaction.getDeltaHandle(), unwrap(DELTA, session), table, dynamicFilter, preferDeterministicSplits, constraint);
        }
        if (table instanceof HudiTableHandle) {
            return hudiSplitManager.getSplits(transaction.getHudiHandle(), unwrap(HUDI, session), table, dynamicFilter, preferDeterministicSplits, constraint);
        }
        throw new VerifyException("Unhandled class: " + table.getClass().getName());
    }

    @Override
    public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        if (split instanceof HiveSplit) {
            return hiveSplitManager.getCacheSplitId(split);
        }
        if (split instanceof IcebergSplit) {
            return icebergSplitManager.getCacheSplitId(split);
        }
        if (split instanceof DeltaLakeSplit) {
            return deltaSplitManager.getCacheSplitId(split);
        }
        if (split instanceof HudiSplit) {
            return hudiSplitManager.getCacheSplitId(split);
        }
        throw new VerifyException("Unhandled class: " + split.getClass().getName());
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
        if (function instanceof TableChangesTableFunctionHandle) {
            return deltaSplitManager.getSplits(transactionHandle.getDeltaHandle(), sessionProperties.unwrap(DELTA, session), function);
        }
        if (function instanceof io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle) {
            return icebergSplitManager.getSplits(transactionHandle.getIcebergHandle(), sessionProperties.unwrap(ICEBERG, session), function);
        }
        throw new VerifyException("Unhandled class: " + function.getClass().getName());
    }
}
