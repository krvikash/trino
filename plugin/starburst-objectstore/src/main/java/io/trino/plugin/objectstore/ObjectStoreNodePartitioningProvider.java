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
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeUpdateHandle;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.Objects.requireNonNull;

public class ObjectStoreNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final ConnectorNodePartitioningProvider hiveNodePartitioningProvider;
    private final ConnectorNodePartitioningProvider icebergNodePartitioningProvider;
    private final ConnectorNodePartitioningProvider deltaNodePartitioningProvider;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public ObjectStoreNodePartitioningProvider(
            @ForHive ConnectorNodePartitioningProvider hiveNodePartitioningProvider,
            @ForIceberg ConnectorNodePartitioningProvider icebergNodePartitioningProvider,
            @ForDelta ConnectorNodePartitioningProvider deltaNodePartitioningProvider,
            ObjectStoreSessionProperties sessionProperties)
    {
        this.hiveNodePartitioningProvider = requireNonNull(hiveNodePartitioningProvider, "hiveNodePartitioningProvider is null");
        this.icebergNodePartitioningProvider = requireNonNull(icebergNodePartitioningProvider, "icebergNodePartitioningProvider is null");
        this.deltaNodePartitioningProvider = requireNonNull(deltaNodePartitioningProvider, "deltaNodePartitioningProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        TableType tableType = tableType(partitioningHandle);
        return delegate(tableType, transactionHandle, (partitioningProvider, transaction) ->
                partitioningProvider.getBucketNodeMapping(transactionHandle, sessionProperties.unwrap(tableType, session), partitioningHandle));
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, int bucketCount)
    {
        TableType tableType = tableType(partitioningHandle);
        return delegate(tableType, transactionHandle, (partitioningProvider, transaction) ->
                partitioningProvider.getSplitBucketFunction(transactionHandle, sessionProperties.unwrap(tableType, session), partitioningHandle, bucketCount));
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        TableType tableType = tableType(partitioningHandle);
        return delegate(tableType, transactionHandle, (partitioningProvider, transaction) ->
                partitioningProvider.getBucketFunction(transactionHandle, sessionProperties.unwrap(tableType, session), partitioningHandle, partitionChannelTypes, bucketCount));
    }

    private <T> T delegate(TableType tableType, ConnectorTransactionHandle transactionHandle, BiFunction<ConnectorNodePartitioningProvider, ConnectorTransactionHandle, T> consumer)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        return switch (tableType) {
            case HIVE -> consumer.apply(hiveNodePartitioningProvider, transaction.getHiveHandle());
            case ICEBERG -> consumer.apply(icebergNodePartitioningProvider, transaction.getIcebergHandle());
            case DELTA -> consumer.apply(deltaNodePartitioningProvider, transaction.getDeltaHandle());
            case HUDI -> throw new UnsupportedOperationException();
        };
    }

    private TableType tableType(ConnectorPartitioningHandle partitioningHandle)
    {
        if (partitioningHandle instanceof HivePartitioningHandle) {
            return HIVE;
        }
        if ((partitioningHandle instanceof IcebergPartitioningHandle)) {
            return ICEBERG;
        }
        if ((partitioningHandle instanceof DeltaLakePartitioningHandle) || (partitioningHandle instanceof DeltaLakeUpdateHandle)) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + partitioningHandle.getClass().getName());
    }
}
