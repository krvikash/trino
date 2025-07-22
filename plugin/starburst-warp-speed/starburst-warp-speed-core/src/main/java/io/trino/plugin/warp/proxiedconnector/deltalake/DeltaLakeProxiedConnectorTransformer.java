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
package io.trino.plugin.warp.proxiedconnector.deltalake;

import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.deltalake.CorruptedDeltaLakeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeSplit;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.PartitionKey;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static java.util.Objects.requireNonNull;

@Singleton
public class DeltaLakeProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    private final ProxiedConnectorConfig proxiedConnectorConfig;

    @Inject
    public DeltaLakeProxiedConnectorTransformer(ProxiedConnectorConfig proxiedConnectorConfig)
    {
        this.proxiedConnectorConfig = requireNonNull(proxiedConnectorConfig);
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.isManaged(),
                tableHandle.getLocation(),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                TupleDomain.all(),
                TupleDomain.all(),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                tableHandle.getReadVersion(),
                tableHandle.isTimeTravel(),
                tableHandle.getVendedCredentials());
    }

    @Override
    public boolean proxyHasPushedDownFilter(DispatcherTableHandle dispatcherTableHandle)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return !tableHandle.getNonPartitionConstraint().isAll();
    }

    @Override
    public RegularColumn getWarpRegularColumn(ColumnHandle columnHandle)
    {
        String name = ((DeltaLakeColumnHandle) columnHandle).qualifiedPhysicalName();
        return new RegularColumn(name);
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        DeltaLakeColumnHandle deltaLakeColumnHandle = (DeltaLakeColumnHandle) columnHandle;

        return deltaLakeColumnHandle.projectionInfo().isPresent() ? ((DeltaLakeColumnHandle) columnHandle).projectionInfo().orElseThrow().type() : ((DeltaLakeColumnHandle) columnHandle).baseType();
    }

    @Override
    public Optional<Object> getPartitionValue(ColumnHandle columnHandle, String partitionName, String partitionValue)
    {
        return Optional.ofNullable(deserializePartitionValue((DeltaLakeColumnHandle) columnHandle, Optional.of(partitionValue)));
    }

    @Override
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        if (proxiedConnectorConfig.getPassThroughDispatcherSet().contains(ProxiedConnectorConfig.DELTA_LAKE_CONNECTOR_NAME)) {
            return false;
        }

        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return tableHandle.getWriteType().isEmpty();
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        DeltaLakeSplit deltaLakeSplit = (DeltaLakeSplit) proxyConnectorSplit;

        List<HostAddress> hostAddresses = getHostAddressForSplit(
                getSplitKey(deltaLakeSplit.getPath(), deltaLakeSplit.getStart(), deltaLakeSplit.getLength()),
                connectorSplitNodeDistributor);

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (Map.Entry<String, Optional<String>> entry : deltaLakeSplit.getPartitionKeys().entrySet()) {
            entry.getValue().ifPresent(value -> partitionKeys.add(new PartitionKey(new RegularColumn(entry.getKey()), value)));
        }

        String deletedFileHash = deltaLakeSplit.getDeletionVector().isPresent() ?
                Hashing.sha256().hashString(deltaLakeSplit.getDeletionVector().get().toString(), StandardCharsets.UTF_8).toString() : "";

        return new DispatcherSplit(dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                deltaLakeSplit.getPath(),
                deltaLakeSplit.getStart(),
                deltaLakeSplit.getLength(),
                deltaLakeSplit.getFileModifiedTime(),
                hostAddresses,
                partitionKeys,
                deletedFileHash,
                proxyConnectorSplit);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        if (connectorTableHandle instanceof CorruptedDeltaLakeTableHandle corruptedDeltaLakeTableHandle) {
            return corruptedDeltaLakeTableHandle.schemaTableName();
        }
        return ((DeltaLakeTableHandle) connectorTableHandle).getSchemaTableName();
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.isManaged(),
                tableHandle.getLocation(),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                TupleDomain.all(),
                TupleDomain.all(),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                tableHandle.getReadVersion(),
                tableHandle.isTimeTravel(),
                tableHandle.getVendedCredentials());
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        DeltaLakeSplit originSplit = (DeltaLakeSplit) connectorSplit;
        return new DeltaLakeSplit(originSplit.getPath(),
                originSplit.getStart(),
                originSplit.getLength(),
                originSplit.getFileSize(),
                originSplit.getFileRowCount(),
                originSplit.getFileModifiedTime(),
                originSplit.getDeletionVector(),
                originSplit.getSplitWeight(),
                TupleDomain.all(),
                originSplit.getPartitionKeys());
    }

    @Override
    public Optional<Long> getRowCount(ConnectorSplit connectorSplit)
    {
        DeltaLakeSplit deltaLakeSplit = (DeltaLakeSplit) connectorSplit;
        return deltaLakeSplit.getFileRowCount();
    }
}
