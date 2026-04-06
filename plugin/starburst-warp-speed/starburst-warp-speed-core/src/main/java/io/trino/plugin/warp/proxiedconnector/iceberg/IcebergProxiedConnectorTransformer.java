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
package io.trino.plugin.warp.proxiedconnector.iceberg;

import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.iceberg.CorruptedIcebergTableHandle;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.PartitionKey;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static java.util.Objects.requireNonNull;

@Singleton
public class IcebergProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    private final ProxiedConnectorConfig proxiedConnectorConfig;
    private final TypeManager typeManager;

    @Inject
    public IcebergProxiedConnectorTransformer(ProxiedConnectorConfig proxiedConnectorConfig, TypeManager typeManager)
    {
        this.proxiedConnectorConfig = requireNonNull(proxiedConnectorConfig);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public boolean isValidForTableStatistics(ConnectorTableHandle connectorTableHandle)
    {
        if (connectorTableHandle instanceof IcebergTableHandle icebergTableHandle) {
            return (!icebergTableHandle.isRecordScannedFiles() && icebergTableHandle.getMaxScannedFileSize().isEmpty());
        }
        return false;
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new IcebergTableHandle(
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getSpecId(),
                icebergTableHandle.getPartitionSpecJsons(),
                icebergTableHandle.getFormatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                icebergTableHandle.getLimit(),
                false,
                OptionalInt.empty(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                Optional.empty(),
                icebergTableHandle.getBranch(),
                icebergTableHandle.isRecordScannedFiles(),
                Optional.empty(),
                icebergTableHandle.isForceReadingAllFiles(),
                Collections.emptySet(),
                icebergTableHandle.getForAnalyze()); // don't limit
    }

    @Override
    public boolean proxyHasPushedDownFilter(DispatcherTableHandle dispatcherTableHandle)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return !tableHandle.getUnenforcedPredicate().isAll();
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new IcebergTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getTableType(),
                tableHandle.getSnapshotId(),
                tableHandle.getTableSchemaJson(),
                tableHandle.getSpecId(),
                tableHandle.getPartitionSpecJsons(),
                tableHandle.getFormatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                tableHandle.getLimit(),
                tableHandle.preferSmallInitialReads(),
                tableHandle.getSortOrderId(),
                tableHandle.getProjectedColumns(),
                tableHandle.getNameMappingJson(),
                tableHandle.getTableLocation(),
                tableHandle.getStorageProperties(),
                Optional.empty(),
                tableHandle.getBranch(),
                tableHandle.isRecordScannedFiles(),
                Optional.empty(),
                tableHandle.isForceReadingAllFiles(),
                tableHandle.getConstraintColumns(),
                tableHandle.getForAnalyze());  // must be empty to allow mixed query (see isValidForAcceleration())
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        IcebergSplit original = (IcebergSplit) connectorSplit;
        return new IcebergSplit(original.getPath(),
                original.getStart(),
                original.getLength(),
                original.getFileSize(),
                original.getFileRecordCount(),
                original.getFileFormat(),
                original.getSpecId(),
                original.getSortOrderId(),
                original.getPartitionValues(),
                original.getDeletes(),
                original.getSplitWeight(),
                TupleDomain.all(),
                original.getDataSequenceNumber(),
                original.getFileFirstRowId());
    }

    @Override
    public RegularColumn getWarpRegularColumn(ColumnHandle columnHandle)
    {
        IcebergColumnHandle icebergColumnHandle = (IcebergColumnHandle) columnHandle;
        return new RegularColumn(icebergColumnHandle.getName(), String.valueOf(icebergColumnHandle.getId()));
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return ((IcebergColumnHandle) columnHandle).getType();
    }

    @Override
    public Optional<Object> getPartitionValue(ColumnHandle columnHandle, String partitionName, String partitionValue)
    {
        return Optional.of(deserializePartitionValue(getColumnType(columnHandle), partitionValue, partitionName));
    }

    private List<PartitionKey> getPartitionKeysMap(IcebergSplit icebergSplit, IcebergTableHandle icebergTableHandle)
    {
        Schema tableSchema = SchemaParser.fromJson(icebergTableHandle.getTableSchemaJson());
        String partitionSpecJson = icebergTableHandle.getPartitionSpecJsons().get(icebergSplit.getSpecId());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(tableSchema, partitionSpecJson);
        Map<Integer, Pair<String, org.apache.iceberg.types.Type>> columnIdToName = partitionSpec.fields().stream()
                .filter(partitionField -> partitionField.transform().isIdentity())
                .collect(Collectors.toMap(PartitionField::sourceId, (partitionField) -> Pair.of(partitionField.name(), partitionField.transform().getResultType(tableSchema.findType(partitionField.sourceId())))));
        org.apache.iceberg.types.Type[] partitionColumnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(tableSchema.findType(field.sourceId())))
                .toArray(org.apache.iceberg.types.Type[]::new);
        PartitionData partitionData = PartitionData.fromBlocks(icebergSplit.getPartitionValues(), partitionColumnTypes, typeManager);
        Map<Integer, Optional<String>> partitionKeys = IcebergUtil.getPartitionKeys(partitionData, partitionSpec);

        List<PartitionKey> result = new ArrayList<>();
        for (Map.Entry<Integer, Optional<String>> entry : partitionKeys.entrySet()) {
            entry.getValue().ifPresent(value -> {
                Pair<String, org.apache.iceberg.types.Type> nameAndType = columnIdToName.get(entry.getKey());
                String columnName = nameAndType.getKey();
                result.add(new PartitionKey(new RegularColumn(columnName, String.valueOf(entry.getKey())), value));
            });
        }
        return result;
    }

    @Override
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        if (proxiedConnectorConfig.getPassThroughDispatcherSet().contains(ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME)) {
            return false;
        }

        IcebergTableHandle tableHandle = (IcebergTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();

        // This limitation should only be present upon EXECUTE OPTIMIZE queries that are not relevant for acceleration anyway
        // (see file_size_threshold at https://trino.io/docs/current/connector/iceberg.html#optimize)
        // but if we got it, we must go to proxy because we don't apply this limitation on the data in Wrap Speed
        return tableHandle.getMaxScannedFileSize().isEmpty();
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        IcebergSplit icebergSplit = (IcebergSplit) proxyConnectorSplit;

        List<HostAddress> hostAddresses = getHostAddressForSplit(
                getSplitKey(icebergSplit.getPath(), icebergSplit.getStart(), icebergSplit.getLength()),
                connectorSplitNodeDistributor);

        List<PartitionKey> partitionKeyMap = getPartitionKeysMap(icebergSplit,
                (IcebergTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle());

        String snapshotIdStr = proxiedConnectorConfig.getEnableIcebergSnapshotIdUniqueness() ? String.valueOf(((IcebergTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle()).getSnapshotId().orElse(-1L)) : "";
        String deletedFilesHash = Hashing.sha256()
                .hashString(icebergSplit.getDeletes().stream().map(DeleteFile::path).sorted().collect(Collectors.joining()), StandardCharsets.UTF_8).toString() + snapshotIdStr;

        return new DispatcherSplit(dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                icebergSplit.getPath(),
                icebergSplit.getStart(),
                icebergSplit.getLength(),
                Objects.hash(icebergSplit.getFileSize(), icebergSplit.getFileRecordCount()), // iceberg split doesn't have modification time. using these 2 parameters as additional uniqueness parameters
                hostAddresses,
                partitionKeyMap,
                deletedFilesHash,
                proxyConnectorSplit);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        if (connectorTableHandle instanceof CorruptedIcebergTableHandle corruptedIcebergTableHandle) {
            return corruptedIcebergTableHandle.schemaTableName();
        }
        return ((IcebergTableHandle) connectorTableHandle).getSchemaTableName();
    }

    @Override
    public Optional<Long> getRowCount(ConnectorSplit connectorSplit)
    {
        IcebergSplit icebergSplit = (IcebergSplit) connectorSplit;
        return Optional.of(icebergSplit.getFileRecordCount());
    }

    @Override
    public long getDeletedRowsCount(ConnectorSplit connectorSplit)
    {
        IcebergSplit icebergSplit = (IcebergSplit) connectorSplit;
        return icebergSplit.getDeletes().stream().mapToLong(DeleteFile::recordCount).sum();
    }
}
