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

import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.passthrough.DispatcherProxiedConnectorColumnTransformer;
import io.trino.plugin.warp.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.util.DomainUtils;
import io.trino.plugin.warp.util.SimplifyResult;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static java.lang.String.format;

public interface DispatcherProxiedConnectorTransformer
        extends DispatcherProxiedConnectorColumnTransformer
{
    default boolean isValidForTableStatistics(ConnectorTableHandle connectorTableHandle)
    {
        return true;
    }

    default List<HostAddress> getHostAddressForSplit(
            String splitKey,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor)
    {
        connectorSplitNodeDistributor.updateNodeBucketsIfNeeded();
        Node node = connectorSplitNodeDistributor.getNode(splitKey);
        if (Objects.isNull(node)) {
            throw new TrinoException(WarpErrorCode.WARP_CLUSTER_NOT_READY, "no worker nodes available");
        }
        return Collections.singletonList(node.getHostAndPort());
    }

    default String getSplitKey(String path, long start, long length)
    {
        return format("%s-%d-%d", path, start, length);
    }

    default Set<String> calculateColumnsNotFitForDictionary(Map<ColumnHandle, ColumnStatistics> columnStatistics, int dictionaryMaxSize)
    {
        return columnStatistics.entrySet()
                .stream()
                .filter(entry -> entry.getValue().getDistinctValuesCount().getValue() >= dictionaryMaxSize)
                .map(entry -> getWarpRegularColumn(entry.getKey()).getName())
                .collect(Collectors.toSet());
    }

    DispatcherSplit createDispatcherSplit(ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session);

    ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle);

    default boolean proxyHasPushedDownFilter(DispatcherTableHandle dispatcherTableHandle)
    {
        // Currently, no proxy supports expressions. If it changes need to update implementations to check expressions pushDown
        return true;
    }

    default Pair<ConnectorMetadata, ConnectorTransactionHandle> createProxiedMetadata(
            Connector proxiedConnector,
            ConnectorSession session)
    {
        ConnectorTransactionHandle connectorTransactionHandle = proxiedConnector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata metadata = proxiedConnector.getMetadata(session, connectorTransactionHandle);
        return Pair.of(metadata, connectorTransactionHandle);
    }

    default Object getConvertedPartitionValue(String partitionValue, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        RegularColumn regularColumn = null;
        try {
            regularColumn = getWarpRegularColumn(columnHandle);
            if (partitionValue == null || partitionValue.equals(nullPartitionValue.orElse(null))) {
                return null;
            }
            return getPartitionValue(columnHandle, regularColumn.getName(), partitionValue).get();
        }
        catch (Exception e) {
            String errorMessage = format("failed to convert partition. error=%s, columnName=%s, partitionValue=%s, type=%s, nullPartitionValue=%s", e.getMessage(), regularColumn, partitionValue, getColumnType(columnHandle), nullPartitionValue);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    default Optional<Object> getConvertedPartitionValue(RowGroupData rowGroupData, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        String partitionValue = null;
        RegularColumn regularColumn = null;
        try {
            regularColumn = getWarpRegularColumn(columnHandle);
            partitionValue = rowGroupData.getPartitionKeys().get(regularColumn);
            if (partitionValue == null) {
                return Optional.empty();
            }
            if (partitionValue.equals(nullPartitionValue.orElse(null))) {
                return Optional.empty();
            }
            return getPartitionValue(columnHandle, regularColumn.getName(), partitionValue);
        }
        catch (Exception e) {
            String errorMessage = format("failed to convert partition. error=%s, columnName=%s, partitionValue=%s, type=%s, nullPartitionValue=%s, partitionKeys=%s", e.getMessage(), regularColumn, partitionValue, getColumnType(columnHandle), nullPartitionValue, rowGroupData.getPartitionKeys());
            throw new IllegalArgumentException(errorMessage);
        }
    }

    default Optional<Object> getPartitionValue(ColumnHandle columnHandle, String partitionName, String partitionValue)
    {
        return Optional.of(parsePartitionValue(partitionName, partitionValue, getColumnType(columnHandle)));
    }

    /**
     * Controls whether we consider a query for warmup/query on Wrap Speed or not.
     * <p>
     * return: false on unsupported queries (for example "update"/"insert"/"delete")
     */
    default boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        return true;
    }

    default SimplifiedColumns getSimplifiedColumns(
            ConnectorTableHandle connectorTableHandle,
            TupleDomain<ColumnHandle> fullPredicate,
            int predicateThreshold)
    {
        SimplifyResult<ColumnHandle> simplifyResult = DomainUtils.simplify(fullPredicate, predicateThreshold);

        Set<RegularColumn> currentSimplifiedColumns = connectorTableHandle instanceof DispatcherTableHandle dispatcherTableHandle ?
                dispatcherTableHandle.getSimplifiedColumns().simplifiedColumns() : Collections.emptySet();

        return new SimplifiedColumns(Stream.concat(simplifyResult.getSimplifiedColumns()
                                .stream()
                                .map(this::getWarpRegularColumn),
                        currentSimplifiedColumns.stream())
                .collect(Collectors.toSet()));
    }

    SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle);

    ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle);

    ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit);

    default Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            ConnectorNodePartitioningProvider nodePartitionProvider,
            List<Node> nodes)
    {
        Optional<ConnectorBucketNodeMap> connectorBucketNodeMapOptional =
                nodePartitionProvider.getBucketNodeMapping(transactionHandle, session, partitioningHandle);

        connectorBucketNodeMapOptional.ifPresent(connectorBucketNodeMap -> {
            if (connectorBucketNodeMap.hasFixedMapping()) {
                throw new UnsupportedOperationException();
            }
        });
        return connectorBucketNodeMapOptional;
    }

    default Optional<Long> getRowCount(ConnectorSplit connectorSplit)
    {
        return Optional.empty();
    }

    default long getDeletedRowsCount(ConnectorSplit connectorSplit)
    {
        return 0;
    }
}
