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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HivePartition;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.PartitionKey;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.objectstore.ObjectStoreProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.plugin.warp.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.testing.InterfaceTestUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.mockito.Mockito.mock;

public class ObjectStoreProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private final HiveProxiedConnectorTransformer hiveProxiedConnectorTransformer =
            new HiveProxiedConnectorTransformer(new ProxiedConnectorConfig());
    final Map<String, DispatcherProxiedConnectorTransformer> transformerMap =
            Map.of(ObjectStoreProxiedConnectorTransformer.getTransformerKey(HiveColumnHandle.class),
                    hiveProxiedConnectorTransformer);

    @Test
    public void testCreateDispatcherSplit()
    {
        HiveSplit hiveSplit = new HiveSplit(
                "partitionName",
                "path",
                1L,
                2L,
                3L,
                4L,
                new Schema(HiveStorageFormat.PARQUET.getSerde(), false, ImmutableMap.of()),
                List.of(new HivePartitionKey("name1", "key1"),
                        new HivePartitionKey("name2", "key2")),
                List.of(HostAddress.fromString("http://host:8080")),
                OptionalInt.empty(),
                OptionalInt.empty(),
                true,
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                SplitWeight.fromProportion(1L));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                mock(HiveTableHandle.class),
                Optional.empty(),
                Collections.emptyList(),
                false,
                Set.of());

        List<PartitionKey> partitionKeys = hiveSplit.getPartitionKeys()
                .stream()
                .map(hivePartitionKey -> new PartitionKey(new RegularColumn(hivePartitionKey.name()), hivePartitionKey.value()))
                .toList();
        DispatcherSplit expectedDispatcherSplit = new DispatcherSplit(
                dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                hiveSplit.getPath(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileModifiedTime(),
                List.of(node.getHostAndPort()),
                partitionKeys,
                "",
                hiveSplit);

        super.testCreateDispatcherSplit(
                new ObjectStoreProxiedConnectorTransformer(transformerMap),
                hiveSplit,
                dispatcherTableHandle,
                expectedDispatcherSplit);
    }

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        HiveTableHandle hiveTableHandle = new HiveTableHandle(
                "schema",
                "table",
                Optional.of(Map.of("1", "2")),
                List.of(mock(HiveColumnHandle.class)),
                List.of(mock(HiveColumnHandle.class)),
                Optional.empty(),
                Optional.of(List.of(new HivePartition(new SchemaTableName("1", "2")))),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptySet(),
                Set.of(mock(HiveColumnHandle.class)),
                mock(AcidTransaction.class),
                true,
                Optional.of(1L));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                hiveTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false,
                Set.of());

        HiveTableHandle expectedTableHandleForWarming = new HiveTableHandle(
                hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                hiveTableHandle.getTableParameters(),
                hiveTableHandle.getPartitionColumns(),
                hiveTableHandle.getDataColumns(),
                hiveTableHandle.getPartitionNames(),
                hiveTableHandle.getPartitions(),
                TupleDomain.all(),
                TupleDomain.all(),
                hiveTableHandle.getTablePartitioning(),
                hiveTableHandle.getBucketFilter(),
                hiveTableHandle.getAnalyzePartitionValues(),
                Collections.emptySet(),
                hiveTableHandle.getProjectedColumns(),
                hiveTableHandle.getTransaction(),
                hiveTableHandle.isRecordScannedFiles(),
                Optional.empty());

        super.testCreateProxyTableHandleForWarming(
                new ObjectStoreProxiedConnectorTransformer(transformerMap),
                dispatcherTableHandle,
                expectedTableHandleForWarming);
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        HiveTableHandle hiveTableHandle = new HiveTableHandle(
                "schema",
                "table",
                Optional.of(Map.of("1", "2")),
                List.of(mock(HiveColumnHandle.class)),
                List.of(mock(HiveColumnHandle.class)),
                Optional.empty(),
                Optional.of(List.of(new HivePartition(new SchemaTableName("1", "2")))),
                TupleDomain.withColumnDomains(Map.of(
                        new HiveColumnHandle("col", 0, HIVE_INT, INTEGER, Optional.empty(), REGULAR, Optional.empty()),
                        Domain.singleValue(INTEGER, 1L))),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptySet(),
                Set.of(mock(HiveColumnHandle.class)),
                mock(AcidTransaction.class),
                true,
                Optional.of(1L));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                hiveTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false,
                Set.of());

        HiveTableHandle expectedTableHandleMixedQuery = new HiveTableHandle(
                hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                hiveTableHandle.getTableParameters(),
                hiveTableHandle.getPartitionColumns(),
                hiveTableHandle.getDataColumns(),
                hiveTableHandle.getPartitionNames(),
                hiveTableHandle.getPartitions(),
                TupleDomain.all(),
                TupleDomain.all(),
                hiveTableHandle.getTablePartitioning(),
                hiveTableHandle.getBucketFilter(),
                hiveTableHandle.getAnalyzePartitionValues(),
                Collections.emptySet(),
                hiveTableHandle.getProjectedColumns(),
                hiveTableHandle.getTransaction(),
                hiveTableHandle.isRecordScannedFiles(),
                Optional.empty());

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                new ObjectStoreProxiedConnectorTransformer(transformerMap),
                dispatcherTableHandle,
                expectedTableHandleMixedQuery);
    }

    @Test
    public void assertAllImplemented()
            throws NoSuchMethodException
    {
        InterfaceTestUtils.assertAllMethodsOverridden(
                DispatcherProxiedConnectorTransformer.class,
                ObjectStoreProxiedConnectorTransformer.class,
                Set.of(
                        DispatcherProxiedConnectorTransformer.class.getMethod("getHostAddressForSplit", String.class, ConnectorSplitNodeDistributor.class),
                        DispatcherProxiedConnectorTransformer.class.getMethod("getSimplifiedColumns", ConnectorTableHandle.class, TupleDomain.class, int.class),
                        DispatcherProxiedConnectorTransformer.class.getMethod("getSplitKey", String.class, long.class, long.class),
                        DispatcherProxiedConnectorTransformer.class.getMethod("createProxiedMetadata", Connector.class, ConnectorSession.class)));
    }
}
