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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.plugin.warp.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.warp.util.NodeUtils;
import io.trino.spi.Node;
import io.trino.spi.SplitWeight;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE_NAME = "test_table";
    private static final String PATH = "/test/path.parquet";
    private static final long START = 0L;
    private static final long LENGTH = 100L;
    private static final long SNAPSHOT_ID = 1L;

    private final IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformer =
            new IcebergProxiedConnectorTransformer(new ProxiedConnectorConfig());

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                CatalogHandle.createRootCatalogHandle(new CatalogName("warp"), new CatalogHandle.CatalogVersion("422")),
                "schema",
                "table",
                TableType.DATA,
                Optional.of(1L),
                "tableSchemaJson",
                Optional.of("partitionSpecJson"),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                Set.of(mock(IcebergColumnHandle.class)),
                Optional.of("nameMappingJson"),
                "tableLocation",
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.of(DataSize.of(1, DataSize.Unit.BYTE)),
                false,
                Collections.emptySet(),
                Optional.empty());

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                icebergTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false,
                Set.of());

        IcebergTableHandle expectedTableHandleForWarming = new IcebergTableHandle(
                icebergTableHandle.getCatalog(),
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getPartitionSpecJson(),
                icebergTableHandle.getFormatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                Optional.empty(),
                icebergTableHandle.getBranch(),
                icebergTableHandle.isRecordScannedFiles(),
                Optional.empty(),
                false,
                icebergTableHandle.getConstraintColumns(),
                icebergTableHandle.getForAnalyze());

        super.testCreateProxyTableHandleForWarming(
                icebergProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleForWarming);
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                CatalogHandle.createRootCatalogHandle(new CatalogName("warp"), new CatalogHandle.CatalogVersion("422")),
                "schema",
                "table",
                TableType.DATA,
                Optional.of(1L),
                "tableSchemaJson",
                Optional.of("partitionSpecJson"),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                Set.of(mock(IcebergColumnHandle.class)),
                Optional.of("nameMappingJson"),
                "tableLocation",
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.of(DataSize.of(1, DataSize.Unit.BYTE)),
                false,
                Collections.emptySet(),
                Optional.empty());

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                icebergTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false,
                Set.of());

        IcebergTableHandle expectedTableHandleMixedQuery = new IcebergTableHandle(
                icebergTableHandle.getCatalog(),
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getPartitionSpecJson(),
                icebergTableHandle.getFormatVersion(),
                icebergTableHandle.getUnenforcedPredicate(),
                TupleDomain.all(),
                icebergTableHandle.getLimit(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                Optional.empty(),
                icebergTableHandle.getBranch(),
                icebergTableHandle.isRecordScannedFiles(),
                Optional.empty(),
                false,
                icebergTableHandle.getConstraintColumns(),
                icebergTableHandle.getForAnalyze());

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                icebergProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleMixedQuery);
    }

    @Test
    public void testSnapshotKeyUniqueness()
    {
        ConnectorSplitNodeDistributor splitDistributor = mock(ConnectorSplitNodeDistributor.class);
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                CatalogHandle.createRootCatalogHandle(mock(io.trino.spi.catalog.CatalogName.class), new CatalogHandle.CatalogVersion("1")),
                SCHEMA_NAME,
                TABLE_NAME,
                TableType.DATA,
                Optional.of(SNAPSHOT_ID),
                """
                        {
                          "type": "struct",
                          "schema-id": 0,
                          "fields": [
                            {
                              "id": 1,
                              "name": "id",
                              "required": true,
                              "type": "long"
                            }
                          ]
                        }
                        """,
                Optional.empty(),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(mock(IcebergColumnHandle.class)),
                Optional.empty(),
                "",
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                false,
                Collections.emptySet(),
                Optional.empty());

        ConnectorSplit icebergSplit = new IcebergSplit(
                PATH,
                START,
                LENGTH,
                1024L,
                100L,
                IcebergFileFormat.ORC,
                """
                        { "spec-id": 0, "fields": [] }
                        """,
                """
                        { "partitionValues": [] }
                        """,
                ImmutableList.of(),
                SplitWeight.standard(),
                TupleDomain.all(),
                ImmutableMap.of(),
                1L
        );
        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                OptionalLong.of(SNAPSHOT_ID),
                TupleDomain.all(),
                mock(io.trino.plugin.warp.dispatcher.SimplifiedColumns.class),
                icebergTableHandle,
                Optional.empty(),
                ImmutableList.of(),
                false,
                Set.of()
        );
        Node node1 = NodeUtils.node(0, true);
        when(splitDistributor.getNode(anyString())).thenReturn(node1);

        ConnectorSession session = mock(ConnectorSession.class);

        DispatcherSplit dispatcherSplit = this.icebergProxiedConnectorTransformer.createDispatcherSplit(icebergSplit, dispatcherTableHandle, splitDistributor, session);

        assertThat(dispatcherSplit.getSchemaName()).isEqualTo(SCHEMA_NAME);
        assertThat(dispatcherSplit.getTableName()).isEqualTo(TABLE_NAME);
        assertThat(dispatcherSplit.getStart()).isEqualTo(START);
        assertThat(dispatcherSplit.getLength()).isEqualTo(LENGTH);
        assertThat(dispatcherSplit.getProxyConnectorSplit()).isEqualTo(icebergSplit);

        DispatcherTableHandle dispatcherTableHandleAnotherSnapshot = new DispatcherTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                OptionalLong.of(2L),
                TupleDomain.all(),
                mock(io.trino.plugin.warp.dispatcher.SimplifiedColumns.class),
                icebergTableHandle,
                Optional.empty(),
                ImmutableList.of(),
                false,
                Set.of());

        DispatcherSplit dispatcherSplitAnotherSnapshot = this.icebergProxiedConnectorTransformer.createDispatcherSplit(icebergSplit, dispatcherTableHandleAnotherSnapshot, splitDistributor, session);
        assertThat(dispatcherSplitAnotherSnapshot.getDeletedFilesHash()).isEqualTo(dispatcherSplit.getDeletedFilesHash());

        ProxiedConnectorConfig newConfig = new ProxiedConnectorConfig();
        newConfig.setEnableIcebergSnapshotIdUniqueness(true);
        IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformerUnique = new IcebergProxiedConnectorTransformer(newConfig);
        dispatcherSplitAnotherSnapshot = icebergProxiedConnectorTransformerUnique.createDispatcherSplit(icebergSplit, dispatcherTableHandleAnotherSnapshot, splitDistributor, session);
        assertThat(dispatcherSplitAnotherSnapshot.getDeletedFilesHash()).isNotEqualTo(dispatcherSplit.getDeletedFilesHash());
    }

    @Override
    protected void assertTablesAreEqual(ConnectorTableHandle expected, ConnectorTableHandle actual)
    {
        super.assertTablesAreEqual(expected, actual);

        IcebergTableHandle icebergActual = (IcebergTableHandle) actual;

        // not part of equals() and should always be overwritten to empty
        assertThat(icebergActual.getMaxScannedFileSize()).isEmpty();
    }
}
