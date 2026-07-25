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
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
            new IcebergProxiedConnectorTransformer(new ProxiedConnectorConfig(), TESTING_TYPE_MANAGER);

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                "schema",
                "table",
                TableType.DATA,
                OptionalLong.of(1L),
                "tableSchemaJson",
                OptionalInt.empty(),
                ImmutableMap.of(),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                Set.of(mock(IcebergColumnHandle.class)),
                Optional.of("nameMappingJson"),
                "tableLocation",
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                UUID.randomUUID(),
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
                Metrics.EMPTY,
                false);

        IcebergTableHandle expectedTableHandleForWarming = new IcebergTableHandle(
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
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                Optional.empty(),
                icebergTableHandle.getBranch(),
                icebergTableHandle.isVersionPinnedByQuery(),
                icebergTableHandle.getTableUuid(),
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
                "schema",
                "table",
                TableType.DATA,
                OptionalLong.of(1L),
                "tableSchemaJson",
                OptionalInt.empty(),
                ImmutableMap.of(),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                Set.of(mock(IcebergColumnHandle.class)),
                Optional.of("nameMappingJson"),
                "tableLocation",
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                UUID.randomUUID(),
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
                Metrics.EMPTY,
                false);

        IcebergTableHandle expectedTableHandleMixedQuery = new IcebergTableHandle(
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getSpecId(),
                icebergTableHandle.getPartitionSpecJsons(),
                icebergTableHandle.getFormatVersion(),
                icebergTableHandle.getUnenforcedPredicate(),
                TupleDomain.all(),
                icebergTableHandle.getLimit(),
                icebergTableHandle.preferSmallInitialReads(),
                OptionalInt.empty(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                Optional.empty(),
                icebergTableHandle.getBranch(),
                icebergTableHandle.isVersionPinnedByQuery(),
                icebergTableHandle.getTableUuid(),
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
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                TableType.DATA,
                OptionalLong.of(SNAPSHOT_ID),
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
                OptionalInt.of(0),
                ImmutableMap.of(0, PartitionSpecParser.toJson(PartitionSpec.unpartitioned())),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                ImmutableSet.of(mock(IcebergColumnHandle.class)),
                Optional.empty(),
                "",
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                UUID.randomUUID(),
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
                0,
                0,
                ImmutableList.of(),
                ImmutableList.of(),
                SplitWeight.standard(),
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.of(1L),
                OptionalLong.empty());
        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                OptionalLong.of(SNAPSHOT_ID),
                TupleDomain.all(),
                mock(SimplifiedColumns.class),
                icebergTableHandle,
                Optional.empty(),
                Metrics.EMPTY,
                false);
        ConnectorSession session = mock(ConnectorSession.class);

        DispatcherSplit dispatcherSplit = this.icebergProxiedConnectorTransformer.createDispatcherSplit(icebergSplit, dispatcherTableHandle, session);

        assertThat(dispatcherSplit.schemaName()).isEqualTo(SCHEMA_NAME);
        assertThat(dispatcherSplit.tableName()).isEqualTo(TABLE_NAME);
        assertThat(dispatcherSplit.start()).isEqualTo(START);
        assertThat(dispatcherSplit.length()).isEqualTo(LENGTH);
        assertThat(dispatcherSplit.proxyConnectorSplit()).isEqualTo(icebergSplit);

        DispatcherTableHandle dispatcherTableHandleAnotherSnapshot = new DispatcherTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                OptionalLong.of(2L),
                TupleDomain.all(),
                mock(SimplifiedColumns.class),
                icebergTableHandle,
                Optional.empty(),
                Metrics.EMPTY,
                false);

        DispatcherSplit dispatcherSplitAnotherSnapshot = this.icebergProxiedConnectorTransformer.createDispatcherSplit(icebergSplit, dispatcherTableHandleAnotherSnapshot, session);
        assertThat(dispatcherSplitAnotherSnapshot.deletedFilesHash()).isEqualTo(dispatcherSplit.deletedFilesHash());

        ProxiedConnectorConfig newConfig = new ProxiedConnectorConfig();
        newConfig.setEnableIcebergSnapshotIdUniqueness(true);
        IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformerUnique = new IcebergProxiedConnectorTransformer(newConfig, TESTING_TYPE_MANAGER);
        dispatcherSplitAnotherSnapshot = icebergProxiedConnectorTransformerUnique.createDispatcherSplit(icebergSplit, dispatcherTableHandleAnotherSnapshot, session);
        assertThat(dispatcherSplitAnotherSnapshot.deletedFilesHash()).isNotEqualTo(dispatcherSplit.deletedFilesHash());
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
