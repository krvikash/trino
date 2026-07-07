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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.ZERO;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.iceberg.IcebergTestUtils.CREATE_CHANGELOG_VIEW;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.OPTIMIZE_POSITION_DELETES;
import static io.trino.plugin.iceberg.IcebergTestUtils.REMOVE_DANGLING_DELETE_FILES;
import static io.trino.plugin.iceberg.IcebergTestUtils.TABLE_STATISTICS_READER;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.TestIcebergPartitionStatistics.PARTITION_STATISTICS_WRITER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

final class TestCompositeIcebergSplitSource
        extends AbstractTestQueryFramework
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig().setCompositeSplitsEnabled(true),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .setPropertyValues(ImmutableMap.of(IcebergSessionProperties.SPLIT_SIZE, "100MB"))
            .build();

    private TrinoFileSystemFactory fileSystemFactory;
    private TrinoCatalog catalog;
    private IcebergMetadata icebergMetadata;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .build();

        HiveMetastore metastore = getHiveMetastore(queryRunner);

        this.fileSystemFactory = getFileSystemFactory(queryRunner);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        this.catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                new NoopWorkScheduler(),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                FILE_IO_FACTORY,
                TESTING_TYPE_MANAGER,
                new FileMetastoreTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY),
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                new IcebergScheduledMvRefreshConfig().isScheduledMaterializedViewRefreshEnabled(),
                new IcebergIncrementalMvRefreshConfig().isMaterializedViewIncrementalColumnRefreshEnabled(),
                directExecutor(),
                newDirectExecutorService());
        this.icebergMetadata = new IcebergMetadata(
                LocationAccessControl.ALLOW_ALL,
                AiModelAccessControl.ALLOW_ALL,
                new CatalogName("iceberg"),
                PLANNER_CONTEXT.getTypeManager(),
                jsonCodec(CommitTaskData.class),
                catalog,
                (_, _) -> {
                    throw new UnsupportedOperationException();
                },
                TABLE_STATISTICS_READER,
                new TableStatisticsWriter(new NodeVersion("test-version")),
                PARTITION_STATISTICS_WRITER,
                UNSUPPORTED_DELETION_VECTOR_WRITER,
                OPTIMIZE_POSITION_DELETES,
                REMOVE_DANGLING_DELETE_FILES,
                CREATE_CHANGELOG_VIEW,
                Optional.empty(),
                3,
                false,
                _ -> false,
                UTC,
                newDirectExecutorService(),
                directExecutor(),
                newDirectExecutorService(),
                newDirectExecutorService(),
                0,
                ZERO,
                ConnectorExpressionEvaluator.NO_OP,
                false);

        return queryRunner;
    }

    @Test
    void testCompositeSplitsWithPartitionedTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_composite_partitioned WITH (format = 'PARQUET', partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation WHERE nationkey < 5", 5);
        assertUpdate("INSERT INTO test_composite_partitioned SELECT * FROM tpch.tiny.nation WHERE nationkey >= 5 AND nationkey < 10", 5);
        assertUpdate("INSERT INTO test_composite_partitioned SELECT * FROM tpch.tiny.nation WHERE nationkey >= 10 AND nationkey < 15", 5);
        assertUpdate("INSERT INTO test_composite_partitioned SELECT * FROM tpch.tiny.nation WHERE nationkey >= 15 AND nationkey < 20", 5);
        assertUpdate("INSERT INTO test_composite_partitioned SELECT * FROM tpch.tiny.nation WHERE nationkey >= 20", 5);
        try {
            SchemaTableName schemaTableName = new SchemaTableName("tpch", "test_composite_partitioned");
            Table table = catalog.loadTable(SESSION, schemaTableName);
            IcebergColumnHandle nationKey = IcebergColumnHandle.optional(
                            new ColumnIdentity(1, "nationkey", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()))
                    .columnType(BIGINT)
                    .build();
            IcebergTableHandle tableHandle = createTableHandle(schemaTableName, table, TupleDomain.all(), ImmutableSet.of(nationKey));

            List<ConnectorSplit> splits = generateConnectorSplits(SESSION, table, tableHandle, ImmutableSet.of());
            assertThat(splits).isNotEmpty();

            List<CompositeIcebergSplit> compositeSplits = splits.stream()
                    .filter(CompositeIcebergSplit.class::isInstance)
                    .map(CompositeIcebergSplit.class::cast)
                    .toList();
            assertThat(compositeSplits).isNotEmpty();
        }
        finally {
            assertUpdate("DROP TABLE test_composite_partitioned");
        }
    }

    @Test
    void testCompositeSplitsDisabledWithoutExplicitSplitSize()
            throws Exception
    {
        assertUpdate(withSmallRowGroups(getSession()), "CREATE TABLE test_composite_no_split_size WITH (format = 'PARQUET') AS SELECT * FROM tpch.tiny.nation", 25);
        try {
            SchemaTableName schemaTableName = new SchemaTableName("tpch", "test_composite_no_split_size");
            Table table = catalog.loadTable(SESSION, schemaTableName);
            IcebergColumnHandle nationKey = IcebergColumnHandle.optional(
                            new ColumnIdentity(1, "nationkey", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()))
                    .columnType(BIGINT)
                    .build();
            IcebergTableHandle tableHandle = createTableHandle(schemaTableName, table, TupleDomain.all(), ImmutableSet.of(nationKey));

            // Composite splits enabled but no explicit split size
            ConnectorSession session = TestingConnectorSession.builder()
                    .setPropertyMetadata(new IcebergSessionProperties(
                            new IcebergConfig().setCompositeSplitsEnabled(true),
                            new OrcReaderConfig(),
                            new OrcWriterConfig(),
                            new ParquetReaderConfig(),
                            new ParquetWriterConfig())
                            .getSessionProperties())
                    .build();
            List<ConnectorSplit> splits = generateConnectorSplits(session, table, tableHandle, ImmutableSet.of());
            assertThat(splits).isNotEmpty();
            assertThat(splits).allMatch(IcebergSplit.class::isInstance);
        }
        finally {
            assertUpdate("DROP TABLE test_composite_no_split_size");
        }
    }

    @Test
    void testCompositeSplitsDisabledWithMetadataColumns()
            throws Exception
    {
        assertUpdate(withSmallRowGroups(getSession()), "CREATE TABLE test_composite_metadata_col WITH (format = 'PARQUET') AS SELECT * FROM tpch.tiny.nation", 25);
        try {
            SchemaTableName schemaTableName = new SchemaTableName("tpch", "test_composite_metadata_col");
            Table table = catalog.loadTable(SESSION, schemaTableName);
            IcebergColumnHandle nationKey = IcebergColumnHandle.optional(
                            new ColumnIdentity(1, "nationkey", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()))
                    .columnType(BIGINT)
                    .build();
            IcebergColumnHandle pathColumn = IcebergColumnHandle.pathColumnHandle();
            IcebergTableHandle tableHandle = createTableHandle(schemaTableName, table, TupleDomain.all(), ImmutableSet.of(nationKey, pathColumn));

            List<ConnectorSplit> splits = generateConnectorSplits(SESSION, table, tableHandle, ImmutableSet.of());
            assertThat(splits).isNotEmpty();
            assertThat(splits).allMatch(IcebergSplit.class::isInstance);
        }
        finally {
            assertUpdate("DROP TABLE test_composite_metadata_col");
        }
    }

    private List<ConnectorSplit> generateConnectorSplits(
            ConnectorSession session,
            Table table,
            IcebergTableHandle tableHandle,
            Set<ColumnHandle> dynamicFilterColumns)
            throws Exception
    {
        try (IcebergSplitSource splitSource = new IcebergSplitSource(
                new DefaultIcebergFileSystemFactory(fileSystemFactory),
                session,
                icebergMetadata,
                tableHandle,
                table,
                table.newScan(),
                Optional.empty(),
                alwaysTrue(),
                TESTING_TYPE_MANAGER,
                false,
                0,
                new NoopSplitAffinityProvider(),
                new InMemoryMetricsReporter(),
                newDirectExecutorService(),
                dynamicFilterColumns,
                ConnectorExpressionEvaluator.NO_OP)) {
            ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get()
                        .forEach(builder::add);
            }
            assertThat(splitSource.isFinished()).isTrue();
            return builder.build();
        }
    }

    private static IcebergTableHandle createTableHandle(
            SchemaTableName schemaTableName,
            Table nationTable,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns)
    {
        return new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                OptionalLong.empty(),
                SchemaParser.toJson(nationTable.schema()),
                nationTable.spec() == null ? OptionalInt.empty() : OptionalInt.of(nationTable.spec().specId()),
                transformValues(nationTable.specs(), PartitionSpecParser::toJson),
                1,
                unenforcedPredicate,
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                projectedColumns,
                Optional.empty(),
                nationTable.location(),
                nationTable.properties(),
                Optional.empty(),
                Optional.empty(),
                false,
                UUID.randomUUID(),
                false,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.of(false));
    }
}
