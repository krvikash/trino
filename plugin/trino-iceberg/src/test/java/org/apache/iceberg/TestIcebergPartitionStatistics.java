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
package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.IcebergPageSourceProviderFactory;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.PartitionStatisticsReader;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.listFiles;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.apache.iceberg.TestIcebergPartitionStatistics.Statistics.column;
import static org.apache.iceberg.TestIcebergPartitionStatistics.Statistics.rowCount;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestIcebergPartitionStatistics
        extends AbstractTestQueryFramework
{
    public static final BlocksHashFactory BLOCKS_HASH_FACTORY = new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators())).createBlocksHashFactory();
    public static final PartitionStatisticsReader PARTITION_STATISTICS_READER = new PartitionStatisticsReader(
            TESTING_TYPE_MANAGER,
            new IcebergPageSourceProviderFactory(
                    new DefaultIcebergFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)),
                    new ForwardingFileIoFactory(newDirectExecutorService()),
                    new FileFormatDataSourceStats(),
                    new OrcReaderConfig(),
                    new ParquetReaderConfig(),
                    new IcebergConfig(),
                    TESTING_TYPE_MANAGER,
                    BLOCKS_HASH_FACTORY,
                    ParquetFooterCache.noop()));

    public static final PartitionStatisticsWriter PARTITION_STATISTICS_WRITER = new PartitionStatisticsWriter(
            TESTING_TYPE_MANAGER,
            new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
            new IcebergFileWriterFactory(
                    TESTING_TYPE_MANAGER,
                    new NodeVersion("test_version"),
                    new FileFormatDataSourceStats(),
                    new IcebergConfig(),
                    new OrcWriterConfig()),
            PARTITION_STATISTICS_READER,
            newDirectExecutorService());

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path dataDirectory = Files.createTempDirectory("test_iceberg_partition_stats");
        dataDirectory.toFile().deleteOnExit();
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setMetastoreDirectory(dataDirectory.toFile())
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.partition-statistics.collect-on-write", "true")
                .build();

        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        fileSystem = fileSystemFactory.create(SESSION);

        return queryRunner;
    }

    @Test
    void testUnpartitionTable()
    {
        try (TestTable table = newTrinoTable("test", "AS SELECT * FROM tpch.tiny.nation")) {
            BaseTable icebergTable = loadTable(table.getName());
            assertThat(icebergTable.partitionStatisticsFiles()).isEmpty();

            assertStats(
                    table.getName(),
                    column("nationkey", null, 25.0, 0.0, null, "0", "24"),
                    column("name", 513.0, 25.0, 0.0, null, null, null),
                    column("regionkey", null, 5.0, 0.0, null, "0", "4"),
                    column("comment", 2087.0, 25.0, 0.0, null, null, null),
                    rowCount(25.0));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testPartitionTable(int version)
    {
        try (TestTable table = newTrinoTable(
                "test_partition",
                "(id INT, part INT) WITH (format_version = " + version + ", partitioning = ARRAY['part'])",
                ImmutableList.of("1, 10", "2, 20", "3, 20"))) {
            assertStats(
                    table.getName(),
                    column("id", null, 3.0, 0.0, null, null, null),
                    column("part", null, 2.0, 0.03, null, null, null),
                    rowCount(3.0));

            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 30), (5, 30), (6, 30)", 3);
            assertStats(
                    table.getName(),
                    column("id", null, 6.0, 0.0, null, null, null),
                    column("part", null, 3.0, 0.03, null, null, null),
                    rowCount(6.0));

            // Test filtering on partition column
            assertStats(
                    "(SELECT * FROM " + table.getName() + " WHERE part = 10)",
                    column("id", null, 1.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.0, null, null, null),
                    rowCount(1.0));
            assertStats(
                    "(SELECT * FROM " + table.getName() + " WHERE part = 20)",
                    column("id", null, 2.0, 0.0, null, null, null),
                    column("part", null, 2.0, 0.0, null, null, null),
                    rowCount(2.0));
            assertStats(
                    "(SELECT * FROM " + table.getName() + " WHERE part = 30)",
                    column("id", null, 3.0, 0.0, null, null, null),
                    column("part", null, 3.0, 0.0, null, null, null),
                    rowCount(3.0));
        }
    }

    @ParameterizedTest
    @CsvSource(delimiterString = "|", quoteCharacter = '\\', value = {
            "BOOLEAN | true",
            "INTEGER | 123",
            "BIGINT | 456",
            "REAL | 0.1",
            "DOUBLE | 1.2",
            "DECIMAL(3, 1) | 32.1",
            "DECIMAL(38, 1) | 123456.1",
            "DATE | DATE '2024-01-01'",
            "TIME(6) | TIME '12:34:56.123456'",
            "TIMESTAMP(6) | TIMESTAMP '2024-01-01 12:34:56.123456'",
            "TIMESTAMP(9) | TIMESTAMP '2024-01-01 12:34:56.123456789'",
            "TIMESTAMP(6) WITH TIME ZONE | TIMESTAMP '2024-01-01 12:34:56.123456 UTC'",
            "TIMESTAMP(9) WITH TIME ZONE | TIMESTAMP '2024-01-01 12:34:56.123456789 UTC'",
            "VARCHAR | 'test'",
            "UUID | UUID '123e4567-e89b-12d3-a456-426614174000'",
            "VARBINARY | X'01020304'",
    })
    void testPartitionTableColumnType(String type, String value)
    {
        try (TestTable table = newTrinoTable(
                "test_partition",
                "(id INT, part " + type + ") WITH (partitioning = ARRAY['part'])",
                ImmutableList.of("1, " + value))) {
            assertStats(
                    table.getName(),
                    column("id", null, 1.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.0, null, null, null),
                    rowCount(1.0));

            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, " + value + ")", 1);
            assertStats(
                    table.getName(),
                    column("id", null, 2.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(2.0));
        }
    }

    @Test
    void testPredicateWithPartitionTransform()
    {
        try (TestTable table = newTrinoTable(
                "test_non_identity",
                "(id INT, ds DATE) WITH (partitioning = ARRAY['year(ds)'])",
                ImmutableList.of("(1, DATE '2024-03-15')", "(2, DATE '2024-07-20')", "(3, DATE '2025-01-10')"))) {
            // The connector ignores the filtering condition
            assertStats(
                    "(SELECT * FROM " + table.getName() + " WHERE ds >= DATE '2024-01-01' AND ds < DATE '2025-01-01')",
                    column("id", null, 3.0, 0.0, null, null, null),
                    column("ds", null, 3.0, 0.0, null, null, null),
                    rowCount(3.0));
        }
    }

    @Test
    void testPredicateAfterPartitionSpecEvolution()
    {
        try (TestTable table = newTrinoTable("test_evolution", "(id INT, ds DATE) WITH (partitioning = ARRAY['ds'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, DATE '2024-03-15'), (2, DATE '2024-07-20')", 2);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['year(ds)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, DATE '2025-01-10')", 1);

            assertStats(
                    "(SELECT * FROM " + table.getName() + " WHERE ds >= DATE '2024-01-01' AND ds < DATE '2025-01-01')",
                    column("id", null, 3.0, 0.0, null, null, null),
                    column("ds", null, 3.0, 0.0, null, null, null),
                    rowCount(3.0));
        }
    }

    @ParameterizedTest
    @CsvSource(delimiterString = "|", quoteCharacter = '\\', value = {
            "TIMESTAMP | TIMESTAMP '1971-01-01 12:34:56.123456' | year(part) | {\"part_year\": 1}",
            "TIMESTAMP | TIMESTAMP '1970-02-01 12:34:56.123456' | month(part) | {\"part_month\": 1}",
            "TIMESTAMP | TIMESTAMP '1970-01-02 12:34:56.123456' | day(part) | {\"part_day\": 1}",
            "TIMESTAMP | TIMESTAMP '1970-01-01 12:34:56.123456' | hour(part) | {\"part_hour\": 12}",
            "INT | 123 | bucket(part, 3) | {\"part_bucket\": 1}",
            "VARCHAR | 'test' | truncate(part, 2) | {\"part_trunc\": \"te\"}",
    })
    void testPartitionTransform(String type, String value, String partitioning, String expectedPartition)
    {
        try (TestTable table = newTrinoTable(
                "test_partition_transform",
                "(id INT, part " + type + ") WITH (partitioning = ARRAY['" + partitioning + "'])",
                ImmutableList.of("1, " + value))) {
            BaseTable icebergTable = loadTable(table.getName());
            PartitionStatisticsFile partitionStatisticsFile = icebergTable.partitionStatisticsFiles().stream().collect(onlyElement());
            List<GenericData.Record> partitionStatistics = readPartitionStatistics(icebergTable, partitionStatisticsFile.path());

            assertThat(partitionStatistics).hasSize(1);
            assertThat(partitionStatistics.getFirst().get(0).toString()).isEqualTo(expectedPartition);
        }
    }

    @Test
    void testNestedPartition()
    {
        try (TestTable table = newTrinoTable("test_nested_partition", "(id INT, part VARCHAR, nested VARCHAR) WITH (partitioning = ARRAY['part', 'nested'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'part#1', 'nested#1'), (2, 'part#1', 'nested#2'), (3, 'part#2', 'nested#3'), (4, 'part#2', 'nested#4')", 4);
            BaseTable icebergTable = loadTable(table.getName());
            PartitionStatisticsFile partitionStatisticsFile = icebergTable.partitionStatisticsFiles().stream().collect(onlyElement());
            List<GenericData.Record> partitionStatistics = readPartitionStatistics(icebergTable, partitionStatisticsFile.path());
            assertThat(partitionStatistics)
                    .extracting(statistics -> statistics.get(0).toString())
                    .containsExactly(
                            "{\"part\": \"part#1\", \"nested\": \"nested#1\"}",
                            "{\"part\": \"part#1\", \"nested\": \"nested#2\"}",
                            "{\"part\": \"part#2\", \"nested\": \"nested#3\"}",
                            "{\"part\": \"part#2\", \"nested\": \"nested#4\"}");
        }
    }

    @Test
    void testPositionDeletes()
    {
        try (TestTable table = newTrinoTable(
                "test_position_deletes",
                "(id INT, part INT) WITH (format_version = 2, partitioning = ARRAY['part'])",
                ImmutableList.of("1, 10", "2, 10"))) {
            assertStats(
                    table.getName(),
                    column("id", null, 2.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(2.0));

            // Row-level delete doesn't update partition level stats
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertStats(
                    table.getName(),
                    column("id", null, 2.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(2.0));

            // ANALYZE updates partition stats
            assertUpdate("ANALYZE " + table.getName());
            assertStats(
                    table.getName(),
                    column("id", null, 1.0, 0.03, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(1.6));
        }
    }

    @Test
    void testDeletionVectors()
    {
        try (TestTable table = newTrinoTable(
                "test_deletion_vectors",
                "(id INT, part INT) WITH (format_version = 2, partitioning = ARRAY['part'])",
                ImmutableList.of("1, 10", "2, 10"))) {
            assertStats(
                    table.getName(),
                    column("id", null, 2.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(2.0));

            // Row-level delete doesn't update partition level stats
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertStats(
                    table.getName(),
                    column("id", null, 2.0, 0.0, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(2.0));

            // ANALYZE updates partition stats
            assertUpdate("ANALYZE " + table.getName());
            assertStats(
                    table.getName(),
                    column("id", null, 1.0, 0.03, null, null, null),
                    column("part", null, 1.0, 0.03, null, null, null),
                    rowCount(1.6));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testPartitionTableDetails(int version)
    {
        try (TestTable table = newTrinoTable(
                "test_partition",
                "(id INT, part INT) WITH (format_version = " + version + ", partitioning = ARRAY['part'])",
                ImmutableList.of("1, 10", "2, 20", "3, 20"))) {
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(1);

            PartitionStatisticsFile partitionStatisticsFile = partitionStatisticsFiles.getFirst();
            assertThat(partitionStatisticsFile).isNotNull();

            Parquet.ReadBuilder file = Parquet.read(new ForwardingInputFile(fileSystem.newInputFile(Location.of(partitionStatisticsFile.path()))));
            file.project(PartitionStatsHandler.schema(Partitioning.partitionType(icebergTable), version));
            List<GenericData.Record> records = ImmutableList.copyOf(file.build());
            assertThat(records).hasSize(2);

            int expectedFieldCount = version <= 2 ? 12 : 13;

            // part=10
            GenericData.Record part10 = records.getFirst();
            assertThat(part10.getSchema().getFields()).hasSize(expectedFieldCount);
            assertThat(((GenericData.Record) part10.get("partition")).get("part")).isEqualTo(10);
            assertThat(part10.get("spec_id")).isEqualTo(0);
            assertThat(part10.get("data_record_count")).isEqualTo(1L);
            assertThat(part10.get("data_file_count")).isEqualTo(1);
            assertThat(part10.get("total_data_file_size_in_bytes")).isEqualTo(323L);
            assertThat(part10.get("position_delete_record_count")).isEqualTo(0L);
            assertThat(part10.get("position_delete_file_count")).isEqualTo(0);
            assertThat(part10.get("equality_delete_record_count")).isEqualTo(0L);
            assertThat(part10.get("equality_delete_file_count")).isEqualTo(0);
            assertThat(part10.get("total_record_count")).isNull(); // optional in all versions
            assertThat(part10.get("last_updated_at")).isNotNull();
            assertThat(part10.get("last_updated_snapshot_id")).isNotNull();
            if (version >= 3) {
                assertThat(part10.get("dv_count")).isEqualTo(0);
            }

            // part=20
            GenericData.Record part20 = records.get(1);
            assertThat(part20.getSchema().getFields()).hasSize(expectedFieldCount);
            assertThat(((GenericData.Record) part20.get("partition")).get("part")).isEqualTo(20);
            assertThat(part20.get("spec_id")).isEqualTo(0);
            assertThat(part20.get("data_record_count")).isEqualTo(2L);
            assertThat(part20.get("data_file_count")).isEqualTo(1);
            assertThat(part20.get("total_data_file_size_in_bytes")).isEqualTo(362L);
            assertThat(part20.get("position_delete_record_count")).isEqualTo(0L);
            assertThat(part20.get("position_delete_file_count")).isEqualTo(0);
            assertThat(part20.get("equality_delete_record_count")).isEqualTo(0L);
            assertThat(part20.get("equality_delete_file_count")).isEqualTo(0);
            assertThat(part20.get("total_record_count")).isNull(); // optional in all versions
            assertThat(part20.get("last_updated_at")).isNotNull();
            assertThat(part20.get("last_updated_snapshot_id")).isNotNull();
            if (version >= 3) {
                assertThat(part20.get("dv_count")).isEqualTo(0);
            }
        }
    }

    @Test
    void testPartitionEvolution()
    {
        try (TestTable table = newTrinoTable("test_partition_evolution", "(id INT, part VARCHAR, nested VARCHAR) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'part#1', 'nested#1'), (2, 'part#1', 'nested#1'), (3, 'part#2', 'nested#2'), (4, 'part#2', 'nested#2')", 4);
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> initialPartitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(initialPartitionStatisticsFiles).hasSize(1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['part', 'nested']");
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");
            icebergTable.refresh();
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(2).containsOnlyOnceElementsOf(initialPartitionStatisticsFiles);
        }
    }

    @ParameterizedTest
    @EnumSource
    void testFormat(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable(
                "test_format",
                "(id INT, part INT) WITH (format = '" + format + "', partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10), (2, 20), (3, 20)", 3);
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            String extension = format != IcebergFileFormat.AVRO ? format.name().toLowerCase(ENGLISH) : "parquet";
            assertThat(partitionStatisticsFiles)
                    .extracting(PartitionStatisticsFile::path)
                    .allMatch(path -> path.endsWith(extension));
        }
    }

    @Test
    void testInsert()
    {
        try (TestTable table = newTrinoTable("test_insert", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10), (2, 10)", 2);
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(1);

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, partitionStatisticsFiles.getFirst().path());
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().get("data_record_count")).isEqualTo(2L);
        }
    }

    @Test
    void testIncrementalMergeNewPartitionSortedBeforeExisting()
    {
        try (TestTable table = newTrinoTable("test_incremental_sort_before", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            // First insert creates initial stats in REPLACE mode — only part=20 is present.
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 20)", 1);

            // Second insert introduces part=10 which must sort BEFORE part=20 in the merged output.
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 10)", 1);

            BaseTable icebergTable = loadTable(table.getName());
            long currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
            PartitionStatisticsFile statsFile = icebergTable.partitionStatisticsFiles().stream()
                    .filter(statisticsFile -> statisticsFile.snapshotId() == currentSnapshotId)
                    .collect(onlyElement());

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, statsFile.path());
            assertThat(records).hasSize(2);
            // The incremental partition (part=10) must appear first in sorted order.
            assertThat(((GenericData.Record) records.get(0).get("partition")).get("part")).isEqualTo(10);
            assertThat(records.get(0).get("data_record_count")).isEqualTo(1L);
            assertThat(((GenericData.Record) records.get(1).get("partition")).get("part")).isEqualTo(20);
            assertThat(records.get(1).get("data_record_count")).isEqualTo(1L);
        }
    }

    @Test
    void testIncrementalMergeUpdatesExistingAndAddsNewPartition()
    {
        try (TestTable table = newTrinoTable("test_incremental_merge_mixed", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            // First insert part=10 and part=30
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10), (2, 30)", 2);

            // Second insert, adds another record to part=10 and introduces part=20 (sorts between 10 and 30).
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 10), (4, 20)", 2);

            BaseTable icebergTable = loadTable(table.getName());
            long currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
            PartitionStatisticsFile statsFile = icebergTable.partitionStatisticsFiles().stream()
                    .filter(statisticsFile -> statisticsFile.snapshotId() == currentSnapshotId)
                    .collect(onlyElement());

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, statsFile.path());
            assertThat(records).hasSize(3);
            // part=10, merged — two separate data files from two separate inserts
            assertThat(((GenericData.Record) records.get(0).get("partition")).get("part")).isEqualTo(10);
            assertThat(records.get(0).get("data_record_count")).isEqualTo(2L);
            assertThat(records.get(0).get("data_file_count")).isEqualTo(2);
            // part=20, new partition inserted in sorted position between part=10 and part=30
            assertThat(((GenericData.Record) records.get(1).get("partition")).get("part")).isEqualTo(20);
            assertThat(records.get(1).get("data_record_count")).isEqualTo(1L);
            // part=30, passed through unchanged from the base stats file
            assertThat(((GenericData.Record) records.get(2).get("partition")).get("part")).isEqualTo(30);
            assertThat(records.get(2).get("data_record_count")).isEqualTo(1L);
        }
    }

    @Test
    void testIncrementalMergeKeepsSamePartitionWithDifferentSpecIdSeparate()
    {
        // Verifies that incremental merge does not collapse entries that project to the same current
        // partition value but originate from different partition specs.
        try (TestTable table = newTrinoTable("test_incremental_merge_same_partition_different_spec", "(id INT, part VARCHAR, nested VARCHAR) WITH (partitioning = ARRAY['part', 'nested'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'same', 'nested#1')", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['part']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 'same', 'nested#2')", 1);

            BaseTable icebergTable = loadTable(table.getName());
            long currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
            PartitionStatisticsFile statsFile = icebergTable.partitionStatisticsFiles().stream()
                    .filter(statisticsFile -> statisticsFile.snapshotId() == currentSnapshotId)
                    .collect(onlyElement());

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, statsFile.path());
            assertThat(records).hasSize(2);

            // According to https://iceberg.apache.org/spec/#partition-statistics-file.
            // These rows must be sorted (in ascending manner with NULL FIRST) by partition field.
            // the first record is the partition part only
            // the second record is the partition part + nested
            GenericData.Record first = records.get(0);
            GenericData.Record second = records.get(1);
            assertThat(((GenericData.Record) first.get("partition")).get("part").toString()).isEqualTo("same");
            assertThat(((GenericData.Record) first.get("partition")).get("nested")).isNull();
            assertThat(((GenericData.Record) second.get("partition")).get("part").toString()).isEqualTo("same");
            assertThat(((GenericData.Record) second.get("partition")).get("nested").toString()).isEqualTo("nested#1");
            assertThat(first.get("spec_id")).isEqualTo(1);
            assertThat(second.get("spec_id")).isEqualTo(0);
            assertThat(first.get("data_record_count")).isEqualTo(1L);
            assertThat(second.get("data_record_count")).isEqualTo(1L);
        }
    }

    @Test
    void testTruncate()
    {
        try (TestTable table = newTrinoTable("test_truncate", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10), (2, 10)", 2);
            assertUpdate("TRUNCATE TABLE " + table.getName());

            assertThat(loadTable(table.getName()).partitionStatisticsFiles()).isEmpty();
        }
    }

    @Test
    void testOptimize()
    {
        try (TestTable table = newTrinoTable("test_optimize", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 10)", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(3);

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, partitionStatisticsFiles.get(1).path());
            assertThat(records).hasSize(1);
        }
    }

    @Test
    void testAnalyze()
    {
        try (TestTable table = newTrinoTable("test_analyze", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10), (2, 10)", 2);
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(1);

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, partitionStatisticsFiles.getFirst().path());
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().get("data_record_count")).isEqualTo(2L);
        }
    }

    @Test
    void testCopyOnWrite()
    {
        try (TestTable table = newTrinoTable("test_cow", "(id INT, part INT) WITH (partitioning = ARRAY['part'], merge_mode = 'copy-on-write')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10), (2, 10)", 2);
            assertUpdate("UPDATE " + table.getName() + " SET id = id * 10", 2);
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(1);

            List<GenericData.Record> records = readPartitionStatistics(icebergTable, partitionStatisticsFiles.getFirst().path());
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().get("data_record_count")).isEqualTo(2L);
            assertThat(records.getFirst().get("data_file_count")).isEqualTo(1);
        }
    }

    @Test
    void testBranching()
    {
        try (TestTable table = newTrinoTable("test_branching", "(id INT, part INT) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("CREATE BRANCH dev IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES (1, 10), (2, 10)", 2);

            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(1);

            assertUpdate("ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO dev");
            assertThat(loadTable(table.getName()).partitionStatisticsFiles()).isEqualTo(partitionStatisticsFiles);
        }
    }

    @Test
    void testRemoveOrphanFiles()
            throws Exception
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "remove_orphan_files_min_retention", "0s")
                .build();

        try (TestTable table = newTrinoTable(
                "test_remove_orphan_files",
                "(id INT, part INT) WITH (partitioning = ARRAY['part'])",
                ImmutableList.of("1, 10", "2, 20", "3, 20"))) {
            BaseTable icebergTable = loadTable(table.getName());
            assertThat(listFiles(fileSystem, icebergTable.location() + "/metadata"))
                    .anySatisfy(entry -> assertThat(entry).startsWith("partition-stats"));

            assertUpdate(session, "ALTER TABLE " + table.getName() + " EXECUTE remove_orphan_files(retention_threshold => '0d')");
            assertThat(listFiles(fileSystem, icebergTable.location() + "/metadata"))
                    .anySatisfy(entry -> assertThat(entry).startsWith("partition-stats"));
        }
    }

    @Test
    void testNegativeRowCount()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_negative_row_count", "(id INT, part INT) WITH (partitioning = ARRAY['part'])", List.of("1, 10"))) {
            BaseTable icebergTable = loadTable(table.getName());

            // Create equality delete files resulting in negative row count
            writeEqualityDeleteForTable(icebergTable, new LocalFileSystemFactory(Files.createTempDirectory("prefix")), Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Object[] {1})), ImmutableMap.of("id", 1), Optional.empty());
            writeEqualityDeleteForTable(icebergTable, new LocalFileSystemFactory(Files.createTempDirectory("prefix")), Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Object[] {1})), ImmutableMap.of("id", 1), Optional.empty());
            assertUpdate("ANALYZE " + table.getName());

            assertStats(
                    table.getName(),
                    column("id", null, 0.8, 0.0, null, "null", "null"),
                    column("part", null, 0.8, 0.0, null, "null", "null"),
                    rowCount(0.8));
        }
    }

    @Test
    void testSkipWriteStats()
    {
        try (TestTable table = newTrinoTable(
                "test_skip_write_stats",
                "(id INT, part INT) WITH (partitioning = ARRAY['part'])",
                ImmutableList.of("1, 10", "2, 20", "3, 20"))) {
            BaseTable icebergTable = loadTable(table.getName());
            List<PartitionStatisticsFile> partitionStatisticsFiles = icebergTable.partitionStatisticsFiles();
            assertThat(partitionStatisticsFiles).hasSize(1);

            // Verify that stats file is not rewritten if there is no change
            assertUpdate("ANALYZE " + table.getName());
            assertThat(icebergTable.partitionStatisticsFiles()).isEqualTo(partitionStatisticsFiles);
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }

    private List<GenericData.Record> readPartitionStatistics(BaseTable icebergTable, String path)
    {
        Types.StructType partitionType = Partitioning.partitionType(icebergTable);
        Parquet.ReadBuilder file = Parquet.read(new ForwardingInputFile(fileSystem.newInputFile(Location.of(path))));
        file.project(PartitionStatsHandler.schema(partitionType, formatVersion(icebergTable)));
        return ImmutableList.copyOf(file.build());
    }

    private void assertStats(String tableName, Statistics... statistics)
    {
        checkArgument(statistics.length >= 2, "Should have at least one column and one row count");

        List<String> expected = Arrays.stream(statistics)
                .map(stat -> "(CAST(%s AS VARCHAR), CAST(%s AS DOUBLE), CAST(%s AS DOUBLE), CAST(%s AS DOUBLE), CAST(%s AS DOUBLE), CAST(%s AS VARCHAR), CAST(%s AS VARCHAR))".formatted(
                        stat.columnName,
                        stat.dataSize,
                        stat.distinctValuesCount,
                        stat.nullsFraction,
                        stat.rowCount,
                        stat.lowValue,
                        stat.highValue))
                .toList();
        assertThat(query("SHOW STATS FOR " + tableName))
                .matches("VALUES " + String.join(", ", expected));
    }

    record Statistics(String columnName, Double dataSize, Double distinctValuesCount, Double nullsFraction, Double rowCount, String lowValue, String highValue)
    {
        static Statistics column(String columnName, Double dataSize, Double distinctValuesCount, Double nullsFraction, Double rowCount, String lowValue, String highValue)
        {
            return new Statistics("'%s'".formatted(columnName), dataSize, distinctValuesCount, nullsFraction, rowCount, lowValue, highValue);
        }

        static Statistics rowCount(Double rowCount)
        {
            return new Statistics("null", null, null, null, rowCount, null, null);
        }
    }
}
