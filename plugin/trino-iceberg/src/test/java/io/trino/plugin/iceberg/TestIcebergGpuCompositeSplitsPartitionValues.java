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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.LongPredicate;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

/// Composite splits combine data files from different partitions, and their data files need
/// not contain the identity partition columns, e.g. after migrating a Hive table.
final class TestIcebergGpuCompositeSplitsPartitionValues
        extends AbstractTestQueryFramework
{
    private static final List<String> PARTITIONS = ImmutableList.of("a", "b", "c");
    private static final int FILE_COUNT = 8;
    private static final int ROW_GROUPS_PER_FILE = 2;
    private static final int ROWS_PER_ROW_GROUP = 25_000;
    private static final long FILE_ID_STRIDE = 100_000;
    private static final long ROW_GROUP_ID_STRIDE = 50_000;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .configureGpuDistributedExecution()
                .addIcebergProperty("iceberg.experimental.composite-splits.enabled", "true")
                // Small enough for a scan of a composite split to produce multiple pages
                .addIcebergProperty("gpu.scan.max-page-size", "1MB")
                .amendSession(session -> session.setCatalogSessionProperty(ICEBERG_CATALOG, "max_split_size", "100MB"))
                .build();

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data")));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of(
                "hive.security", "allow-all",
                // Share the Iceberg catalog metastore, so that Hive tables can be migrated
                "hive.metastore.catalog.dir", "local:///iceberg"));

        return queryRunner;
    }

    @Test
    void testMigratedHiveTable()
    {
        String tableName = "test_migrated_hive_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.tpch." + tableName + " (id bigint, part_varchar varchar, part_integer integer) " +
                "WITH (format = 'PARQUET', partitioned_by = ARRAY['part_varchar', 'part_integer'])");
        assertUpdate("INSERT INTO hive.tpch." + tableName + " VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)", 3);
        assertUpdate("INSERT INTO hive.tpch." + tableName + " VALUES (4, 'a', 10), (5, 'b', 20), (6, 'c', 30)", 3);
        assertUpdate("INSERT INTO hive.tpch." + tableName + " VALUES (7, 'a', 10), (8, 'b', 20), (9, 'c', 30)", 3);
        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat(query("SELECT id, part_varchar, part_integer FROM " + tableName))
                .executesWithGpu(TableScanNode.class)
                .matches(
                        """
                        VALUES
                            (BIGINT '1', VARCHAR 'a', 10), (BIGINT '2', VARCHAR 'b', 20), (BIGINT '3', VARCHAR 'c', 30),
                            (BIGINT '4', VARCHAR 'a', 10), (BIGINT '5', VARCHAR 'b', 20), (BIGINT '6', VARCHAR 'c', 30),
                            (BIGINT '7', VARCHAR 'a', 10), (BIGINT '8', VARCHAR 'b', 20), (BIGINT '9', VARCHAR 'c', 30)
                        """);

        // No column is read from the data files
        assertThat(query("SELECT part_varchar, part_integer FROM " + tableName))
                .executesWithGpu(TableScanNode.class)
                .matches(
                        """
                        VALUES
                            (VARCHAR 'a', 10), (VARCHAR 'b', 20), (VARCHAR 'c', 30),
                            (VARCHAR 'a', 10), (VARCHAR 'b', 20), (VARCHAR 'c', 30),
                            (VARCHAR 'a', 10), (VARCHAR 'b', 20), (VARCHAR 'c', 30)
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testPagesSpanningSubSplits()
            throws IOException
    {
        String tableName = "test_pages_spanning_sub_splits_" + randomNameSuffix();
        createTableWithoutPartitionColumnsInDataFiles(tableName);

        assertThat(query("SELECT part, part_int, count(*), min(id), max(id), sum(id) FROM " + tableName + " GROUP BY part, part_int"))
                .executesWithGpu(TableScanNode.class)
                .matches(expectedAggregates(_ -> true));

        // No column is read from the data files
        assertThat(query("SELECT part, part_int, count(*) FROM " + tableName + " GROUP BY part, part_int"))
                .executesWithGpu(TableScanNode.class)
                .matches(expectedCounts());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSubSplitsWithAllRowGroupsPruned()
            throws IOException
    {
        String tableName = "test_sub_splits_with_all_row_groups_pruned_" + randomNameSuffix();
        createTableWithoutPartitionColumnsInDataFiles(tableName);

        // Row group statistics prune all row groups of most data files, and one of the two row groups of others
        assertThat(query("SELECT part, part_int, count(*), min(id), max(id), sum(id) FROM " + tableName + " " +
                "WHERE id < 10000 OR id BETWEEN 360000 AND 380000 OR id >= 700000 " +
                "GROUP BY part, part_int"))
                .executesWithGpu(TableScanNode.class)
                .matches(expectedAggregates(id -> id < 10000 || (id >= 360000 && id <= 380000) || id >= 700000));

        assertUpdate("DROP TABLE " + tableName);
    }

    /// Creates a table whose data files do not contain the partition columns, with interleaved partition values.
    private void createTableWithoutPartitionColumnsInDataFiles(String tableName)
            throws IOException
    {
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, part varchar, part_int integer) WITH (partitioning = ARRAY['part', 'part_int'])");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, getHiveMetastore(getQueryRunner()), fileSystemFactory, ICEBERG_CATALOG, "tpch");
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(INT64).id(icebergTable.schema().findField("id").fieldId()).named("id"))
                .named("schema");
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(ImmutableList.of(BIGINT), ImmutableList.of("id"), false, true);
        ParquetWriterOptions writerOptions = ParquetWriterOptions.builder()
                .setMaxRowGroupRowCount(ROWS_PER_ROW_GROUP)
                // Small Parquet pages let the GPU reader end a page anywhere within a data file
                .setMaxPageValueCount(1_000)
                .setBatchSize(1_000)
                .build();

        AppendFiles append = icebergTable.newAppend();
        for (int file = 0; file < FILE_COUNT; file++) {
            Location dataFile = Location.of(icebergTable.location() + "/data/file_" + file + ".parquet");
            try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                    ParquetWriter writer = new ParquetWriter(
                            out,
                            parquetSchema,
                            schemaConverter.getPrimitiveTypes(),
                            writerOptions,
                            SNAPPY,
                            "test-version",
                            Optional.empty(),
                            Optional.empty())) {
                for (int rowGroup = 0; rowGroup < ROW_GROUPS_PER_FILE; rowGroup++) {
                    BlockBuilder ids = BIGINT.createFixedSizeBlockBuilder(ROWS_PER_ROW_GROUP);
                    for (int row = 0; row < ROWS_PER_ROW_GROUP; row++) {
                        BIGINT.writeLong(ids, id(file, rowGroup, row));
                    }
                    writer.write(new Page(ids.build()));
                }
            }
            append.appendFile(DataFiles.builder(icebergTable.spec())
                    .withPath(dataFile.toString())
                    .withFileSizeInBytes(fileSystem.newInputFile(dataFile).length())
                    .withRecordCount((long) ROW_GROUPS_PER_FILE * ROWS_PER_ROW_GROUP)
                    .withFormat(PARQUET)
                    .withPartitionPath("part=%s/part_int=%s".formatted(partition(file), partitionInt(file)))
                    .build());
        }
        append.commit();
    }

    private static String expectedAggregates(LongPredicate filter)
    {
        Map<String, long[]> groups = new TreeMap<>();
        for (int file = 0; file < FILE_COUNT; file++) {
            String key = "VARCHAR '%s', %s".formatted(partition(file), partitionInt(file));
            for (int rowGroup = 0; rowGroup < ROW_GROUPS_PER_FILE; rowGroup++) {
                for (int row = 0; row < ROWS_PER_ROW_GROUP; row++) {
                    long id = id(file, rowGroup, row);
                    if (filter.test(id)) {
                        // count, min, max, sum
                        long[] aggregates = groups.computeIfAbsent(key, _ -> new long[] {0, Long.MAX_VALUE, Long.MIN_VALUE, 0});
                        aggregates[0]++;
                        aggregates[1] = Math.min(aggregates[1], id);
                        aggregates[2] = Math.max(aggregates[2], id);
                        aggregates[3] += id;
                    }
                }
            }
        }
        return groups.entrySet().stream()
                .map(entry -> "(%s, BIGINT '%s', BIGINT '%s', BIGINT '%s', BIGINT '%s')".formatted(
                        entry.getKey(), entry.getValue()[0], entry.getValue()[1], entry.getValue()[2], entry.getValue()[3]))
                .collect(joining(", ", "VALUES ", ""));
    }

    private static String expectedCounts()
    {
        Map<String, Long> counts = new TreeMap<>();
        for (int file = 0; file < FILE_COUNT; file++) {
            counts.merge("VARCHAR '%s', %s".formatted(partition(file), partitionInt(file)), (long) ROW_GROUPS_PER_FILE * ROWS_PER_ROW_GROUP, Long::sum);
        }
        return counts.entrySet().stream()
                .map(entry -> "(%s, BIGINT '%s')".formatted(entry.getKey(), entry.getValue()))
                .collect(joining(", ", "VALUES ", ""));
    }

    private static long id(int file, int rowGroup, int row)
    {
        // Gaps between row groups let a filter on id prune some row groups of a file but not the whole file
        return file * FILE_ID_STRIDE + rowGroup * ROW_GROUP_ID_STRIDE + row;
    }

    private static String partition(int file)
    {
        return PARTITIONS.get(file % PARTITIONS.size());
    }

    private static int partitionInt(int file)
    {
        return file % 2;
    }
}
