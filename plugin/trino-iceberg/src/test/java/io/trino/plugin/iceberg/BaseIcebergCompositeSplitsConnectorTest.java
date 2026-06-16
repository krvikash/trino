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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.operator.OperatorStats;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.spi.QueryId;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.plugin.iceberg.IcebergMetadata.toCompressionCodecTableProperty;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergTableProperties.isCompressionCodecSupportedForFormat;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.getCompressionPropertyName;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseIcebergCompositeSplitsConnectorTest
        extends BaseIcebergParquetConnectorTest
{
    BaseIcebergCompositeSplitsConnectorTest()
    {
        super(2);
    }

    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return super.createQueryRunnerBuilder()
                .addIcebergProperty("iceberg.experimental.composite-splits.enabled", "true")
                .amendSession(session -> session
                        .setCatalogSessionProperty("iceberg", "max_split_size", "100MB")
                        .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "20"));
    }

    @Test
    @Override
    public void testRepartitionDataOnCtas()
    {
        // identity partitioning column
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", true, "'orderstatus'", 3);
        // bucketing
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", true, "'bucket(custkey, 13)'", 13);
        // varchar-based
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", true, "'truncate(comment, 1)'", 35);
        // complex; would exceed 100 open writers limit in IcebergPageSink without write repartitioning
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", true, "'bucket(custkey, 4)', 'truncate(comment, 1)'", 131);
        // same column multiple times
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", true, "'truncate(comment, 1)', 'orderstatus', 'bucket(comment, 2)'", 180);
    }

    @Test
    @Override
    public void testRepartitionDataOnInsert()
    {
        // identity partitioning column
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", false, "'orderstatus'", 3);
        // bucketing
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", false, "'bucket(custkey, 13)'", 13);
        // varchar-based
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", false, "'truncate(comment, 1)'", 35);
        // complex; would exceed 100 open writers limit in IcebergPageSink without write repartitioning
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", false, "'bucket(custkey, 4)', 'truncate(comment, 1)'", 131);
        // same column multiple times
        testRepartitionData(getFileCountSensitiveWriterSession(), "tpch.tiny.orders", false, "'truncate(comment, 1)', 'orderstatus', 'bucket(comment, 2)'", 180);
    }

    @Override
    protected void testStatsBasedRepartitionData(boolean ctas)
    {
        String catalog = getFileCountSensitiveWriterSession().getCatalog().orElseThrow();
        try (TestTable sourceTable = new TestTable(
                sql -> assertQuerySucceeds(
                        Session.builder(getFileCountSensitiveWriterSession())
                                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, "true")
                                .build(),
                        sql),
                "temp_table_analyzed",
                "AS SELECT orderkey, custkey, orderstatus FROM tpch.\"sf0.03\".orders")) {
            Session sessionRepartitionMany = Session.builder(getFileCountSensitiveWriterSession())
                    .setSystemProperty(SCALE_WRITERS, "false")
                    .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                    .build();
            // Use DISTINCT to add data redistribution between source table and the writer. This makes it more likely that all writers get some data.
            String sourceRelation = "(SELECT DISTINCT orderkey, custkey, orderstatus FROM " + sourceTable.getName() + ")";
            testRepartitionData(
                    getFileCountSensitiveWriterSession(),
                    sourceRelation,
                    ctas,
                    "'orderstatus'",
                    3);
            // Test uses relatively small table (45K rows). When engine doesn't redistribute data for writes,
            // occasionally a worker node doesn't get any data and fewer files get created.
            assertEventually(new Duration(3, MINUTES), () -> {
                testRepartitionData(
                        sessionRepartitionMany,
                        sourceRelation,
                        ctas,
                        "'orderstatus'",
                        9);
            });
        }
    }

    @Override
    protected void verifySplitCount(QueryId queryId, long expectedSplitCount)
    {
        if (expectedSplitCount > 0) {
            OperatorStats operatorStats = getOperatorStats(queryId);
            assertThat(operatorStats.getTotalDrivers()).isLessThanOrEqualTo(expectedSplitCount);
            assertThat(operatorStats.getPhysicalInputPositions()).isGreaterThan(0);
            assertThat(operatorStats.getPhysicalInputReadTime().getValue()).isGreaterThan(0);
        }
        else {
            super.verifySplitCount(queryId, expectedSplitCount);
        }
    }

    @Test
    @Override
    public void testOptimizeTableAfterDeleteWithFormatVersion2()
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate(getFileCountSensitiveWriterSession(), "CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);

        List<String> initialFiles = getActiveFiles(tableName);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        // Verify that delete files exists
        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '1'");

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        assertUpdate(
                withSingleWriterPerTask(getFileCountSensitiveWriterSession()),
                "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE",
                "VALUES ('rewritten_data_files_count', 1), ('removed_delete_files_count', 1), ('added_data_files_count', 1)");

        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles)
                .hasSize(1)
                .isNotEqualTo(initialFiles);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testOptimizeFilesDoNotInheritSequenceNumber()
            throws IOException
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate(getFileCountSensitiveWriterSession(), "CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        // Verify that delete file exists
        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '1'");

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getFileCountSensitiveWriterSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        List<IcebergEntry> activeEntries = getIcebergEntries(tableName);
        assertThat(activeEntries).hasSize(3);

        // New rewritten data file should not inherit sequence number as it is a rewrite
        assertThat(activeEntries.stream().filter(entry -> entry.status() == 1))
                .hasSize(1)
                .allMatch(entry -> entry.sequenceNumber() == 2 && entry.fileSequenceNumber() == 3);

        // Other files should inherit sequence number
        assertThat(activeEntries.stream().filter(entry -> entry.status() == 2))
                .hasSize(2)
                .allMatch(entry -> entry.sequenceNumber().equals(entry.fileSequenceNumber()));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    void testOptimizeAfterChangeInPartitioning()
    {
        String tableName = "test_optimize_after_change_in_partitioning_" + randomNameSuffix();
        assertUpdate(getFileCountSensitiveWriterSession(), "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['bucket(nationkey, 5)']) AS SELECT * FROM tpch.tiny.supplier", 100);
        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles).hasSize(5);

        // OPTIMIZE shouldn't have to rewrite files
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT COUNT(*) FROM " + tableName)).matches("VALUES BIGINT '100'");
        assertThat(getActiveFiles(tableName))
                .containsExactlyInAnyOrderElementsOf(initialFiles);

        // Change in partitioning should result in OPTIMIZE rewriting all files
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['nationkey']");
        computeActual(getFileCountSensitiveWriterSession(), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT COUNT(*) FROM " + tableName)).matches("VALUES BIGINT '100'");
        List<String> filesAfterPartioningChange = getActiveFiles(tableName);
        assertThat(filesAfterPartioningChange)
                .hasSize(25)
                .doesNotContainAnyElementsOf(initialFiles);

        // OPTIMIZE shouldn't have to rewrite files anymore
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT COUNT(*) FROM " + tableName)).matches("VALUES BIGINT '100'");
        assertThat(getActiveFiles(tableName))
                .hasSize(25)
                .containsExactlyInAnyOrderElementsOf(filesAfterPartioningChange);
    }

    @Test
    @Override
    public void testTargetMaxFileSize()
    {
        String tableName = "test_default_max_file_size" + randomNameSuffix();
        @Language("SQL") String createTableSql = format("CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem LIMIT 200000", tableName);

        Session session = Session.builder(getFileCountSensitiveWriterSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
        assertUpdate(session, createTableSql, 200000);
        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles.size()).isLessThanOrEqualTo(3);
        assertUpdate(format("DROP TABLE %s", tableName));

        DataSize maxSize = DataSize.of(80, DataSize.Unit.KILOBYTE);
        session = Session.builder(getFileCountSensitiveWriterSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();

        assertUpdate(session, format("CREATE TABLE %s WITH (target_max_file_size = '%s') AS SELECT * FROM tpch.sf1.lineitem LIMIT 200000", tableName, maxSize), 200000);
        assertThat(query(format("SELECT count(*) FROM %s", tableName))).matches("VALUES BIGINT '200000'");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles.size()).isGreaterThan(10);

        computeActual(format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName))
                .getMaterializedRows()
                // as target_max_file_size is set to quite low value it can happen that created files are bigger,
                // so just to be safe we check if it is not much bigger
                .forEach(row -> assertThat((Long) row.getField(0)).isBetween(1L, maxSize.toBytes() * 6));
    }

    @Test
    @Override
    public void testTargetMaxFileSizeOnSortedTable()
    {
        String tableName = "test_default_max_file_size_sorted_" + randomNameSuffix();
        @Language("SQL") String createTableSql = format("CREATE TABLE %s WITH (sorted_by = ARRAY['shipdate']) AS SELECT * FROM tpch.sf1.lineitem LIMIT 200000", tableName);

        Session session = Session.builder(getFileCountSensitiveWriterSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
        assertUpdate(session, createTableSql, 200000);
        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles.size()).isLessThanOrEqualTo(3);
        assertUpdate(format("DROP TABLE %s", tableName));

        DataSize maxSize = DataSize.of(50, DataSize.Unit.KILOBYTE);
        session = Session.builder(getFileCountSensitiveWriterSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();

        assertUpdate(session, format("CREATE TABLE %s WITH (sorted_by = ARRAY['shipdate'], target_max_file_size = '%s') AS SELECT * FROM tpch.sf1.lineitem LIMIT 200000", tableName, maxSize), 200000);
        assertThat(query(format("SELECT count(*) FROM %s", tableName))).matches("VALUES BIGINT '200000'");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles.size()).isGreaterThan(5);

        computeActual(format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName))
                .getMaterializedRows()
                // as target_max_file_size is set to quite low value it can happen that created files are bigger,
                // so just to be safe we check if it is not much bigger
                .forEach(row -> assertThat((Long) row.getField(0)).isBetween(1L, maxSize.toBytes() * 20));
    }

    @Test
    @Override
    public void testCreateTableAsWithCompressionCodecs()
    {
        String compressionProperty = getCompressionPropertyName(format);

        for (HiveCompressionCodec compressionCodec : getCompressionCodecs(Optional.empty())) {
            String tableName = format("test_ctas_%s_codec_%s_%s", compressionCodec.name(), format, randomNameSuffix());
            if (isCompressionCodecSupportedForFormat(format, compressionCodec)) {
                assertUpdate(
                        getFileCountSensitiveWriterSession(),
                        format("CREATE TABLE %s WITH (format = '%s', compression_codec = '%s') AS SELECT * FROM nation", tableName, format, compressionCodec.name()),
                        "SELECT count(*) FROM nation");

                assertThat(getTableProperties(tableName))
                        .containsEntry(DEFAULT_FILE_FORMAT, format.toString())
                        .containsEntry(compressionProperty, toCompressionCodecTableProperty(format, compressionCodec));

                assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM nation");
                assertThat(query(format("SELECT count(*) FROM \"%s$files\" WHERE file_path LIKE '%%.%s'", tableName, format.name().toLowerCase(ENGLISH))))
                        .matches("SELECT BIGINT '1'");

                assertUpdate(
                        getFileCountSensitiveWriterSession(),
                        "INSERT INTO " + tableName + " SELECT * FROM nation WHERE nationkey >= 10",
                        "SELECT count(*) FROM nation WHERE nationkey >= 10");

                assertUpdate("DROP TABLE " + tableName);
            }
            else {
                assertQueryFails(
                        getFileCountSensitiveWriterSession(),
                        format("CREATE TABLE %s WITH (format = '%s', compression_codec = '%s') AS SELECT * FROM nation", tableName, format, compressionCodec.name()),
                        "Compression codec LZ4 not supported for .*");
            }
        }
    }

    @Test
    @Override
    void testTableChangesOnMultiRowGroups()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_table_changes_function_multi_row_groups_",
                "WITH (parquet_writer_row_group_size = '" + getTableChangesParquetRowGroupSize() + "') AS SELECT orderkey, partkey, suppkey FROM tpch.tiny.lineitem WITH NO DATA")) {
            long initialSnapshot = getMostRecentSnapshotId(table.getName());
            assertUpdate(
                    getFileCountSensitiveWriterSession(),
                    "INSERT INTO %s SELECT orderkey, partkey, suppkey FROM tpch.tiny.lineitem".formatted(table.getName()),
                    60175L);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());
            DateTimeFormatter instantMillisFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSVV").withZone(UTC);
            String snapshotAfterInsertTime = getSnapshotTime(table.getName(), snapshotAfterInsert).format(instantMillisFormatter);

            // make sure splits are processed in more than one batch
            // Decrease parquet row groups size or add more columns if this test fails
            String filePath = getOnlyTableFilePath(table.getName());
            ParquetMetadata parquetMetadata = getParquetFileMetadata(fileSystem.newInputFile(Location.of(filePath)));
            int blocksSize = parquetMetadata.getBlocks().size();
            int splitBatchSize = getTableChangesSplitBatchSize();
            assertThat(blocksSize).isGreaterThan(splitBatchSize);

            assertQuery(
                    """
                    SELECT orderkey, partkey, suppkey, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal
                    FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))
                    """.formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT orderkey, partkey, suppkey, 'insert', %s, '%s', 0 FROM lineitem".formatted(snapshotAfterInsert, snapshotAfterInsertTime));
        }
    }

    @Test
    void testMergeWithBucketPartitioning()
    {
        String target = "test_merge_bucket_target_" + randomNameSuffix();
        String source = "test_merge_bucket_source_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + target + " (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['bucket(customer, 3)'])");
        assertUpdate("INSERT INTO " + target + " VALUES ('Aaron', 5, 'Antioch')", 1);
        assertUpdate("INSERT INTO " + target + " VALUES ('Bill', 7, 'Buena')", 1);
        assertUpdate("INSERT INTO " + target + " VALUES ('Carol', 3, 'Cambridge')", 1);
        assertUpdate("INSERT INTO " + target + " VALUES ('Dave', 11, 'Devon')", 1);

        assertUpdate("CREATE TABLE " + source + " (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['bucket(customer, 3)'])");
        assertUpdate("INSERT INTO " + source + " VALUES ('Aaron', 6, 'Arches')", 1);
        assertUpdate("INSERT INTO " + source + " VALUES ('Ed', 7, 'Etherville')", 1);
        assertUpdate("INSERT INTO " + source + " VALUES ('Carol', 9, 'Centreville')", 1);
        assertUpdate("INSERT INTO " + source + " VALUES ('Dave', 11, 'Darbyshire')", 1);

        assertUpdate(
                "MERGE INTO " + target + " t USING " + source + " s ON (t.customer = s.customer)" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                4);

        assertQuery(
                "SELECT * FROM " + target,
                "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + source);
        assertUpdate("DROP TABLE " + target);
    }

    private Session getFileCountSensitiveWriterSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "10000")
                .build();
    }
}
