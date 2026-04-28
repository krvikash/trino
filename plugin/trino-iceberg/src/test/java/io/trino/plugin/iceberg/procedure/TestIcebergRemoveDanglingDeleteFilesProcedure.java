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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRemoveDanglingDeleteFilesProcedure
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder().build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testPartitionedEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_partitioned_equality_deletes",
                "(id bigint, part varchar) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b')", 4);

            BaseTable icebergTable = loadTable(table.getName());
            // Active — partitions 'a' and 'b' have data
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Object[] {
                    "a"})), ImmutableMap.of("id", 1L), Optional.empty());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Object[] {
                    "b"})), ImmutableMap.of("id", 3L), Optional.empty());
            // Dangling — partition 'c' has no data files
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Object[] {
                    "c"})), ImmutableMap.of("id", 1L), Optional.empty());
            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(3);
            // Dangling delete in partition 'c' has no effect; active deletes in 'a' and 'b' remove id=1 and id=3
            assertThat(query("SELECT id, part FROM " + table.getName()))
                    .matches("VALUES (BIGINT '2', VARCHAR 'a'), (4, 'b')");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 1),
                            ('dangling_position_delete_files_count', 0),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");

            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(2);
            assertThat(query("SELECT id, part FROM " + table.getName()))
                    .matches("VALUES (BIGINT '2', VARCHAR 'a'), (4, 'b')");
        }
    }

    @Test
    void testDanglingEqualityDeleteRemoved()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_dangling_equality_delete",
                "(nationkey bigint, name varchar, regionkey bigint, comment varchar)")) {
            BaseTable icebergTable = loadTable(table.getName());

            // Equality deletes written before data have lower sequence numbers and are dangling
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("regionkey", 1L), Optional.empty());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("regionkey", 1L), Optional.empty());
            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM tpch.tiny.nation", 25);
            // Dangling deletes have lower sequence numbers, so all 25 rows are visible
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '25'");

            // OPTIMIZE cannot remove these dangling deletes
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");
            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(2);

            // Write one active equality delete after data
            icebergTable = loadTable(table.getName());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("regionkey", 2L), Optional.empty());
            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(3);
            // Active delete removes regionkey=2 rows
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.nation WHERE regionkey != 2");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 2),
                            ('dangling_equality_delete_files_count', 2),
                            ('dangling_position_delete_files_count', 0),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");

            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.nation WHERE regionkey != 2");
        }
    }

    @Test
    void testGlobalPositionDelete()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_global_position_delete",
                "(nationkey bigint, name varchar, regionkey bigint, comment varchar)")) {
            // Dangling — written before data has lower sequence number
            BaseTable icebergTable = loadTable(table.getName());
            writePositionDeleteForTable(icebergTable, PartitionSpec.unpartitioned(), null, "local:///placeholder.parquet", 0);

            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM tpch.tiny.nation", 25);

            // Active — written after data has higher sequence number
            icebergTable = loadTable(table.getName());
            String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + table.getName() + "$files\" WHERE content = 0 LIMIT 1").getOnlyValue();
            writePositionDeleteForTable(icebergTable, PartitionSpec.unpartitioned(), null, dataFilePath, 0);
            assertThat(positionDeleteFileCount(table.getName())).isEqualTo(2);
            // Active position delete removes one row; dangling one (lower seq) has no effect
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '24'");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 0),
                            ('dangling_position_delete_files_count', 1),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");
            assertThat(positionDeleteFileCount(table.getName())).isEqualTo(1);
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '24'");
        }
    }

    @Test
    void testPartitionedPositionDelete()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_partitioned_position_delete",
                "(id bigint, part varchar) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'a'), (3, 'b')", 3);

            BaseTable icebergTable = loadTable(table.getName());
            String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + table.getName() + "$files\" WHERE content = 0 LIMIT 1").getOnlyValue();
            // Active — partition 'a' has data and delete has higher seq
            writePositionDeleteForTable(icebergTable, icebergTable.spec(), new PartitionData(new Object[] {"a"}), dataFilePath, 0);
            // Dangling — partition 'c' has no data files
            writePositionDeleteForTable(icebergTable, icebergTable.spec(), new PartitionData(new Object[] {"c"}), "local:///placeholder.parquet", 0);
            assertThat(positionDeleteFileCount(table.getName())).isEqualTo(2);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 0),
                            ('dangling_position_delete_files_count', 1),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");
            assertThat(positionDeleteFileCount(table.getName())).isEqualTo(1);
        }
    }

    @Test
    void testFileReferencedPositionDelete()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_file_referenced_position_delete",
                "(nationkey bigint, name varchar, regionkey bigint, comment varchar)")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM tpch.tiny.nation", 25);

            BaseTable icebergTable = loadTable(table.getName());
            String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + table.getName() + "$files\" WHERE content = 0 LIMIT 1").getOnlyValue();
            // Active — referencedDataFile points to existing data file
            writeFileReferencedPositionDelete(icebergTable, dataFilePath);
            // Dangling — referencedDataFile points to non-existent data file
            icebergTable = loadTable(table.getName());
            writeFileReferencedPositionDelete(icebergTable, "local:///nonexistent_data_file.parquet");
            assertThat(positionDeleteFileCount(table.getName())).isEqualTo(2);
            // Active position delete removes one row; dangling one references non-existent file
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '24'");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 0),
                            ('dangling_position_delete_files_count', 1),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");
            assertThat(positionDeleteFileCount(table.getName())).isEqualTo(1);
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '24'");
        }
    }

    @Test
    void testDeletionVectorActiveAndDangling()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_deletion_vector",
                "(id integer) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3, 4, 5", 5);

            BaseTable icebergTable = loadTable(table.getName());
            String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + table.getName() + "$files\" LIMIT 1").getOnlyValue();
            writeDeletionVector(icebergTable, dataFilePath, 0L);

            // DV referencing a non-existent data file — immediately dangling
            icebergTable = loadTable(table.getName());
            writeDeletionVector(icebergTable, "local:///nonexistent_data_file.parquet", 0L);
            assertThat(deleteFileCount(table.getName())).isEqualTo(2);
            // Active DV removes one row; dangling DV references non-existent file
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '4'");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 0),
                            ('dangling_position_delete_files_count', 0),
                            ('dangling_dv_files_count', 1),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");
            assertThat(deleteFileCount(table.getName())).isEqualTo(1);
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '4'");
        }
    }

    @Test
    void testPositionDeletesDanglingWhenDvExists()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_position_deletes_with_deletion_vector",
                "(id integer) WITH (format_version = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3, 4, 5", 5);

            BaseTable icebergTable = loadTable(table.getName());
            String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + table.getName() + "$files\" WHERE content = 0 LIMIT 1").getOnlyValue();
            // Two file-scoped position deletes referencing the same data file
            writeFileReferencedPositionDelete(icebergTable, dataFilePath);
            icebergTable = loadTable(table.getName());
            writeFileReferencedPositionDelete(icebergTable, dataFilePath);

            // Upgrade to V3 and write a DV for the same data file — per spec, this subsumes both position deletes
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");
            icebergTable = loadTable(table.getName());
            writeDeletionVector(icebergTable, dataFilePath, 0L);
            assertThat(deleteFileCount(table.getName())).isEqualTo(3);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 2),
                            ('dangling_equality_delete_files_count', 0),
                            ('dangling_position_delete_files_count', 2),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");

            assertThat(deleteFileCount(table.getName())).isEqualTo(1);
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '4'");
        }
    }

    @Test
    void testPartitionEvolution()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_partition_evolution",
                "(id bigint, part varchar) WITH (partitioning = ARRAY['part'])")) {
            // Insert data under spec 0 (identity(part))
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'b')", 2);

            // Write an equality delete for partition 'a' under spec 0 — this is active
            BaseTable icebergTable = loadTable(table.getName());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Object[] {
                    "a"})), ImmutableMap.of("id", 1L), Optional.empty());

            // Evolve partition spec and insert data under new spec
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['truncate(part, 1)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'c')", 1);

            // Write an equality delete for partition 'c' under spec 1 — dangling since it targets a partition
            // where no data exists under spec 1 with lower sequence number (the data was just inserted)
            icebergTable = loadTable(table.getName());
            PartitionSpec newSpec = icebergTable.spec();
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.of(newSpec), Optional.of(new PartitionData(new Object[] {
                    "a"})), ImmutableMap.of("id", 99L), Optional.empty());

            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(2);

            // The delete under spec 1 for partition 'a' is dangling because there are no data files
            // under spec 1 with partition 'a'. Without spec_id in the key, data from spec 0 partition 'a'
            // would incorrectly prevent this delete from being identified as dangling.
            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 1),
                            ('dangling_position_delete_files_count', 0),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");
            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(1);
        }
    }

    @Test
    void testOptimizeAllPartitionsWithGlobalEqualityDelete()
            throws Exception
    {
        // When OPTIMIZE targets only a subset of partitions, a global (unpartitioned-spec) delete
        // file cannot be safely removed because it may still apply to non-optimized partitions.
        // However, rewritten data files get a sequence number >= the delete file's,
        // making the delete effectively dangling for those partitions.
        try (TestTable testTable = newTrinoTable("test_optimize_partition_keeps_global_delete_",
                "(id bigint, part varchar)")) {
            String tableName = testTable.getName();

            // 3 rows per partition, 3 partitions = 9 rows total
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'a'), (3, 'a')", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'b'), (2, 'b'), (3, 'b')", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'c'), (2, 'c'), (3, 'c')", 3);

            Table icebergTable = loadTable(tableName);
            PartitionSpec unpartitionedSpec = icebergTable.spec();
            assertThat(unpartitionedSpec.isUnpartitioned()).isTrue();

            // Evolve to partitioned by 'part'
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['part']");

            // Rewrite data files into partitioned layout
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

            icebergTable.refresh();

            // Global equality delete using the old unpartitioned spec targeting id=1
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory,
                    Optional.of(unpartitionedSpec), Optional.empty(),
                    ImmutableMap.of("id", 1L), Optional.empty());

            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 6"); // 9 - 3 deleted (id=1 from each partition)

            // Capture the equality delete file's sequence number before optimize
            long deleteFileSequenceNumber = (long) computeScalar(
                    "SELECT sequence_number FROM \"" + tableName + "$entries\" WHERE data_file.content = " + EQUALITY_DELETES.id());

            // OPTIMIZE only part='a' — global delete file must NOT be removed
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 'a'");

            // Global delete file should persist — it still applies to non-optimized partitions 'b' and 'c'
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");

            // OPTIMIZE part='b' and 'c' — global delete file must NOT be removed
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 'b'");
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 'c'");

            // Global delete file should persist — it still applies as table wasn't optimized whole
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");

            // The rewritten data files have a sequence number >= the delete's, so the (dangling) delete no longer applies to it
            long rewrittenDataFileSequenceNumberPartA = (long) computeScalar(
                    "SELECT sequence_number FROM \"" + tableName + "$entries\" WHERE data_file.content = 0 AND status = 1 AND data_file.file_path LIKE '%part=a%'");
            assertThat(rewrittenDataFileSequenceNumberPartA).isGreaterThanOrEqualTo(deleteFileSequenceNumber);
            long rewrittenDataFileSequenceNumberPartB = (long) computeScalar(
                    "SELECT sequence_number FROM \"" + tableName + "$entries\" WHERE data_file.content = 0 AND status = 1 AND data_file.file_path LIKE '%part=b%'");
            assertThat(rewrittenDataFileSequenceNumberPartB).isGreaterThanOrEqualTo(deleteFileSequenceNumber);
            long rewrittenDataFileSequenceNumberPartC = (long) computeScalar(
                    "SELECT sequence_number FROM \"" + tableName + "$entries\" WHERE data_file.content = 0 AND status = 1 AND data_file.file_path LIKE '%part=c%'");
            assertThat(rewrittenDataFileSequenceNumberPartC).isGreaterThanOrEqualTo(deleteFileSequenceNumber);

            // Data correctness — id=1 still deleted from all partitions
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE id = 1", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 6");

            // remove_dangling_delete_files should clean up dangling global delete file
            assertUpdate(
                    "ALTER TABLE " + tableName + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 1),
                            ('dangling_equality_delete_files_count', 1),
                            ('dangling_position_delete_files_count', 0),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");

            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 6");
            assertQuery("SELECT id, part FROM " + tableName + " ORDER BY part, id",
                    "VALUES (2, 'a'), (3, 'a'), (2, 'b'), (3, 'b'), (2, 'c'), (3, 'c')");
        }
    }

    @Test
    void testEmptyTable()
    {
        try (TestTable table = newTrinoTable("test_empty_table", "(id bigint)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files");
            assertThat(deleteFileCount(table.getName())).isEqualTo(0);
        }
    }

    @Test
    void testCorruptPartitionDataTypeMismatch()
            throws Exception
    {
        // Regression test: manifests with wrong partition data types (e.g., String in Integer field)
        // cause StructLikeWrapper.equals() returning false
        try (TestTable table = newTrinoTable("test_corrupt_partition_types",
                "(id bigint, part integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 20)", 1);

            BaseTable icebergTable = loadTable(table.getName());

            // Active equality delete for partition 10 (has data)
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory,
                    Optional.of(icebergTable.spec()),
                    Optional.of(new PartitionData(new Object[] {10})),
                    ImmutableMap.of("id", 1L),
                    Optional.empty());
            icebergTable = loadTable(table.getName());
            // Dangling equality delete for partition 30 (no data)
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory,
                    Optional.of(icebergTable.spec()),
                    Optional.of(new PartitionData(new Object[] {30})),
                    ImmutableMap.of("id", 99L),
                    Optional.empty());

            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(2);

            // Replace real data manifests with two corrupt manifests (one entry each, same
            // corrupt partition value). Two separate manifests ensure merge() is exercised
            icebergTable = loadTable(table.getName());
            replaceDataManifestsWithCorrupt(icebergTable);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files",
                    """
                            VALUES
                            ('removed_delete_files_count', 2),
                            ('dangling_equality_delete_files_count', 2),
                            ('dangling_position_delete_files_count', 0),
                            ('dangling_dv_files_count', 0),
                            ('data_files_without_sequence_numbers', 0),
                            ('unexpected_delete_files_count', 0)""");

            // Both delete files must be deleted
            assertThat(equalityDeleteFileCount(table.getName())).isEqualTo(0);
        }
    }

    private void replaceDataManifestsWithCorrupt(BaseTable icebergTable)
            throws IOException
    {
        List<ManifestFile> dataManifests = icebergTable.currentSnapshot().dataManifests(icebergTable.io());

        // Split entries across two manifests
        // Both use the same corrupt value so they collide during merge().
        ManifestFile firstCorruptManifest = writeCorruptDataManifest(icebergTable);
        ManifestFile secondCorruptManifest = writeCorruptDataManifest(icebergTable);

        RewriteManifests rewrite = icebergTable.rewriteManifests();
        dataManifests.forEach(rewrite::deleteManifest);
        rewrite.addManifest(firstCorruptManifest)
                .addManifest(secondCorruptManifest)
                .commit();
    }

    private ManifestFile writeCorruptDataManifest(BaseTable icebergTable)
            throws IOException
    {
        Schema icebergTableSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "part", Types.StringType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(icebergTableSchema)
                .identity("part")
                .build();

        long snapshotId = icebergTable.currentSnapshot().snapshotId();

        try (FileIO fileIo = FILE_IO_FACTORY.create(fileSystemFactory.create(SESSION))) {
            OutputFile manifestOutput = fileIo.newOutputFile("local:///corrupt_manifest_" + UUID.randomUUID() + ".avro");
            // snapshotId assigned during commit
            ManifestWriter<DataFile> manifestWriter = ManifestFiles.write(
                    2, partitionSpec, manifestOutput, null);

            org.apache.iceberg.PartitionData corruptPartition = new org.apache.iceberg.PartitionData(partitionSpec.partitionType());
            // setting string value for int partition column
            corruptPartition.set(0, "corrupt_value");

            DataFile corruptDataFile = DataFiles.builder(partitionSpec)
                    .withPath("local:///fake_data_" + UUID.randomUUID() + ".parquet")
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(corruptPartition)
                    .withFileSizeInBytes(100)
                    .withRecordCount(1)
                    .build();
            manifestWriter.existing(corruptDataFile, snapshotId, 1L, 1L);
            manifestWriter.close();

            return manifestWriter.toManifestFile();
        }
    }

    @Test
    void testWhereNotSupported()
    {
        try (TestTable table = newTrinoTable("test_where_rejected", "WITH (partitioning = ARRAY['part']) AS SELECT 1 id, 1 part")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files WHERE id = 1",
                    ".* WHERE not supported for procedure REMOVE_DANGLING_DELETE_FILES");
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE remove_dangling_delete_files WHERE part = 10",
                    ".* WHERE not supported for procedure REMOVE_DANGLING_DELETE_FILES");
        }
    }

    private void writePositionDeleteForTable(BaseTable icebergTable, PartitionSpec spec, PartitionData partitionData, String dataFilePath, long position)
            throws IOException
    {
        try (FileIO fileIo = FILE_IO_FACTORY.create(fileSystemFactory.create(SESSION))) {
            Parquet.DeleteWriteBuilder builder = Parquet.writeDeletes(fileIo.newOutputFile("local:///delete_file_" + UUID.randomUUID()))
                    .createWriterFunc(GenericParquetWriter::create)
                    .forTable(icebergTable)
                    .overwrite()
                    .rowSchema(null)
                    .withSpec(spec);
            if (partitionData != null) {
                builder.withPartition(partitionData);
            }
            PositionDeleteWriter<Void> writer = builder.buildPositionWriter();

            PositionDelete<Void> positionDelete = PositionDelete.create();
            positionDelete.set(dataFilePath, position);
            try (Closeable _ = writer) {
                writer.write(positionDelete);
            }

            icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        }
    }

    private void writeFileReferencedPositionDelete(BaseTable icebergTable, String referencedDataFile)
            throws IOException
    {
        String deletePath = "local:///delete_file_" + UUID.randomUUID();
        try (FileIO fileIo = FILE_IO_FACTORY.create(fileSystemFactory.create(SESSION))) {
            PositionDeleteWriter<Record> writer = Parquet.writeDeletes(fileIo.newOutputFile(deletePath))
                    .createWriterFunc(GenericParquetWriter::create)
                    .forTable(icebergTable)
                    .overwrite()
                    .rowSchema(icebergTable.schema())
                    .withSpec(PartitionSpec.unpartitioned())
                    .buildPositionWriter();

            PositionDelete<Record> positionDelete = PositionDelete.create();
            positionDelete.set(referencedDataFile, 0, GenericRecord.create(icebergTable.schema()));
            try (Closeable _ = writer) {
                writer.write(positionDelete);
            }

            DeleteFile deleteFileWithRef = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withPath(deletePath)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(writer.toDeleteFile().fileSizeInBytes())
                    .withRecordCount(1)
                    .withReferencedDataFile(referencedDataFile)
                    .build();

            icebergTable.newRowDelta().addDeletes(deleteFileWithRef).commit();
        }
    }

    private void writeDeletionVector(BaseTable icebergTable, String dataFilePath, long position)
            throws IOException
    {
        try (DVFileWriter dvWriter = new BaseDVFileWriter(
                OutputFileFactory.builderFor(icebergTable, 1, 1).format(FileFormat.PUFFIN).build(),
                _ -> PositionDeleteIndex.empty())) {
            dvWriter.delete(dataFilePath, position, icebergTable.spec(), null);
            dvWriter.close();

            icebergTable.newRowDelta()
                    .addDeletes(getOnlyElement(dvWriter.result().deleteFiles()))
                    .commit();
        }
    }

    private long deleteFileCount(String tableName)
    {
        return equalityDeleteFileCount(tableName) + positionDeleteFileCount(tableName);
    }

    private long equalityDeleteFileCount(String tableName)
    {
        return (long) computeActual("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id()).getOnlyValue();
    }

    private long positionDeleteFileCount(String tableName)
    {
        return (long) computeActual("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id()).getOnlyValue();
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
