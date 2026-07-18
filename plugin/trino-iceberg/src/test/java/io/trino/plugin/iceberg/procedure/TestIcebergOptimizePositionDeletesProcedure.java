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
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TrinoGenericFileWriterFactory;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static org.apache.iceberg.GenericDataFiles.setDataSequenceNumber;
import static org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput;
import static org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergOptimizePositionDeletesProcedure
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

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizePositionDeletes(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_optimize_position_deletes", "WITH (format='" + format + "') AS SELECT * FROM tpch.tiny.region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 0", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 1", 1);

            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(2);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 2), " +
                            "('added_delete_files_count', 1)");
            assertThat(positionDeleteFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(deleteFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT * FROM " + table.getName() + " WHERE regionkey NOT IN (0, 1)");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizePositionDeletesWithPartitionTable(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_partition", "(id int, part varchar) WITH (partitioning = ARRAY['part'], format = '" + format + "')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'a'), (3, 'a')", 3);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'b'), (5, 'b'), (6, 'b')", 3);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 4", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 5", 1);

            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(4);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 4), " +
                            "('added_delete_files_count', 2)");
            assertThat(positionDeleteFiles(table.getName()))
                    .hasSize(2)
                    .doesNotContainAnyElementsOf(deleteFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (3, VARCHAR 'a'), (6, 'b')");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizePositionDeletesWithPartitionEvolution(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_partition", "(id int, part varchar) WITH (partitioning = ARRAY['part'], format = '" + format + "')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'a'), (3, 'a')", 3);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'b'), (5, 'b'), (6, 'b')", 3);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 4", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 5", 1);

            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(4);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY[]");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (3, VARCHAR 'a'), (6, 'b')");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 4), " +
                            "('added_delete_files_count', 2)");
            assertThat(positionDeleteFiles(table.getName()))
                    .hasSize(2)
                    .doesNotContainAnyElementsOf(deleteFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (3, VARCHAR 'a'), (6, 'b')");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizeSinglePositionDeletes(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_optimize_position_deletes", "WITH (format = '" + format + "') AS SELECT * FROM tpch.tiny.region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 0", 1);

            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(1);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 0), " +
                            "('added_delete_files_count', 0)");
            assertThat(positionDeleteFiles(table.getName()))
                    .hasSize(1)
                    .isEqualTo(deleteFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT * FROM " + table.getName() + " WHERE regionkey != 0");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testEmptyPositionDeletes(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_no_rewrite", "(x int) WITH (format = '" + format + "')")) {
            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).isEmpty();

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 0), " +
                            "('added_delete_files_count', 0)");
            assertThat(positionDeleteFiles(table.getName())).isEmpty();

            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUnsupportedDanglingEqualityDeletes(IcebergFileFormat format)
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_dangling_eq_deletes", "(x int, part varchar) WITH (partitioning = ARRAY['part'], format = '" + format + "')")) {
            BaseTable icebergTable = loadTable(table.getName());
            String tableLocation = icebergTable.location();

            DataFile dataFile = dataFile(icebergTable, "data", "part=a");
            dataFile = setDataSequenceNumber(dataFile, 2L);
            icebergTable.newAppend().appendFile(dataFile).commit();

            DeleteFile danglingDeleteFile = writeEqualityDelete(icebergTable, tableLocation + "/part=a/eq-deletes", "a", Map.of("part", "a"));
            danglingDeleteFile = setDataSequenceNumber(danglingDeleteFile, 1L);

            // "Dangling" equality delete files mean equality delete files with a data sequence number less than or equal to that of any data file in the same partition
            assertThat(danglingDeleteFile.dataSequenceNumber()).isLessThanOrEqualTo(dataFile.dataSequenceNumber());
            assertThat(danglingDeleteFile.partition()).isEqualTo(dataFile.partition());

            icebergTable.newRowDelta()
                    .addRows(dataFile)
                    .addDeletes(danglingDeleteFile)
                    .commit();

            Set<String> deleteFiles = equalityDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(1);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 0), " +
                            "('added_delete_files_count', 0)");

            // optimize_position_deletes doesn't remove dangling equality deletes
            assertThat(equalityDeleteFiles(table.getName())).hasSize(1);
        }
    }

    @Test
    void testEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_equality_deletes", "AS SELECT * FROM tpch.tiny.nation")) {
            BaseTable icebergTable = loadTable(table.getName());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("regionkey", 1L), Optional.empty());

            Set<String> deleteFiles = equalityDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(1);

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 0), " +
                            "('added_delete_files_count', 0)");
            assertThat(equalityDeleteFiles(table.getName()))
                    .hasSize(1)
                    .isEqualTo(deleteFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.nation WHERE regionkey != 1");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUpgradeToV3(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_upgrade_to_v3", "WITH (format_version = 2, format = '" + format + "') AS SELECT * FROM tpch.tiny.region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 0", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 1", 1);

            Set<String> legacyDeletes = positionDeleteFiles(table.getName());
            assertThat(legacyDeletes).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");
            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 2), " +
                            "('added_delete_files_count', 1)");

            assertThat(legacyPositionDeleteFiles(table.getName())).isEmpty();
            assertThat(deletionVectorFiles(table.getName())).isNotEmpty();

            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.region WHERE regionkey NOT IN (0, 1)");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizeWithOnlyDVs(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_optimize_only_dvs", "WITH (format_version = 3, format = '" + format + "') AS SELECT * FROM tpch.tiny.region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 0", 1);

            Set<String> dvFiles = deletionVectorFiles(table.getName());
            assertThat(dvFiles).isNotEmpty();
            assertThat(legacyPositionDeleteFiles(table.getName())).isEmpty();

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 0), " +
                            "('added_delete_files_count', 0)");

            assertThat(deletionVectorFiles(table.getName())).isEqualTo(dvFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.region WHERE regionkey != 0");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizeConvertPositionDeletesToDVsForPartitionedTable(IcebergFileFormat format)
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_v3_convert", "(x int, part varchar) WITH (partitioning = ARRAY['part'], format_version = 2, format = '" + format + "')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'a'), (3, 'a')", 3);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);

            assertThat(legacyPositionDeleteFiles(table.getName())).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 2), " +
                            "('added_delete_files_count', 1)");

            assertThat(legacyPositionDeleteFiles(table.getName())).isEmpty();
            assertThat(deletionVectorFiles(table.getName())).isNotEmpty();

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (3, VARCHAR 'a')");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testOptimizePartitionEvolution(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_v3_partition_evolution", "(id int, part varchar) WITH (partitioning = ARRAY['part'], format_version = 2, format = '" + format + "')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'a'), (3, 'a')", 3);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'b'), (5, 'b'), (6, 'b')", 3);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 4", 1);

            assertThat(legacyPositionDeleteFiles(table.getName())).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY[]");

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 2), " +
                            "('added_delete_files_count', 2)");

            assertThat(legacyPositionDeleteFiles(table.getName())).isEmpty();
            assertThat(deletionVectorFiles(table.getName())).isNotEmpty();

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (2, VARCHAR 'a'), (3, 'a'), (5, VARCHAR 'b'), (6, 'b')");
        }
    }

    @Test
    void testSinglePositionDeleteSurvivesOptimizeThenExpire()
            throws IOException
    {
        try (TestTable table = newTrinoTable("test_optimize_then_expire", "(id int, part varchar) WITH (partitioning = ARRAY['part'], format_version = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'cold'), (2, 'cold'), (3, 'cold')", 3);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'hot'), (5, 'hot'), (6, 'hot')", 3);

            // Hot partition has 2 position deletes, cold has 1.
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 4", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 5", 1);
            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(3);
            String coldDeleteFile = deleteFiles.stream()
                    .filter(path -> path.contains("part=cold"))
                    .collect(onlyElement());

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes",
                    "VALUES " +
                            "('removed_position_delete_files_count', 2), " +
                            "('added_delete_files_count', 1)");
            assertThat(positionDeleteFiles(table.getName())).contains(coldDeleteFile);
            long optimizeSnapshotId = loadTable(table.getName()).currentSnapshot().snapshotId();

            // Add new snapshot to expire one from optimize_position_deletes.
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (7, 'hot')", 1);

            // expire_snapshots should not remove cold position delete.
            Session shortRetention = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                    .build();
            assertUpdate(shortRetention, "ALTER TABLE " + table.getName() + " EXECUTE expire_snapshots(retention_threshold => '0s')");

            // The optimize_position_deletes snapshot was actually expired, not skipped
            assertThat(computeActual("SELECT snapshot_id FROM \"" + table.getName() + "$snapshots\"").getOnlyColumnAsSet())
                    .doesNotContain(optimizeSnapshotId);

            assertThat(positionDeleteFiles(table.getName())).contains(coldDeleteFile);
            assertThat(fileSystemFactory.create(SESSION).newInputFile(Location.of(coldDeleteFile)).exists())
                    .as("live position delete file %s must not be removed by expire_snapshots", coldDeleteFile)
                    .isTrue();

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (2, VARCHAR 'cold'), (3, 'cold'), (6, VARCHAR 'hot'), (7, 'hot')");
        }
    }

    @Test
    void testUnsupportedWhere()
    {
        try (TestTable table = newTrinoTable("test_unsupported_where", "WITH (partitioning = ARRAY['part']) AS SELECT 1 id, 1 part")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes WHERE id = 1", ".* WHERE not supported for procedure OPTIMIZE_POSITION_DELETES");
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes WHERE part = 10", ".* WHERE not supported for procedure OPTIMIZE_POSITION_DELETES");
        }
    }

    private static DataFile dataFile(Table table, String path, String partition)
    {
        return DataFiles.builder(table.spec())
                .withFormat(getFileFormat(table).toIceberg())
                .withPath(path)
                .withFileSizeInBytes(10)
                .withPartitionPath(partition)
                .withRecordCount(1)
                .build();
    }

    private DeleteFile writePositionDelete(Table table, String dataFilePath, String deleteFilePath, String partitionValue)
            throws IOException
    {
        PartitionData partitionData = PartitionData.fromJson("{\"partitionValues\":[\"%s\"]}".formatted(partitionValue), new Type[] {Types.StringType.get()});
        FileWriterFactory<?> writerFactory = TrinoGenericFileWriterFactory.builderFor(table);
        ForwardingOutputFile outputFile = new ForwardingOutputFile(fileSystemFactory.create(SESSION), Location.of(deleteFilePath));
        try (PositionDeleteWriter<?> appender = writerFactory.newPositionDeleteWriter(encryptedOutput(outputFile, EMPTY), table.spec(), partitionData)) {
            @SuppressWarnings("rawtypes")
            PositionDelete delete = PositionDelete.create();
            delete.set(dataFilePath, 10);
            //noinspection unchecked
            appender.write(delete);
            appender.close();
            return appender.result().deleteFiles().stream().collect(onlyElement());
        }
    }

    private DeleteFile writeEqualityDelete(Table table, String deleteFilePath, String partitionValue, Map<String, Object> deletes)
            throws IOException
    {
        PartitionData partitionData = PartitionData.fromJson("{\"partitionValues\":[\"%s\"]}".formatted(partitionValue), new Type[] {Types.StringType.get()});

        Schema deleteSchema = table.schema().select(deletes.keySet());
        try (FileIO fileIo = FILE_IO_FACTORY.create(fileSystemFactory.create(TestingConnectorSession.SESSION))) {
            Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(fileIo.newOutputFile(deleteFilePath))
                    .forTable(table)
                    .withPartition(partitionData)
                    .rowSchema(deleteSchema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .equalityFieldIds(deletes.keySet().stream()
                            .map(name -> deleteSchema.findField(name).fieldId())
                            .collect(toImmutableList()))
                    .overwrite();
            try (EqualityDeleteWriter<Record> writer = writerBuilder.buildEqualityWriter()) {
                Record dataDelete = GenericRecord.create(deleteSchema);
                try (Closeable ignored = writer) {
                    writer.write(dataDelete.copy(deletes));
                }

                DeleteFile deleteFile = writer.toDeleteFile();
                table.newRowDelta().addDeletes(deleteFile).commit();
                return deleteFile;
            }
        }
    }

    private Set<String> positionDeleteFiles(String tableName)
    {
        return loadFiles(tableName, "content = 1");
    }

    private Set<String> legacyPositionDeleteFiles(String tableName)
    {
        return loadFiles(tableName, "content = 1 AND file_format != 'PUFFIN'");
    }

    private Set<String> deletionVectorFiles(String tableName)
    {
        return loadFiles(tableName, "content = 1 AND file_format = 'PUFFIN'");
    }

    private Set<String> equalityDeleteFiles(String tableName)
    {
        return loadFiles(tableName, "content = 2");
    }

    private Set<String> loadFiles(String tableName, String filter)
    {
        return computeActual("SELECT file_path FROM \"" + tableName + "$files\" WHERE " + filter).getOnlyColumnAsSet().stream()
                .map(path -> (String) path)
                .collect(toImmutableSet());
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
