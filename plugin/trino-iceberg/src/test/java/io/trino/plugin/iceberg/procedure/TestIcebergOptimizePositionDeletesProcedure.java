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
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.TrinoGenericFileWriterFactory;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
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
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.max-format-version", "3")
                .build();
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

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");
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

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");
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

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");
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

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");
            assertThat(positionDeleteFiles(table.getName())).isEmpty();

            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDanglingDeletes(IcebergFileFormat format)
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_dangling_deletes", "(x int, part varchar) WITH (partitioning = ARRAY['part'], format = '" + format + "')")) {
            BaseTable icebergTable = loadTable(table.getName());
            String tableLocation = icebergTable.location();

            DataFile dataFileA = dataFile(icebergTable, "data-a", "part=a");
            DataFile dataFileC = dataFile(icebergTable, "data-c", "part=c");

            icebergTable.newAppend()
                    .appendFile(dataFileA)
                    .appendFile(dataFileC)
                    .commit();

            DeleteFile deleteFileA = deleteFile(icebergTable, "data-a", tableLocation + "/part=a/data-a-pos-deletes", "a");
            DeleteFile danglingDeleteFileB = deleteFile(icebergTable, "data-b", tableLocation + "/part=b/data-b-pos-deletes", "b");

            icebergTable.newRowDelta()
                    .addRows(dataFileA)
                    .addRows(dataFileC)
                    .addDeletes(deleteFileA)
                    .addDeletes(danglingDeleteFileB)
                    .commit();

            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");

            assertThat(positionDeleteFiles(table.getName()))
                    .hasSize(1)
                    .allMatch(deleteFile -> deleteFile.contains("part=a"));
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testAllDanglingDeletes(IcebergFileFormat format)
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_dangling_deletes", "(x int, part varchar) WITH (partitioning = ARRAY['part'], format = '" + format + "')")) {
            BaseTable icebergTable = loadTable(table.getName());
            String tableLocation = icebergTable.location();

            DataFile dataFileA = dataFile(icebergTable, "data-a", "part=a");
            DataFile dataFileC = dataFile(icebergTable, "data-c", "part=c");

            icebergTable.newAppend()
                    .appendFile(dataFileA)
                    .appendFile(dataFileC)
                    .commit();

            DeleteFile danglingDeleteFileB = deleteFile(icebergTable, "data-b", tableLocation + "/part=b/data-b-pos-deletes", "b");
            DeleteFile danglingDeleteFileD = deleteFile(icebergTable, "data-d", tableLocation + "/part=d/data-d-pos-deletes", "d");

            icebergTable.newRowDelta()
                    .addRows(dataFileA)
                    .addRows(dataFileC)
                    .addDeletes(danglingDeleteFileB)
                    .addDeletes(danglingDeleteFileD)
                    .commit();

            Set<String> deleteFiles = positionDeleteFiles(table.getName());
            assertThat(deleteFiles).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");

            assertThat(positionDeleteFiles(table.getName())).isEmpty();
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

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes");
            assertThat(equalityDeleteFiles(table.getName()))
                    .hasSize(1)
                    .isEqualTo(deleteFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.nation WHERE regionkey != 1");
        }
    }

    @Test
    void testUnsupportedDeletionVector()
    {
        try (TestTable table = newTrinoTable("test_optimize_delete_files", "WITH (format_version=3) AS SELECT * FROM tpch.tiny.region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 0", 1);
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize_position_deletes", "Unsupported file format: PUFFIN");

            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM tpch.tiny.region WHERE regionkey != 0");
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

    private DeleteFile deleteFile(Table table, String dataFilePath, String deleteFilePath, String partitionValue)
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

    private Set<String> positionDeleteFiles(String tableName)
    {
        return loadFiles(tableName, 1);
    }

    private Set<String> equalityDeleteFiles(String tableName)
    {
        return loadFiles(tableName, 2);
    }

    private Set<String> loadFiles(String tableName, int content)
    {
        return computeActual("SELECT file_path FROM \"" + tableName + "$files\" WHERE content = " + content).getOnlyColumnAsSet().stream()
                .map(path -> (String) path)
                .collect(toImmutableSet());
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
