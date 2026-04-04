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

import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Uses file metastore sharing location between catalogs
final class TestIcebergAddFilesProcedure
        extends AbstractTestQueryFramework
{
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
        dataDirectory = baseDataDir.resolve("iceberg_data");
        verify(dataDirectory.toFile().mkdirs());

        queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                "iceberg.add-files-procedure.enabled", "true",
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                // Intentionally sharing the file metastore directory with Hive
                "hive.metastore.catalog.dir", "local:///iceberg_data",
                "fs.hadoop.enabled", "true"));

        queryRunner.installPlugin(new TestingHivePlugin(baseDataDir));
        queryRunner.createCatalog("hive", "hive", Map.of(
                "hive.security", "allow-all",
                "hive.metastore", "file",
                // Intentionally sharing the file metastore directory with Iceberg
                "hive.metastore.catalog.dir", "local:///iceberg_data"));

        queryRunner.execute("CREATE SCHEMA tpch");

        return queryRunner;
    }

    @Test
    void testAddFilesFomTable()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);

        assertUpdate(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "VALUES ('added_data_files', 1)");

        assertQuery("SELECT * FROM hive.tpch." + hiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesNotNull()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int NOT NULL)");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");
        assertQuery("SELECT * FROM hive.tpch." + hiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesNotNullViolation()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT CAST(NULL AS int) x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int NOT NULL)");

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                ".*NULL value not allowed for NOT NULL column: x");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                ".*NULL value not allowed for NOT NULL column: x");

        assertQueryReturnsEmptyResult("SELECT * FROM iceberg.tpch." + icebergTableName);

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesDifferentFileFormat()
    {
        testAddFilesDifferentFileFormat("PARQUET", "ORC");
        testAddFilesDifferentFileFormat("PARQUET", "AVRO");
        testAddFilesDifferentFileFormat("ORC", "PARQUET");
        testAddFilesDifferentFileFormat("ORC", "AVRO");
        testAddFilesDifferentFileFormat("AVRO", "PARQUET");
        testAddFilesDifferentFileFormat("AVRO", "ORC");
    }

    private void testAddFilesDifferentFileFormat(String hiveFormat, String icebergFormat)
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + icebergFormat + "') AS SELECT 1 x, 2 y", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + hiveFormat + "') AS SELECT 3 x, 4 y", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 2), (3, 4)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesAcrossSchema()
    {
        String hiveSchemaName = "test_schema" + randomNameSuffix();
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA hive." + hiveSchemaName);

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = 'PARQUET') AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive." + hiveSchemaName + "." + hiveTableName + " WITH (format = 'ORC') AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE tpch." + icebergTableName + " EXECUTE add_files_from_table('" + hiveSchemaName + "', '" + hiveTableName + "')");

        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        assertUpdate("DROP SCHEMA hive." + hiveSchemaName + " CASCADE ");
    }

    @Test
    void testAddFilesTypeMismatch()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = 'ORC') AS SELECT '1' x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'ORC') AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "Target 'x' column is 'string' type, but got source 'int' type");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromLessColumnTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 x, 20 y", 1);

            assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");
            assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES (1, NULL), (2, 20)");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesFromLessColumnTableNotNull()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int, y int NOT NULL) WITH (format = '" + format + "')");

            assertQueryFails(
                    "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                    ".*NULL value not allowed for NOT NULL column: y");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesFromMoreColumnTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x, 'extra' y", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 x", 1);

            assertQueryFails(
                    "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                    "Target table should have at least 2 columns but got 1");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesDifferentAllDataColumnDefinitions()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 y", 1);

            assertQueryFails(
                    "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                    "All columns in the source table do not exist in the target table");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesDifferentPartitionColumnDefinitions()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (partitioned_by = ARRAY['hive_part']) AS SELECT 1 x, 10 hive_part", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['iceberg_part']) AS SELECT 2 x, 20 iceberg_part", 1);

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['hive_part'], ARRAY['10']))",
                "Partition column 'hive_part' does not exist");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesSpecialCharPartitionColumnDefinitions()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (partitioned_by = ARRAY['special@col']) AS SELECT 1 x, 10 \"special@col\"", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['\"special@col\"']) AS SELECT 2 x, 20 \"special@col\"", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['special@col'], ARRAY['10']))");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES (1, 10), (2, 20)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromNonPartitionTable()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['iceberg_part']) AS SELECT 2 x, 20 iceberg_part", 1);

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "Numbers of partition columns should be equivalent. target: 1, source: 0");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesPartitionFilter()
    {
        String hiveTableName = "test_add_files_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, part varchar) WITH (partitioning = ARRAY['part'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int, part varchar) WITH (partitioned_by = ARRAY['part'])");
        assertUpdate("INSERT INTO hive.tpch." + hiveTableName + " VALUES (1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')", 4);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['test1']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1')");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['test2']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        // no-partition filter on the partitioned table
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "partition_filter argument must be provided for partitioned tables");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        // empty partition filter on the partitioned table
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map())",
                ".* partition value count must match partition column count");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void tetAddFilesNonPartitionTableWithPartitionFilter()
    {
        String hiveTableName = "test_add_files_non_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, part varchar) WITH (partitioning = ARRAY['part'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int)");

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['test1']))",
                "Numbers of partition columns should be equivalent. target: 1, source: 0");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void tetAddFilesInvalidPartitionFilter()
    {
        String hiveTableName = "test_add_files_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, part varchar) WITH (partitioning = ARRAY['part'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int, part varchar) WITH (partitioned_by = ARRAY['part'])");

        // Invalid partition key
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['invalid_part'], ARRAY['test1']))",
                ".*Invalid partition: invalid_part=test1");
        // Invalid partition value
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['invalid_value']))",
                ".*Invalid partition: part=invalid_value");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesNestedPartitionFilter()
    {
        String hiveTableName = "test_add_files_nested_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_nested_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, parent varchar, child varchar) WITH (partitioning = ARRAY['parent', 'child'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int, parent varchar, child varchar) WITH (partitioned_by = ARRAY['parent', 'child'])");
        assertUpdate("INSERT INTO hive.tpch." + hiveTableName + " VALUES (1, 'parent1', 'child1'), (2, 'parent1', 'child2'), (3, 'parent2', 'child3'), (4, 'parent2', 'child4')", 4);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['parent', 'child'], ARRAY['parent1', 'child1']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'parent1', 'child1')");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['parent', 'child'], ARRAY['parent1', 'child2']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'parent1', 'child1'), (2, 'parent1', 'child2')");

        // TODO: Add support for partial partition filters
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['parent'], ARRAY['parent2']))",
                ".*partition value count must match partition column count.*");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesWithRecursiveDirectory()
            throws Exception
    {
        String hiveTableName = "test_migrate_recursive_directory_" + randomNameSuffix();
        String icebergTableName = "test_migrate_recursive_directory_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'ORC') AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int)");

        // Move a file to the nested directory
        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String fileName = Location.of(path).fileName();
        Path tableLocation = dataDirectory.resolve("tpch").resolve(hiveTableName);
        Files.createDirectory(tableLocation.resolve("nested"));
        Files.move(tableLocation.resolve(fileName), tableLocation.resolve("nested").resolve(fileName));

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC')",
                ".*Recursive directory must not exist when recursive_directory argument is 'fail'.*");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC', 'fail')",
                ".*Recursive directory must not exist when recursive_directory argument is 'fail'.*");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC', 'false')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + icebergTableName);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC', 'true')");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesDirectoryLocation()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesRowLineagePreExistingColumn()
    {
        String hiveTableName = "test_add_files_pre_existing_" + randomNameSuffix();
        String icebergTableName = "test_add_files_pre_existing_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 _row_id", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 _row_id", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')");

        assertThat(query("SELECT \"$row_id\" FROM " + icebergTableName)).failure().hasStackTraceContaining("Column '$row_id' cannot be resolved");
        assertThat(query("SELECT \"$last_updated_sequence_number\" FROM " + icebergTableName)).failure().hasStackTraceContaining("Column '$last_updated_sequence_number' cannot be resolved");
        assertThat(query("SELECT _row_id FROM " + icebergTableName)).matches("VALUES (1), (2)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromTableRowLineagePreExistingColumn()
    {
        String hiveTableName = "test_add_files_from_table_pre_existing_" + randomNameSuffix();
        String icebergTableName = "test_add_files_from_table_pre_existing_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 _row_id", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 _row_id", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertThat(query("SELECT \"$row_id\" FROM " + icebergTableName)).failure().hasStackTraceContaining("Column '$row_id' cannot be resolved");
        assertThat(query("SELECT \"$last_updated_sequence_number\" FROM " + icebergTableName)).failure().hasStackTraceContaining("Column '$last_updated_sequence_number' cannot be resolved");
        assertThat(query("SELECT _row_id FROM " + icebergTableName)).matches("VALUES (1), (2)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFileLocation()
    {
        // Spark allows adding files from specific file location
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);

        assertUpdate(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                "VALUES ('added_data_files', 1)");

        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromNonPartitionTableToPartitionTable()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + icebergTableName + " WITH (partitioning = ARRAY['part']) AS SELECT 1 x, 'test' part", 1);
        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')",
                "Failed to add files: Invalid partition location, expected path with partitions defined as 'part=value':.*");

        assertUpdate("DROP TABLE " + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAddFilesFromPartitionTableToDifferentPartitionTable()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + icebergTableName + " WITH (partitioning = ARRAY['part']) AS SELECT 1 x, 'test' part", 1);
        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part1', 'part2']) AS SELECT 2 x, 'test1' part1, 'test2' part2", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')",
                "Failed to add files: Invalid partition.*");

        assertUpdate("DROP TABLE " + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAddFilesFromPartitionTableToPartitionTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
            String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioning = ARRAY['part']) AS SELECT 1 x, 'iceberg' part".formatted(icebergTableName, format), 1);
            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioned_by = ARRAY['part']) AS SELECT 2 x, 'hive' part".formatted(hiveTableName, format), 1);

            String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

            assertUpdate("ALTER TABLE %s EXECUTE add_files('%s', '%s')".formatted(icebergTableName, path, format));

            assertQuery("SELECT \"$partition\", * FROM " + icebergTableName,
                    "VALUES ('part=iceberg', 1, 'iceberg'), ('part=hive', 2, 'hive')");
            assertQuery("SELECT * FROM " + icebergTableName,
                    "VALUES (1, 'iceberg'), (2, 'hive')");

            assertUpdate("DROP TABLE " + hiveTableName);
            assertUpdate("DROP TABLE " + icebergTableName);
        }
    }

    @Test
    void testAddFilesFromPartitionTableToDifferentlyPartitionedTable()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + icebergTableName + " WITH (partitioning = ARRAY['part1']) AS SELECT 1 x, 'iceberg' part1", 1);
        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part2']) AS SELECT 2 x, 'hive' part2", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

        assertQueryFails("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                "Failed to add files: Invalid partition location, expected partition key 'part1' but was 'part2':.*");

        assertUpdate("DROP TABLE " + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAddFilesFromPartitionTableToDifferentlyOrderedPartitionTable()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + icebergTableName + " WITH (partitioning = ARRAY['part1', 'part2']) AS SELECT 1 x, 'iceberg1' part1, 'iceberg2' part2", 1);
        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part2', 'part1']) AS SELECT 2 x, 'hive2' part2, 'hive1' part1", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

        assertQueryFails("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                "Failed to add files: Invalid partition location, expected partition key 'part1' but was 'part2':.*");

        assertUpdate("DROP TABLE " + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAddFilesFromPartitionTableToTableWithPartitionSuperset()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + icebergTableName + " WITH (partitioning = ARRAY['part1', 'part2']) AS SELECT 1 x, 'iceberg1' part1, 'iceberg2' part2", 1);
        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part1']) AS SELECT 2 x, 'hive1' part1", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

        assertQueryFails("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                "Failed to add files: Invalid partition location, expected path with partitions defined as 'part1=value/part2=value':.*");

        assertUpdate("DROP TABLE " + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAddFilesFromPartitionTableToTableWithPartitionTransform()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + icebergTableName + " WITH (partitioning = ARRAY['bucket(part, 8)']) AS SELECT 1 x, 'iceberg' part", 1);
        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 2 x, 'hive' part", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

        assertQueryFails("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                "Failed to add files: Invalid partition location, expected partition key 'part_bucket' but was 'part':.*");

        assertUpdate("DROP TABLE " + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAddFilesFromPartitionTableToPartitionTableWithSpecialCharacters()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
            String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();
            String specialChars = "\"#%''*/:=?\\{[]^";
            String encodedChars = encode(specialChars.replace("''", "'"), UTF_8);

            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioning = ARRAY['part']) AS SELECT 1 x, 'iceberg_%s' part".formatted(icebergTableName, format, specialChars), 1);
            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioned_by = ARRAY['part']) AS SELECT 2 x, 'hive_%s' part".formatted(hiveTableName, format, specialChars), 1);

            String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

            assertUpdate("ALTER TABLE %s EXECUTE add_files('%s', '%s')".formatted(icebergTableName, path, format));

            assertQuery("SELECT \"$partition\", * FROM " + icebergTableName,
                    "VALUES ('part=iceberg_%1$s', 1, 'iceberg_%2$s'), ('part=hive_%1$s', 2, 'hive_%2$s')".formatted(encodedChars, specialChars));
            assertQuery("SELECT * FROM " + icebergTableName,
                    "VALUES (1, 'iceberg_%1$s'), (2, 'hive_%1$s')".formatted(specialChars));

            assertUpdate("DROP TABLE " + hiveTableName);
            assertUpdate("DROP TABLE " + icebergTableName);
        }
    }

    @Test
    void testAddFilesFromMultiPartitionTableToPartitionTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
            String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioning = ARRAY['part1', 'part2']) AS SELECT 1 x, 'iceberg1' part1, 'iceberg2' part2".formatted(icebergTableName, format), 1);
            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioned_by = ARRAY['part1', 'part2']) AS SELECT 2 x, 'hive1' part1, 'hive2' part2".formatted(hiveTableName, format), 1);

            String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

            assertUpdate("ALTER TABLE %s EXECUTE add_files('%s', '%s')".formatted(icebergTableName, path, format));

            assertQuery("SELECT \"$partition\", * FROM " + icebergTableName,
                    "VALUES ('part1=iceberg1/part2=iceberg2', 1, 'iceberg1', 'iceberg2'), ('part1=hive1/part2=hive2', 2, 'hive1', 'hive2')");
            assertQuery("SELECT * FROM " + icebergTableName,
                    "VALUES (1, 'iceberg1', 'iceberg2'), (2, 'hive1', 'hive2')");

            assertUpdate("DROP TABLE " + hiveTableName);
            assertUpdate("DROP TABLE " + icebergTableName);
        }
    }

    @Test
    void testAddFilesParentDirectoryFromPartitionTableToPartitionTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
            String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioning = ARRAY['part']) AS SELECT 1 x, 'iceberg' part".formatted(icebergTableName, format), 1);
            assertUpdate("CREATE TABLE %s WITH (format = '%s', partitioned_by = ARRAY['part']) AS SELECT 2 x, 'hive' part".formatted(hiveTableName, format), 1);

            String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);
            String directory = Location.of(path).parentDirectory().toString();

            assertUpdate("ALTER TABLE %s EXECUTE add_files('%s', '%s')".formatted(icebergTableName, directory, format));

            assertQuery("SELECT \"$partition\", * FROM " + icebergTableName, "VALUES ('part=iceberg', 1, 'iceberg'), ('part=hive', 2, 'hive')");
            assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'iceberg'), (2, 'hive')");

            assertUpdate("DROP TABLE " + hiveTableName);
            assertUpdate("DROP TABLE " + icebergTableName);
        }
    }

    @Test
    void testAddFilesToTablePartitionedOnTimestamp()
    {
        String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            assertUpdate("CREATE TABLE %s (x INT, part TIMESTAMP) WITH (format = '%s', partitioning = ARRAY['part'])".formatted(icebergTableName, format));
            assertUpdate("INSERT INTO %s VALUES (1, TIMESTAMP '2024-01-01 12:34:56')".formatted(icebergTableName), 1);
            assertUpdate("CREATE TABLE %s (x INT, part TIMESTAMP) WITH (format = '%s', partitioned_by = ARRAY['part'])".formatted(hiveTableName, format));
            assertUpdate("INSERT INTO %s VALUES (2, TIMESTAMP '2024-01-01 11:11:11')".formatted(hiveTableName), 1);

            String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

            assertQueryFails(
                    "ALTER TABLE %s EXECUTE add_files('%s', '%s')".formatted(icebergTableName, path, format),
                    "Failed to add files: Unsupported type for fromPartitionString: timestamp");

            assertUpdate("DROP TABLE " + hiveTableName);
            assertUpdate("DROP TABLE " + icebergTableName);
        }
    }

    @Test
    void testAddFilesToPartitionTableForEachNonTimestampPrimitiveHiveType()
    {
        record PartitionColumn(String name, String type, String hiveValue, String icebergValue) {}

        List<PartitionColumn> partitionColumns = List.of(
                new PartitionColumn("part_tinyint", "TINYINT", "127", "126"),
                new PartitionColumn("part_smallint", "SMALLINT", "32767", "32766"),
                new PartitionColumn("part_int", "INT", "2147483647", "2147483646"),
                new PartitionColumn("part_bigint", "BIGINT", "9223372036854775807", "9223372036854775806"),
                new PartitionColumn("part_boolean", "BOOLEAN", "true", "false"),
                new PartitionColumn("part_real", "REAL", "1.23", "0.45"),
                new PartitionColumn("part_double", "DOUBLE", "4.56", "5.67"),
                new PartitionColumn("part_varchar", "VARCHAR", "'hive'", "'iceberg'"),
                new PartitionColumn("part_date", "DATE", "DATE '2024-01-04'", "DATE '2023-01-04'"),
                new PartitionColumn("part_decimal", "DECIMAL(10, 2)", "123.45", "1.23"));

        String columnDefinitions = partitionColumns.stream()
                .map(c -> c.name() + " " + c.type())
                .collect(joining(", "));
        String partitionArray = partitionColumns.stream()
                .map(c -> "'" + c.name() + "'")
                .collect(joining(", "));
        String columnNames = partitionColumns.stream()
                .map(PartitionColumn::name)
                .collect(joining(", "));
        String hiveValuesList = partitionColumns.stream()
                .map(PartitionColumn::hiveValue)
                .collect(joining(", "));
        String icebergValuesList = partitionColumns.stream()
                .map(PartitionColumn::icebergValue)
                .collect(joining(", "));

        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "hive.tpch.test_add_files_location_" + randomNameSuffix();
            String icebergTableName = "iceberg.tpch.test_add_files_location_" + randomNameSuffix();

            assertUpdate("CREATE TABLE %s (x INT, %s) WITH (format = '%s', partitioned_by = ARRAY[%s])".formatted(hiveTableName, columnDefinitions, format, partitionArray));
            assertUpdate("INSERT INTO %s VALUES (1, %s)".formatted(hiveTableName, hiveValuesList), 1);

            assertUpdate("CREATE TABLE %s (x INT, %s) WITH (format = '%s', partitioning = ARRAY[%s])".formatted(icebergTableName, columnDefinitions, format, partitionArray));
            assertUpdate("INSERT INTO %s VALUES (2, %s)".formatted(icebergTableName, icebergValuesList), 1);

            String path = (String) computeScalar("SELECT \"$path\" FROM " + hiveTableName);

            assertUpdate("ALTER TABLE %s EXECUTE add_files('%s', '%s')".formatted(icebergTableName, path, format));

            assertQuery(
                    "SELECT x, %s FROM %s".formatted(columnNames, icebergTableName),
                    "VALUES (1, %s), (2, %s)".formatted(hiveValuesList, icebergValuesList));

            assertUpdate("DROP TABLE " + hiveTableName);
            assertUpdate("DROP TABLE " + icebergTableName);
        }
    }

    @Test
    void testAddFilesLocationWithWrongFormat()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'ORC') AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String location = Location.of(path).parentDirectory().toString();

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + location + "', 'TEXTFILE')",
                ".* The procedure does not support storage format: TEXTFILE");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files(location=>'" + location + "', format=>'PARQUET')",
                ".*Failed to read file footer.*");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesUnsupportedFileFormat()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT '1' x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'TEXTFILE') AS SELECT '2' x", 1);

        assertQueryFails("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')", ".*Unsupported storage format: TEXTFILE.*");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES '1'");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesToNonIcebergTable()
    {
        String sourceHiveTableName = "test_add_files_" + randomNameSuffix();
        String targetHiveTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + sourceHiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + targetHiveTableName + " AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + targetHiveTableName + " EXECUTE add_files_from_table('tpch', '" + sourceHiveTableName + "')",
                "Not an Iceberg table: .*");

        assertQuery("SELECT * FROM hive.tpch." + sourceHiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM hive.tpch." + targetHiveTableName, "VALUES 2");

        assertUpdate("DROP TABLE hive.tpch." + sourceHiveTableName);
        assertUpdate("DROP TABLE hive.tpch." + targetHiveTableName);
    }

    @Test
    void testAddFilesToView()
    {
        String sourceViewName = "test_add_files_" + randomNameSuffix();
        String targetIcebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE VIEW iceberg.tpch." + sourceViewName + " AS SELECT 1 x");
        assertUpdate("CREATE TABLE iceberg.tpch." + targetIcebergTableName + " AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + targetIcebergTableName + " EXECUTE add_files_from_table('tpch', '" + sourceViewName + "')",
                "The procedure doesn't support adding files from VIRTUAL_VIEW table type");

        assertQuery("SELECT * FROM iceberg.tpch." + sourceViewName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + targetIcebergTableName, "VALUES 2");

        assertUpdate("DROP VIEW iceberg.tpch." + sourceViewName);
        assertUpdate("DROP TABLE iceberg.tpch." + targetIcebergTableName);
    }

    @Test
    void testAddFilesFromIcebergTable()
    {
        String sourceIcebergTableName = "test_add_files_" + randomNameSuffix();
        String targetIcebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + sourceIcebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + targetIcebergTableName + " AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + targetIcebergTableName + " EXECUTE add_files_from_table('tpch', '" + sourceIcebergTableName + "')",
                "Adding files from non-Hive tables is unsupported");

        assertQuery("SELECT * FROM iceberg.tpch." + sourceIcebergTableName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + targetIcebergTableName, "VALUES 2");

        assertUpdate("DROP TABLE iceberg.tpch." + sourceIcebergTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + targetIcebergTableName);
    }

    @Test
    void testAddDuplicatedFiles()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);
        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')");

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                ".*File already exists.*");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files(location=>'" + path + "', format=>'ORC')",
                ".*File already exists.*");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddDuplicatedFilesFromTable()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                ".*File already exists.*");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table(schema_name=>'tpch', table_name=>'" + hiveTableName + "')",
                ".*File already exists.*");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesTargetTableNotFound()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertQueryFails(
                "ALTER TABLE table_not_found EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                ".* Table 'iceberg.tpch.table_not_found' does not exist");
        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
    }

    @Test
    void testAddFilesSourceTableNotFound()
    {
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int)");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', 'table_not_found')",
                "Table 'tpch.table_not_found' not found");
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesLocationNotFound()
    {
        String tableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + tableName + "(x int)");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files('file:///location-not-found', 'ORC')",
                ".*Location not found.*");
        assertUpdate("DROP TABLE iceberg.tpch." + tableName);
    }

    @Test
    void testAddFilesInvalidArguments()
    {
        String tableName = "test_add_files_" + randomNameSuffix();
        assertUpdate("CREATE TABLE iceberg.tpch." + tableName + "(x int)");

        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files_from_table(schema_name=>'tpch')",
                "Required procedure argument 'table_name' is missing");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files_from_table(table_name=>'test')",
                "Required procedure argument 'schema_name' is missing");

        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files(location=>'file:///tmp')",
                "Required procedure argument 'format' is missing");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files(format=>'ORC')",
                "Required procedure argument 'location' is missing");

        assertUpdate("DROP TABLE iceberg.tpch." + tableName);
    }

    @Test
    void testAddFilesFromParquetTimeMillis()
    {
        try (TestTable table = newTrinoTable("test_parquet", "(_time TIME)")) {
            String path = Resources.getResource("iceberg/parquet_time_millis/time_millis.parquet").toString();
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE add_files('" + path + "', 'PARQUET')");

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES TIME '00:00:00.000000', TIME '12:34:56.123', TIME '23:59:59.999'");

            assertThat(query("SELECT lower_bounds[1], upper_bounds[1] FROM \"" + table.getName() + "$files\""))
                    .matches("VALUES (VARCHAR '00:00', VARCHAR '23:59:59.999')");
        }
    }
}
