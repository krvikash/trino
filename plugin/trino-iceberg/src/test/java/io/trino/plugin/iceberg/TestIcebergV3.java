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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.listFiles;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path dataDirectory = Files.createTempDirectory("_test_hidden");
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setMetastoreDirectory(dataDirectory.toFile())
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.max-format-version", "3")
                .addIcebergProperty("iceberg.add-files-procedure.enabled", "true")
                .build();

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .buildOrThrow());

        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);

        return queryRunner;
    }

    @Test
    void testDefaultColumnValues()
    {
        try (TestTable table = newTrinoTable("test_default_column_values", "(id int, data int DEFAULT 123 NOT NULL)")) {
            BaseTable icebergTable = loadTable(table.getName());

            assertQuerySucceeds("INSERT INTO " + table.getName() + " (id) VALUES (1)");
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES (1, 123)");

            assertQuerySucceeds("UPDATE " + table.getName() + " SET id = 2");
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES (2, 123)");

            assertQuerySucceeds("DELETE FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName())).returnsEmptyResult();

            assertQuerySucceeds("TRUNCATE TABLE " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName())).returnsEmptyResult();

            assertQuerySucceeds("MERGE INTO " + table.getName() + " USING (VALUES 42) t(dummy) ON false WHEN NOT MATCHED THEN INSERT (id) VALUES (3)");
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES (3, 123)");

            assertQuerySucceeds("ALTER TABLE " + table.getName() + " EXECUTE optimize");
            assertQuerySucceeds("ANALYZE " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES (3, 123)");

            // Supported column operations
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".data IS 'test comment'");
            icebergTable.refresh();
            Types.NestedField columnAfterComment = icebergTable.schema().columns().get(1);
            assertThat(columnAfterComment.type()).isEqualTo(Types.IntegerType.get());
            assertThat(columnAfterComment.doc()).isEqualTo("test comment");
            assertThat(columnAfterComment.isRequired()).isTrue();
            assertThat(columnAfterComment.writeDefault()).isEqualTo(123);

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN data SET DATA TYPE bigint");
            icebergTable.refresh();
            Types.NestedField columnAfterTypeChange = icebergTable.schema().columns().get(1);
            assertThat(columnAfterTypeChange.type()).isEqualTo(Types.LongType.get());
            assertThat(columnAfterTypeChange.doc()).isEqualTo("test comment");
            assertThat(columnAfterTypeChange.isRequired()).isTrue();
            assertThat(columnAfterTypeChange.writeDefault()).isEqualTo(123L);

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN data TO renamed");
            icebergTable.refresh();
            Types.NestedField columnAfterRenamed = icebergTable.schema().columns().get(1);
            assertThat(columnAfterRenamed.type()).isEqualTo(Types.LongType.get());
            assertThat(columnAfterRenamed.doc()).isEqualTo("test comment");
            assertThat(columnAfterRenamed.isRequired()).isTrue();
            assertThat(columnAfterRenamed.writeDefault()).isEqualTo(123L);

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN renamed DROP NOT NULL");
            icebergTable.refresh();
            Types.NestedField nullableColumn = icebergTable.schema().columns().get(1);
            assertThat(nullableColumn.type()).isEqualTo(Types.LongType.get());
            assertThat(nullableColumn.doc()).isEqualTo("test comment");
            assertThat(nullableColumn.isRequired()).isFalse();
            assertThat(nullableColumn.writeDefault()).isEqualTo(123L);

            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES (3, BIGINT '123')");
        }
    }

    @Test
    void testUpgradeTableToV3FromTrino()
    {
        String tableName = "test_upgrade_table_to_v3_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(1);

        // v1 -> v2
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2");
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(2);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");

        // v2 -> v3
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 3");
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(3);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @ParameterizedTest
    @MethodSource("formatMergeMode")
    void testUpgradeTableToV3FromTrinoWithRowLineage(IcebergFileFormat format, String mergeMode)
    {
        String tableName = "test_upgrade_table_to_v3_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2, format = '" + format + "') AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(2);

        assertThat(query("SELECT \"$row_id\" FROM " + tableName)).failure()
                .hasMessage("line 1:8: Column '$row_id' cannot be resolved");
        assertThat(query("SELECT \"$last_updated_sequence_number\" FROM " + tableName)).failure()
                .hasMessage("line 1:8: Column '$last_updated_sequence_number' cannot be resolved");

        // v2 -> v3
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 3");
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(3);
        Table icebergTable = loadTable(tableName);
        icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");

        // data sequence number from file is 1, first_row_id is null
        assertThat(query("SELECT \"$row_id\",\"$last_updated_sequence_number\", nationkey FROM " + tableName + " WHERE nationkey = 10"))
                .matches("VALUES ( CAST(NULL AS bigint), BIGINT '1', BIGINT '10')");
        assertUpdate("UPDATE " + tableName + " SET nationkey = 110 WHERE nationkey = 10", 1);
        assertThat(query("SELECT \"$last_updated_sequence_number\", nationkey FROM " + tableName + " WHERE nationkey = 110"))
                .matches("VALUES (BIGINT '2', BIGINT '110')");

        // sometimes first_row_id from file is 0, as freshly created table, sometimes it is 24, as next index of row_id in nation table
        assertThat(query("SELECT \"$row_id\" FROM " + tableName + " WHERE nationkey = 110")).result().onlyColumnAsSet()
                .containsAnyOf(0L, 24L);
    }

    @Test
    void testUpgradeTableFromV1ToV3()
    {
        String tableName = "test_upgrade_table_from_v1_to_v3_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(1);

        // v1 -> v3
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 3");
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(3);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    void testDowngradingFromV3Fails()
    {
        String tableName = "test_downgrading_from_v3_fails_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 3) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(formatVersion(loadTable(tableName))).isEqualTo(3);

        assertThat(query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2"))
                .failure()
                .hasMessage("Failed to set new property values")
                .rootCause()
                .hasMessage("Cannot downgrade v3 table to v2");
        assertThat(query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 1"))
                .failure()
                .hasMessage("Failed to set new property values")
                .rootCause()
                .hasMessage("Cannot downgrade v3 table to v1");
    }

    @Test
    void testRemoveOrphanDeletionVectors()
            throws Exception
    {
        Session singleWriterPerTask = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .build();

        Session shortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                .setCatalogSessionProperty("iceberg", "remove_orphan_files_min_retention", "0s")
                .build();

        try (TestTable table = newTrinoTable("expire_snapshots_dv", "(x int) WITH (format_version = 3)", List.of("1", "2"))) {
            Table icebergTable = loadTable(table.getName());
            String dataLocation = icebergTable.location() + "/data";

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertThat(listFiles(fileSystemFactory.create(SESSION), dataLocation))
                    .anyMatch(file -> file.endsWith(".puffin"));

            assertUpdate(singleWriterPerTask, "ALTER TABLE " + table.getName() + " EXECUTE optimize");
            computeActual(shortRetentionUnlocked, "ALTER TABLE " + table.getName() + " EXECUTE expire_snapshots(retention_threshold => '0s')");
            computeActual(shortRetentionUnlocked, "ALTER TABLE " + table.getName() + " EXECUTE remove_orphan_files(retention_threshold => '0s')");
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES 2");

            assertThat(listFiles(fileSystemFactory.create(SESSION), dataLocation))
                    .noneMatch(file -> file.endsWith(".puffin"));
        }
    }

    @Test
    void testPositionDeleteAndDeletionVector()
    {
        try (TestTable table = newTrinoTable("test_delete_v2_v3", "(x int) WITH (format_version = 2)", List.of("1", "2", "3", "4"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES 3, 4");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 3", 1);
            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES 4");
        }
    }

    @Test
    void testTimestampNano()
    {
        testTimestampNano("PARQUET");
        testTimestampNano("ORC");
        testTimestampNano("AVRO");
    }

    private void testTimestampNano(String format)
    {
        try (TestTable table = newTrinoTable("test_nano", "(id int, x timestamp(9)) WITH (format = '" + format + "', format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, timestamp '2022-07-26 12:13:14.123456789')", 1);

            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES timestamp '2022-07-26 12:13:14.123456789'");
            assertThat(query("SELECT 1 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456789'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT 1 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456'"))
                    .returnsEmptyResult();
        }
    }

    @Test
    void testTimestampNanoPartition()
    {
        try (TestTable table = newTrinoTable("test_nano", "(id int, x timestamp(9)) WITH (partitioning = ARRAY['x'], format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, timestamp '2022-07-26 12:13:14.123456789')", 1);

            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES timestamp '2022-07-26 12:13:14.123456789'");
            assertThat(query("SELECT 1 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456789'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456789'"))
                    .isFullyPushedDown();
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testRowLineageWithPreExistingRowId(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_existing_row_id", "(name varchar, _row_id bigint) WITH (format = '" + format + "')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES ('alice', BIGINT '0')", 1);
            assertThat(query("SELECT * FROM " + table.getName())).failure()
                    .hasMessage("Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");
            assertThat(query("SELECT _row_id FROM " + table.getName())).failure()
                    .hasMessage("Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("VALUES (VARCHAR 'alice', CAST(NULL AS bigint), BIGINT '2')");
            assertThat(query("UPDATE " + table.getName() + " SET name = 'BOB' WHERE name = 'alice'")).failure()
                    .hasMessage("Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");
        }
    }

    @ParameterizedTest
    @MethodSource("formatMergeMode")
    void dropRowLineageColumns(IcebergFileFormat format, String mergeMode)
    {
        try (TestTable table = newTrinoTable("test_row_" + mergeMode.replaceAll("-", "_"), "(x varchar, y varchar) WITH (format = '" + format + "')")) {
            Table icebergTable = loadTable(table.getName());
            icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();

            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN \"$row_id\"", "line 1:1: Cannot drop hidden column");
            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN _row_id", "line 1:1: Column '_row_id' does not exist");

            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN \"$last_updated_sequence_number\"", "line 1:1: Cannot drop hidden column");
            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN _last_updated_sequence_number", "line 1:1: Column '_last_updated_sequence_number' does not exist");
        }
    }

    @ParameterizedTest
    @MethodSource("formatMergeMode")
    void testRowLineage(IcebergFileFormat format, String mergeMode)
    {
        // snapshot 1 - create table
        try (TestTable table = newTrinoTable("test_row_" + mergeMode.replaceAll("-", "_"), "(name varchar) WITH (format = '" + format + "')")) {
            Table icebergTable = loadTable(table.getName());
            icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();
            // snapshot 2 - insert alice bob
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'alice', 'bob'", 2);

            // snapshot 3 - insert carol david
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'carol', 'david'", 2);

            // no _row_id in data files at this point, row_lineage fields calculated on the fly
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('bob', 1, 2),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            // snapshot 4 - update bob to BOB, _row_id is written to data file
            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB' WHERE name = 'bob'", 1);

            // expect BOB $last_updated_sequence_number - 4 as from snapshot, who actually updates BOB
            // we keep original row_id $last_updated_sequence_number
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('BOB', 1, 4),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            // expect BOB $last_updated_sequence_number - 5 as from snapshot, who actually updates BOB1
            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB1' WHERE name = 'BOB'", 1);
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('BOB1', 1, 5),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            // no snapshot creation
            assertUpdate(format("COMMENT ON TABLE %s is 'my-table-comment'", table.getName()));
            // lineage values should remain the same
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('BOB1', 1, 5),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);
            // schema evolution - add column info
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN info varchar");

            // lineage values should remain the same
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('BOB1', 1, 5),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);
            assertUpdate("UPDATE " + table.getName() + " SET info = 'info' WHERE name = 'BOB1'", 1);
            assertThat(query("SELECT name, info, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', CAST(NULL AS varchar), BIGINT '0', BIGINT '2'),
                                   ('BOB1', 'info', 1, 6),
                                   ('carol', NULL, 2, 3),
                                   ('david', NULL, 3, 3)
                            """);

            assertThat(query("SELECT name FROM " + table.getName() + " WHERE \"$row_id\" = 2"))
                    .matches("VALUES (VARCHAR 'carol')");

            assertThat(query("SELECT name FROM " + table.getName() + " WHERE \"$last_updated_sequence_number\" = 3"))
                    .matches("VALUES (VARCHAR 'carol'), (VARCHAR 'david')");
        }
    }

    @Test
    void testDirectUpdateRowLineageColumns()
    {
        try (TestTable table = newTrinoTable("test_row_direct_update_", "(name varchar)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'alice', 'bob'", 2);
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('bob', 1, 2)
                            """);

            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB' WHERE name = 'bob'", 1);

            // we can not perform update on metadata columns in the way like "$row_id", it will fail in the same way as for "$path"
            // with QueryFailedException
            assertThat(query("UPDATE " + table.getName() + " SET _row_id = 1 WHERE name = 'bob'")).failure()
                    .hasMessage("line 1:46: The UPDATE SET target column _row_id doesn't exist");
            assertThat(query("UPDATE " + table.getName() + " SET _last_updated_sequence_number = 1 WHERE name = 'bob'")).failure()
                    .hasMessage("line 1:46: The UPDATE SET target column _last_updated_sequence_number doesn't exist");
        }
    }

    @ParameterizedTest
    @MethodSource("formatMergeMode")
    void testRowLineageDelete(IcebergFileFormat format, String mergeMode)
    {
        // snapshot 1 - create table
        try (TestTable table = newTrinoTable("test_row_" + mergeMode.replaceAll("-", "_"), "(name varchar) WITH (format = '" + format + "')")) {
            Table icebergTable = loadTable(table.getName());
            icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();

            // snapshot 2 - insert alice bob
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'alice', 'bob'", 2);

            // snapshot 3 - insert carol david
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'carol', 'david'", 2);

            // no _row_id in data files at this point, row_lineage fields calculated on the fly
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('bob', 1, 2),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            // snapshot 4 - update bob to BOB, _row_id is written to data file
            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB' WHERE name = 'bob'", 1);

            // expect BOB $last_updated_sequence_number - 4 as from snapshot, who actually updates BOB
            // we keep original row_id $last_updated_sequence_number
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('BOB', 1, 4),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE name = 'BOB'", 1);
            // expect BOB $last_updated_sequence_number - 4 as from snapshot, who actually updates BOB
            // we keep original row_id $last_updated_sequence_number
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);
        }
    }

    @ParameterizedTest
    @MethodSource("formatMergeMode")
    void testRowLineageWithOperationsOnRowLineageFields(IcebergFileFormat format, String mergeMode)
    {
        // snapshot 1 - create table
        try (TestTable table = newTrinoTable("test_row_" + mergeMode.replaceAll("-", "_"), "(name varchar) WITH (format = '" + format + "')")) {
            Table icebergTable = loadTable(table.getName());
            icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();

            // snapshot 2 - insert alice bob
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'alice', 'bob'", 2);

            // snapshot 3 - insert carol david
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 'carol', 'david'", 2);

            // no _row_id in data files at this point, row_lineage fields calculated on the fly
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('bob', 1, 2),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            // snapshot 4 - update bob to BOB, _row_id is written to data file
            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB' WHERE \"$row_id\" = 1", 1);

            // expect BOB $last_updated_sequence_number - 4 as from snapshot, who actually updates BOB
            // we keep original row_id $last_updated_sequence_number
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('BOB', 1, 4),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE \"$row_id\" = 1", 1);
            // expect BOB $last_updated_sequence_number - 4 as from snapshot, who actually updates BOB
            // we keep original row_id $last_updated_sequence_number
            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '0', BIGINT '2'),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            assertUpdate("UPDATE " + table.getName() + " SET name = 'ALICE' WHERE \"$last_updated_sequence_number\" = 2", 1);

            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'ALICE', BIGINT '0', BIGINT '6'),
                                   ('carol', 2, 3),
                                   ('david', 3, 3)
                            """);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE \"$last_updated_sequence_number\" = 6", 1);

            assertThat(query("SELECT name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'carol', BIGINT '2', BIGINT '3'),
                                   ('david', 3, 3)
                            """);
        }
    }

    @ParameterizedTest
    @MethodSource("formatMergeMode")
    void testRowLineagePartitioned(IcebergFileFormat format, String mergeMode)
    {
        // snapshot 1 - create table
        try (TestTable table = newTrinoTable("test_row_" + mergeMode.replaceAll("-", "_"), "(name varchar, x bigint) WITH (format = '" + format + "', partitioning = ARRAY['x'])")) {
            Table icebergTable = loadTable(table.getName());
            icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();

            // snapshot 2 - insert alice bob
            assertUpdate("INSERT INTO " + table.getName() + " VALUES ('alice', 1), ('bob', 2)", 2);

            // snapshot 3 - insert carol david
            assertUpdate("INSERT INTO " + table.getName() + " VALUES ('carol', 1), ('david', 2)", 2);

            // no _row_id in data files at this point, row_lineage fields calculated on the fly
            assertThat(query("SELECT name, \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '2'),
                                   ('bob', 2),
                                   ('carol', 3),
                                   ('david', 3)
                            """);

            // we don't know how row_id is assigned between partitioned files, but we know that they are increasing like 0, 1, 2, 3
            assertThat(query("SELECT name FROM " + table.getName() + " WHERE \"$row_id\" IN (0, 1, 2, 3)"))
                    .matches("""
                            VALUES (VARCHAR 'alice'),
                                   ('bob'),
                                   ('carol'),
                                   ('david')
                            """);

            // snapshot 4 - update bob to BOB, _row_id is written to data file
            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB' WHERE name = 'bob'", 1);

            // expect BOB $last_updated_sequence_number - 4 as from snapshot, who actually updates BOB
            // we keep original row_id $last_updated_sequence_number
            assertThat(query("SELECT name, \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '2'),
                                   ('BOB', 4),
                                   ('carol', 3),
                                   ('david', 3)
                            """);

            // we don't know how row_id is assigned between partitioned files, but we know that they are increasing like 0, 1, 2, 3 and should remain the same after update
            assertThat(query("SELECT name FROM " + table.getName() + " WHERE \"$row_id\" IN (0, 1, 2, 3)"))
                    .matches("""
                            VALUES (VARCHAR 'alice'),
                                   ('BOB'),
                                   ('carol'),
                                   ('david')
                            """);

            // expect BOB $last_updated_sequence_number - 5 as from snapshot, who actually updates BOB1
            assertUpdate("UPDATE " + table.getName() + " SET name = 'BOB1' WHERE name = 'BOB'", 1);
            assertThat(query("SELECT name, \"$last_updated_sequence_number\" FROM " + table.getName()))
                    .matches("""
                            VALUES (VARCHAR 'alice', BIGINT '2'),
                                   ('BOB1', 5),
                                   ('carol', 3),
                                   ('david', 3)
                            """);

            // we don't know how row_id is assigned between partitioned files, but we know that they are increasing like 0, 1, 2, 3 and should remain the same after update
            assertThat(query("SELECT name FROM " + table.getName() + " WHERE \"$row_id\" IN (0, 1, 2, 3)"))
                    .matches("""
                            VALUES (VARCHAR 'alice'),
                                   ('BOB1'),
                                   ('carol'),
                                   ('david')
                            """);
        }
    }

    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"DATA", "MATERIALIZED_VIEW_STORAGE"})
    void testRowLineageMetadataTables(TableType tableType)
    {
        try (TestTable table = newTrinoTable("test_row_lineage", "(x int)", List.of("1", "2", "3"))) {
            assertUpdate("UPDATE " + table.getName() + " SET x = 10 WHERE x = 1", 1);

            assertQuerySucceeds("SELECT * FROM \"" + table.getName() + "$" + tableType.name() + "\"");
        }
    }

    public static Stream<Arguments> formatMergeMode()
    {
        return Stream.of(
                Arguments.of(ORC, "copy-on-write"),
                Arguments.of(PARQUET, "copy-on-write"),
                Arguments.of(AVRO, "copy-on-write"),
                Arguments.of(ORC, "merge-on-read"),
                Arguments.of(PARQUET, "merge-on-read"),
                Arguments.of(AVRO, "merge-on-read"));
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUnsupportedRowLineage(IcebergFileFormat format)
    {
        try (TestTable table = newTrinoTable("test_row_lineage", "WITH (format = '" + format + "', format_version = 1) AS SELECT 1 AS x")) {
            assertThat(query("SELECT \"$row_id\" FROM " + table.getName())).failure()
                    .hasMessage("line 1:8: Column '$row_id' cannot be resolved");
            assertThat(query("SELECT \"$last_updated_sequence_number\" FROM " + table.getName())).failure()
                    .hasMessage("line 1:8: Column '$last_updated_sequence_number' cannot be resolved");
        }

        try (TestTable table = newTrinoTable("test_row_lineage", "WITH (format = '" + format + "', format_version = 2) AS SELECT 1 AS x")) {
            assertThat(query("SELECT \"$row_id\" FROM " + table.getName())).failure()
                    .hasMessage("line 1:8: Column '$row_id' cannot be resolved");
            assertThat(query("SELECT \"$last_updated_sequence_number\" FROM " + table.getName())).failure()
                    .hasMessage("line 1:8: Column '$last_updated_sequence_number' cannot be resolved");
        }
    }

    @Test
    void testRowLineageWithViews()
    {
        try (TestTable table = newTrinoTable("test_views", "(id int, name varchar) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'Alice'), (2, 'Bob')", 2);

            String viewName = "test_view_" + randomNameSuffix();
            assertUpdate("CREATE VIEW " + viewName + " AS SELECT id, name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName());

            assertThat(query("SELECT id, name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + viewName))
                    .matches("""
                            VALUES (1, VARCHAR 'Alice', BIGINT '0', BIGINT '2'),
                                   (2, 'Bob', BIGINT '1', BIGINT '2')
                            """);

            assertUpdate("UPDATE " + table.getName() + " SET name = 'Alice Updated' WHERE id = 1", 1);

            assertThat(query("SELECT id, name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + viewName))
                    .matches("""
                            VALUES (1, VARCHAR 'Alice Updated', BIGINT '0', BIGINT '3'),
                                   (2, 'Bob', BIGINT '1', BIGINT '2')
                            """);

            assertUpdate("DROP VIEW " + viewName);
        }
    }

    @Test
    void testRowLineageWithMaterializedViews()
    {
        try (TestTable table = newTrinoTable("test_materialized_views", "(id int, name varchar) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'Alice'), (2, 'Bob')", 2);

            String materializedViewName = "test_materialized_view_" + randomNameSuffix();
            assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT id, name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + table.getName());

            assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 2);

            assertThat(query("SELECT id, name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + materializedViewName))
                    .matches("""
                            VALUES (1, VARCHAR 'Alice', BIGINT '0', BIGINT '2'),
                                   (2, 'Bob', BIGINT '1', BIGINT '2')
                            """);

            assertUpdate("UPDATE " + table.getName() + " SET name = 'Alice Updated' WHERE id = 1", 1);

            assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 2);

            assertThat(query("SELECT id, name, \"$row_id\", \"$last_updated_sequence_number\" FROM " + materializedViewName))
                    .matches("""
                            VALUES (1, VARCHAR 'Alice Updated', BIGINT '0', BIGINT '3'),
                                   (2, 'Bob', BIGINT '1', BIGINT '2')
                            """);

            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        }
    }

    @Test
    void testAddFilesRowLineage()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')");

        assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + icebergTableName))
                .matches("VALUES (BIGINT '0', BIGINT '1', 1), (1, 2, 2)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromTableRowLineage()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + icebergTableName))
                .matches("VALUES (BIGINT '0', BIGINT '1', 1), (1, 2, 2)");

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

        assertThat(query("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')")).failure()
                .hasMessage("Cannot execute add_files procedure when the table contains _row_id column");

        assertThat(query("SELECT \"$row_id\", * FROM " + icebergTableName)).failure()
                .hasMessage("Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");

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

        assertThat(query("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')")).failure()
                .hasMessage("Cannot execute add_files_from_table procedure when the table contains _row_id column");

        assertThat(query("SELECT \"$row_id\", * FROM " + icebergTableName)).failure()
                .hasMessage("Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromTableRowLineagePreExistingColumnOnlyInSourceTable()
    {
        String hiveTableName = "test_add_files_from_table_pre_existing_" + randomNameSuffix();
        String icebergTableName = "test_add_files_from_table_pre_existing_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 3 x, 2 _row_id", 1);
        // make sure _row_id column physically presents in iceberg table
        assertUpdate("UPDATE " + icebergTableName + " SET x = 4 WHERE x = 1", 1);

        assertUpdate("ALTER TABLE hive.tpch." + hiveTableName + " DROP COLUMN _row_id");

        // _row_id column is metadata colum, despite it presents physically in data file
        assertThat(query("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')")).failure()
                .hasMessage("Failed to add files: ORC column OrcType{orcTypeKind=INT, fieldTypeIndexes=[], fieldNames=[]} doesn't have an associated Iceberg ID");

        assertThat(query("SELECT \"$row_id\", * FROM " + icebergTableName)).matches("VALUES (BIGINT '0', 4)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesRowLineagePreExistingColumnOnlyInSourceTable()
    {
        String hiveTableName = "test_add_files_pre_existing_" + randomNameSuffix();
        String icebergTableName = "test_add_files_pre_existing_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 3 x, 2 _row_id", 1);
        // make sure _row_id column physically presents in iceberg table
        assertUpdate("UPDATE " + icebergTableName + " SET x = 4 WHERE x = 1", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertThat(query("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')")).failure()
                .hasMessage("Failed to add files: ORC column OrcType{orcTypeKind=INT, fieldTypeIndexes=[], fieldNames=[]} doesn't have an associated Iceberg ID");

        assertThat(query("SELECT \"$row_id\", * FROM " + icebergTableName)).matches("VALUES (BIGINT '0', 4)");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    public void testMigrateTable(IcebergFileFormat fileFormat)
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format='" + fileFormat + "')  AS SELECT 1 x, 2 _row_id", 1);
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat((String) computeScalar("SHOW CREATE TABLE " + icebergTableName))
                .contains("format = '%s'".formatted(fileFormat));

        assertThat(query("SELECT x, _row_id FROM " + icebergTableName)).failure()
                .hasMessage("Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("mergeMode")
    public void testOptimize(String mergeMode)
            throws Exception
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar)");
        Table icebergTable = loadTable(tableName);
        icebergTable.updateProperties().set("write.merge.mode", mergeMode).commit();

        // DistributedQueryRunner sets node-scheduler.include-coordinator by default, so include coordinator
        int workerCount = getQueryRunner().getNodeCount();

        assertThat(getActiveFiles(tableName)).isEmpty();

        assertUpdate("INSERT INTO " + tableName + " VALUES (0, 'zero'), (1, 'one')", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'four')", 1);

        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles)
                .hasSize(4)
                // Verify we have sufficiently many test rows with respect to worker count.
                .hasSizeGreaterThan(workerCount);

        assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", key, value FROM " + tableName))
                .matches("""
                        VALUES (BIGINT '0', BIGINT '2', 0, VARCHAR 'zero'),
                               (1, 2, 1, 'one'),
                               (2, 3, 2, 'two'),
                               (3, 4, 3, 'three'),
                               (4, 5, 4, 'four')
                        """);

        assertUpdate("UPDATE " + tableName + " SET value = 'zero update' WHERE key = 0", 1);
        assertUpdate("UPDATE " + tableName + " SET value = 'four update' WHERE key = 4", 1);

        assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", key, value FROM " + tableName))
                .matches("""
                        VALUES (BIGINT '0', BIGINT '6', 0, VARCHAR 'zero update'),
                               (1, 2, 1, 'one'),
                               (2, 3, 2, 'two'),
                               (3, 4, 3, 'three'),
                               (4, 7, 4, 'four update')
                        """);

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles)
                .hasSize(1);

        assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", key, value FROM " + tableName))
                .matches("""
                        VALUES (BIGINT '0', BIGINT '6', 0, VARCHAR 'zero update'),
                               (1, 2, 1, 'one'),
                               (2, 3, 2, 'two'),
                               (3, 4, 3, 'three'),
                               (4, 7, 4, 'four update')
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    public static Stream<Arguments> mergeMode()
    {
        return Stream.of(
                Arguments.of("merge-on-read"),
                Arguments.of("copy-on-write"));
    }

    @Test
    void testMergeMultipleOperations()
    {
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, zipcode INT, purchase INT)", targetTable));

        assertUpdate(format("""
                INSERT INTO %s (customer, zipcode, purchase)
                        VALUES ('joe_0', 91000, 0),
                               ('joe_1', 91000, 1),
                               ('joe_2', 92000, 2),
                               ('joe_3', 92000, 3)
                """, targetTable), 4);

        assertQuery(
                "SELECT customer, zipcode, purchase, \"$row_id\", \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_0', 91000, 0, 0, 2),
                               ('joe_1', 91000, 1, 1, 2),
                               ('joe_2', 92000, 2, 2, 2),
                               ('joe_3', 92000, 3, 3, 2)
                        """);

        assertUpdate(format("MERGE INTO %s t USING (VALUES ('joe_2', 83000, 2), ('joe_3', 83000, 3)) AS s(customer, zipcode, purchase)", targetTable) +
                     "    ON t.customer = s.customer" +
                     "    WHEN MATCHED THEN UPDATE SET purchase = s.purchase, zipcode = s.zipcode",
                2);

        assertQuery(
                "SELECT customer, zipcode, purchase, \"$row_id\", \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_0', 91000, 0, 0, 2),
                               ('joe_1', 91000, 1, 1, 2),
                               ('joe_2', 83000, 2, 2, 3),
                               ('joe_3', 83000, 3, 3, 3)
                        """);

        assertUpdate(format("INSERT INTO %s (customer, zipcode, purchase) VALUES ('joe_4', 74000, 4), ('joe_5', 74000, 5)", targetTable), 2);

        // we keep original _row_id for updated rows, but new rows get new _row_id - increasing but mandatory continuous
        assertQuery(
                "SELECT customer, zipcode, purchase, \"$row_id\", \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_0', 91000, 0, 0, 2),
                               ('joe_1', 91000, 1, 1, 2),
                               ('joe_2', 83000, 2, 2, 3),
                               ('joe_3', 83000, 3, 3, 3),
                               ('joe_4', 74000, 4, 6, 4),
                               ('joe_5', 74000, 5, 7, 4)
                        """);

        assertUpdate(format("MERGE INTO %s t USING (VALUES ('joe_0', 85000, 0), ('joe_1', 85000, 1), ('joe_2', 85000, 2), ('joe_3', 85000, 3), ('joe_4', 85000, 4), ('joe_6', 85000, 6)) AS s(customer, zipcode, purchase)", targetTable) +
                     "    ON t.customer = s.customer" +
                     "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                     "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                     "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode" +
                     "    WHEN NOT MATCHED THEN INSERT (customer, zipcode, purchase) VALUES(s.customer, s.zipcode, s.purchase)",
                6);
        // we keep original _row_id for updated rows, but new rows get new _row_id - increasing but mandatory sequential
        assertQuery(
                "SELECT customer, zipcode, purchase, \"$row_id\", \"$last_updated_sequence_number\" FROM " + targetTable + " WHERE \"$row_id\" < 8",
                """
                        VALUES ('joe_2', 60000, 2, 2, 5),
                               ('joe_3', 60000, 3, 3, 5),
                               ('joe_4', 60000, 4, 6, 5),
                               ('joe_5', 74000, 5, 7, 4)
                        """);

        // The new added row we just know the _row_id is greater than 7, but we don't know the exact value
        assertThat(query("SELECT customer, zipcode, purchase, \"$last_updated_sequence_number\" FROM " + targetTable + " WHERE \"$row_id\" >= 8"))
                .matches("VALUES (varchar 'joe_6', 85000, 6, BIGINT '5')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    void testMergeMultipleOperationsPartitioned()
    {
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, zipcode INT, purchase INT) WITH (partitioning = ARRAY['purchase'])", targetTable));

        // joe_0 and joe_1 goes to the same partition
        assertUpdate(format("""
                INSERT INTO %s (customer, zipcode, purchase)
                        VALUES ('joe_0', 91000, 0),
                               ('joe_1', 91000, 0),
                               ('joe_2', 92000, 2),
                               ('joe_3', 92000, 3)
                """, targetTable), 4);

        assertQuery(
                "SELECT customer, zipcode, purchase, \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_0', 91000, 0, 2),
                               ('joe_1', 91000, 0, 2),
                               ('joe_2', 92000, 2, 2),
                               ('joe_3', 92000, 3, 2)
                        """);

        // we don't know how row_id is assigned between partitioned files, but we know that they are increasing like 0, 1, 2, 3
        assertThat(query("SELECT customer FROM " + targetTable + " WHERE \"$row_id\" IN (0, 1, 2, 3)"))
                .matches("""
                        VALUES (VARCHAR 'joe_0'),
                               ('joe_1'),
                               ('joe_2'),
                               ('joe_3')
                        """);

        assertUpdate(format("MERGE INTO %s t USING (VALUES ('joe_2', 83000, 2), ('joe_3', 83000, 3)) AS s(customer, zipcode, purchase)", targetTable) +
                     "    ON t.customer = s.customer" +
                     "    WHEN MATCHED THEN UPDATE SET purchase = s.purchase, zipcode = s.zipcode",
                2);

        assertQuery(
                "SELECT customer, zipcode, purchase, \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_0', 91000, 0, 2),
                               ('joe_1', 91000, 0, 2),
                               ('joe_2', 83000, 2, 3),
                               ('joe_3', 83000, 3, 3)
                        """);

        // we don't know how row_id is assigned between partitioned files, but we know that they are increasing like 0, 1, 2, 3 and should remain the same after update
        assertThat(query("SELECT customer FROM " + targetTable + " WHERE \"$row_id\" IN (0, 1, 2, 3)"))
                .matches("""
                        VALUES (VARCHAR 'joe_0'),
                               ('joe_1'),
                               ('joe_2'),
                               ('joe_3')
                        """);

        assertUpdate(format("INSERT INTO %s (customer, zipcode, purchase) VALUES ('joe_4', 74000, 4), ('joe_5', 74000, 5)", targetTable), 2);

        // we keep original _row_id for updated rows, but new rows get new _row_id - increasing but mandatory continuous
        assertQuery(
                "SELECT customer, zipcode, purchase, \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_0', 91000, 0, 2),
                               ('joe_1', 91000, 0, 2),
                               ('joe_2', 83000, 2, 3),
                               ('joe_3', 83000, 3, 3),
                               ('joe_4', 74000, 4, 4),
                               ('joe_5', 74000, 5, 4)
                        """);

        // we don't know how row_id is assigned between partitioned files, but we know that they are increasing like 0, 1, 2, 3 and should remain the same after update
        assertThat(query("SELECT customer FROM " + targetTable + " WHERE \"$row_id\" IN (0, 1, 2, 3, 6, 7)"))
                .matches("""
                        VALUES (VARCHAR 'joe_0'),
                               ('joe_1'),
                               ('joe_2'),
                               ('joe_3'),
                               ('joe_4'),
                               ('joe_5')
                        """);

        assertUpdate(format("MERGE INTO %s t USING (VALUES ('joe_0', 85000, 0), ('joe_1', 85000, 1), ('joe_2', 85000, 2), ('joe_3', 85000, 3), ('joe_4', 85000, 4), ('joe_6', 85000, 6)) AS s(customer, zipcode, purchase)", targetTable) +
                     "    ON t.customer = s.customer" +
                     "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                     "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                     "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode" +
                     "    WHEN NOT MATCHED THEN INSERT (customer, zipcode, purchase) VALUES(s.customer, s.zipcode, s.purchase)",
                6);
        // we keep original _row_id for updated rows, but new rows get new _row_id - increasing but mandatory sequential
        assertQuery(
                "SELECT customer, zipcode, purchase, \"$last_updated_sequence_number\" FROM " + targetTable,
                """
                        VALUES ('joe_2', 60000, 2, 5),
                               ('joe_3', 60000, 3, 5),
                               ('joe_4', 60000, 4, 5),
                               ('joe_5', 74000, 5, 4),
                               ('joe_6', 85000, 6, 5)
                        """);

        // The new added row we just know the _row_id is greater than 7, but we don't know the exact value
        assertThat(query("SELECT customer, zipcode, purchase, \"$last_updated_sequence_number\" FROM " + targetTable + " WHERE \"$row_id\" >= 8"))
                .matches("VALUES (varchar 'joe_6', 85000, 6, BIGINT '5')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    void testTimeTravelRowLineage()
            throws Exception
    {
        String tableName = "test_iceberg_read_versioned_table_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s(a_string varchar, an_integer integer)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES ('a', 1)", tableName), 1);
        long v1SnapshotId = getLatestSnapshotId(tableName);
        long v1EpochMillis = getCommittedAtInEpochMilliSeconds(tableName, v1SnapshotId);
        Thread.sleep(1);
        assertUpdate(format("INSERT INTO %s VALUES ('b', 2)", tableName), 1);
        long v2SnapshotId = getLatestSnapshotId(tableName);
        long v2EpochMillis = getCommittedAtInEpochMilliSeconds(tableName, v2SnapshotId);
        Thread.sleep(1);
        assertUpdate(format("UPDATE %s SET an_integer = 3 WHERE a_string = 'b'", tableName), 1);
        long v3SnapshotId = getLatestSnapshotId(tableName);
        long v3EpochMillis = getCommittedAtInEpochMilliSeconds(tableName, v3SnapshotId);

        assertThat(query(format("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM %s FOR VERSION AS OF %s", tableName, v1SnapshotId)))
                .matches("VALUES (BIGINT '0', BIGINT '2', VARCHAR 'a', 1)");
        assertThat(query(format("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM %s FOR VERSION AS OF %s", tableName, v2SnapshotId)))
                .matches("VALUES (BIGINT '0', BIGINT '2', VARCHAR 'a', 1), (1, 3, 'b', 2)");
        assertThat(query(format("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM %s FOR VERSION AS OF %s", tableName, v3SnapshotId)))
                .matches("VALUES (BIGINT '0', BIGINT '2', VARCHAR 'a', 1), (1, 4, 'b', 3)");

        assertThat(query(format("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM %s FOR TIMESTAMP AS OF %s", tableName, timestampLiteral(v1EpochMillis, 9))))
                .matches("VALUES (BIGINT '0', BIGINT '2', VARCHAR 'a', 1)");
        assertThat(query(format("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM %s FOR TIMESTAMP AS OF %s", tableName, timestampLiteral(v2EpochMillis, 9))))
                .matches("VALUES (BIGINT '0', BIGINT '2', VARCHAR 'a', 1), (1, 3, 'b', 2)");
        assertThat(query(format("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM %s FOR TIMESTAMP AS OF %s", tableName, timestampLiteral(v3EpochMillis, 9))))
                .matches("VALUES (BIGINT '0', BIGINT '2', VARCHAR 'a', 1), (1, 4, 'b', 3)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSortedBy()
    {
        try (TestTable table = newTrinoTable("test_sorted_by", "(id int, x varchar) WITH (sorted_by = ARRAY['x'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (3, 'c'), (2, 'b')", 3);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName() + " ORDER BY x"))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 1, VARCHAR 'a'),
                                   (1, 2, 2, 'b'),
                                   (2, 2, 3, 'c')
                            """);
            assertUpdate(format("UPDATE %s SET id = 11 WHERE id = 1", table.getName()), 1);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName() + " ORDER BY x"))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '3', 11, VARCHAR 'a'),
                                   (1, 2, 2, 'b'),
                                   (2, 2, 3, 'c')
                            """);
        }
        assertQueryFails("CREATE TABLE test_sorted_by_invalid (id int, x varchar) WITH (sorted_by = ARRAY['$row_id'])", "Unable to parse sort field: \\[\\$row_id\\]");
    }

    @Test
    void testEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_equality_deletes", "AS SELECT * FROM tpch.tiny.nation")) {
            assertUpdate(format("UPDATE %s SET regionkey = 333 WHERE regionkey = 1", table.getName()), 5);
            assertUpdate(format("UPDATE %s SET comment = 'some comment' WHERE regionkey = 2", table.getName()), 5);
            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", name FROM " + table.getName() + " WHERE regionkey = 2"))
                    .matches("""
                            VALUES (BIGINT '12', BIGINT '3', VARCHAR 'JAPAN'),
                                   (9, 3, 'INDONESIA'),
                                   (8, 3, 'INDIA'),
                                   (21, 3, 'VIETNAM'),
                                   (18, 3, 'CHINA')
                            """);
            BaseTable icebergTable = loadTable(table.getName());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("regionkey", 333L), Optional.empty());

            assertThat(query("SELECT name, regionkey FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT name, regionkey FROM tpch.tiny.nation WHERE regionkey != 1");
            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", name FROM " + table.getName() + " WHERE regionkey = 2"))
                    .matches("""
                            VALUES (BIGINT '12', BIGINT '3', VARCHAR 'JAPAN'),
                                   (9, 3, 'INDONESIA'),
                                   (8, 3, 'INDIA'),
                                   (21, 3, 'VIETNAM'),
                                   (18, 3, 'CHINA')
                            """);
        }
    }

    @Test
    void testOptimizeManifests()
    {
        try (TestTable table = newTrinoTable("test_optimize_manifests", "(x int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 1),
                                   (1, 3, 2)
                            """);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 1),
                                   (1, 3, 2)
                            """);
        }
    }

    @Test
    void testOptimizeManifestsWithUpdate()
    {
        try (TestTable table = newTrinoTable("test_optimize_manifests", "(x int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            assertUpdate(format("UPDATE %s SET x = 3 WHERE x = 2", table.getName()), 1);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 1),
                                   (1, 4, 3)
                            """);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(4);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(2);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 1),
                                   (1, 4, 3)
                            """);
        }
    }

    @Test
    public void testBranchRowLineage()
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 0", 1);
            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 0)
                            """);
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES 1, 2, 3", 3);

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 0),
                                   (1, 3, 1),
                                   (2, 3, 2),
                                   (3, 3, 3)
                            """);

            assertUpdate("UPDATE " + table.getName() + " @ dev SET x = x * 2", 4);
            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '2', 0)
                            """);
            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '4', 0),
                                   (1, 4, 2),
                                   (2, 4, 4),
                                   (3, 4, 6)
                            """);

            assertUpdate("ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO dev");

            assertThat(query("SELECT \"$row_id\", \"$last_updated_sequence_number\", * FROM " + table.getName()))
                    .matches("""
                            VALUES (BIGINT '0', BIGINT '4', 0),
                                   (1, 4, 2),
                                   (2, 4, 4),
                                   (3, 4, 6)
                            """);
        }
    }

    @Test
    public void testHourTransformTimestampNano()
    {
        assertUpdate("CREATE TABLE test_hour_transform_timestamp (d TIMESTAMP(9), b BIGINT) WITH (partitioning = ARRAY['hour(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-31 22:59:59.999999999', 1)," +
                "(TIMESTAMP '1969-12-31 23:00:00.000000000', 2)," +
                "(TIMESTAMP '1970-01-01 00:00:00.000000000', 3)," +
                "(TIMESTAMP '1970-01-01 00:59:59.999999999', 4)," +
                "(TIMESTAMP '2015-01-01 09:59:59.999999999', 5)," +
                "(TIMESTAMP '2015-01-01 10:00:00.000000000', 6)," +
                "(TIMESTAMP '2015-01-01 10:30:00.123456789', 7)," +
                "(TIMESTAMP '2015-01-01 11:00:00.000000000', 8)," +
                "(TIMESTAMP '2015-05-15 13:15:00.000000000', 9)," +
                "(TIMESTAMP '2015-05-15 14:45:00.000000000', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210', 11)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321000', 12)";
        assertUpdate("INSERT INTO test_hour_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_hour_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(-2, 1, TIMESTAMP '1969-12-31 22:59:59.999999999', TIMESTAMP '1969-12-31 22:59:59.999999999', 1, 1), " +
                "(-1, 1, TIMESTAMP '1969-12-31 23:00:00.000000000', TIMESTAMP '1969-12-31 23:00:00.000000000', 2, 2), " +
                "(0, 2, TIMESTAMP '1970-01-01 00:00:00.000000000', TIMESTAMP '1970-01-01 00:59:59.999999999', 3, 4), " +
                "(394473, 1, TIMESTAMP '2015-01-01 09:59:59.999999999', TIMESTAMP '2015-01-01 09:59:59.999999999', 5, 5), " +
                "(394474, 2, TIMESTAMP '2015-01-01 10:00:00.000000000', TIMESTAMP '2015-01-01 10:30:00.123456789', 6, 7), " +
                "(394475, 1, TIMESTAMP '2015-01-01 11:00:00.000000000', TIMESTAMP '2015-01-01 11:00:00.000000000', 8, 8), " +
                "(397693, 1, TIMESTAMP '2015-05-15 13:15:00.000000000', TIMESTAMP '2015-05-15 13:15:00.000000000', 9, 9), " +
                "(397694, 1, TIMESTAMP '2015-05-15 14:45:00.000000000', TIMESTAMP '2015-05-15 14:45:00.000000000', 10, 10), " +
                "(439527, 1, TIMESTAMP '2020-02-21 15:11:11.876543210', TIMESTAMP '2020-02-21 15:11:11.876543210', 11, 11), " +
                "(439528, 1, TIMESTAMP '2020-02-21 16:12:12.654321000', TIMESTAMP '2020-02-21 16:12:12.654321000', 12, 12)";

        assertQuery("SELECT partition.d_hour, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_hour_transform_timestamp$partitions\"", expected);
        String expectedStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-12-31 22:59:59.999999', '2020-02-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        // Pushdown checks
        assertThat(query("SHOW STATS FOR test_hour_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedStats);

        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 14:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE CAST(d AS timestamp(9)) >= TIMESTAMP '2015-05-15 14:00:00'"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 14:00:00.000000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date_trunc
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE date_trunc('hour', d) = TIMESTAMP '2015-05-15 14:00:00'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_hour_transform_timestamp");
    }

    @Test
    public void testHourTransformTimestampNanoWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_hour_transform_timestamptz (d timestamp(9) with time zone, b integer) WITH (partitioning = ARRAY['hour(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-31 22:59:59.999999999 UTC', 1)," +
                "(TIMESTAMP '1969-12-31 23:00:00.000000000 UTC', 2)," +
                "(TIMESTAMP '1970-01-01 00:00:00.000000000 UTC', 3)," +
                "(TIMESTAMP '1970-01-01 00:59:59.999999999 UTC', 4)," +
                "(TIMESTAMP '2015-01-01 09:59:59.999999999 UTC', 5)," +
                "(TIMESTAMP '2015-01-01 10:00:00.000000000 UTC', 6)," +
                "(TIMESTAMP '2015-01-01 10:30:00.123456789 UTC', 7)," +
                "(TIMESTAMP '2015-01-01 11:00:00.000000000 UTC', 8)," +
                "(TIMESTAMP '2015-05-15 13:15:00.000000000 UTC', 9)," +
                "(TIMESTAMP '2015-05-15 14:45:00.000000000 UTC', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', 11)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', 12)";
        assertUpdate("INSERT INTO test_hour_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz")).matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(-2, 1, TIMESTAMP '1969-12-31 22:59:59.999999999 UTC', TIMESTAMP '1969-12-31 22:59:59.999999999 UTC', 1, 1), " +
                "(-1, 1, TIMESTAMP '1969-12-31 23:00:00.000000000 UTC', TIMESTAMP '1969-12-31 23:00:00.000000000 UTC', 2, 2), " +
                "(0, 2, TIMESTAMP '1970-01-01 00:00:00.000000000 UTC', TIMESTAMP '1970-01-01 00:59:59.999999999 UTC', 3, 4), " +
                "(394473, 1, TIMESTAMP '2015-01-01 09:59:59.999999999 UTC', TIMESTAMP '2015-01-01 09:59:59.999999999 UTC', 5, 5), " +
                "(394474, 2, TIMESTAMP '2015-01-01 10:00:00.000000000 UTC', TIMESTAMP '2015-01-01 10:30:00.123456789 UTC', 6, 7), " +
                "(394475, 1, TIMESTAMP '2015-01-01 11:00:00.000000000 UTC', TIMESTAMP '2015-01-01 11:00:00.000000000 UTC', 8, 8), " +
                "(397693, 1, TIMESTAMP '2015-05-15 13:15:00.000000000 UTC', TIMESTAMP '2015-05-15 13:15:00.000000000 UTC', 9, 9), " +
                "(397694, 1, TIMESTAMP '2015-05-15 14:45:00.000000000 UTC', TIMESTAMP '2015-05-15 14:45:00.000000000 UTC', 10, 10), " +
                "(439527, 1, TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', 11, 11), " +
                "(439528, 1, TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', 12, 12)";
        assertThat(query("SELECT partition.d_hour, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_hour_transform_timestamptz$partitions\""))
                .matches(expected);

        String expectedStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.07692307692307693, NULL, '1969-12-31 22:59:59.999 UTC', '2020-02-21 16:12:12.654 UTC'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        assertThat(query("SHOW STATS FOR test_hour_transform_timestamptz"))
                .skippingTypesCheck()
                .matches(expectedStats);

        // Pushdown checks
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 14:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 14:00:00.000000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date_trunc
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE date_trunc('hour', d) = TIMESTAMP '2015-05-15 14:00:00 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_hour_transform_timestamptz");
    }

    @Test
    public void testDayTransformTimestampNano()
    {
        assertUpdate("CREATE TABLE test_day_transform_timestamp (d TIMESTAMP(9), b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-25 15:13:12.876543210', 8)," +
                "(TIMESTAMP '1969-12-30 18:47:33.345678900', 9)," +
                "(TIMESTAMP '1969-12-31 00:00:00.000000000', 10)," +
                "(TIMESTAMP '1969-12-31 05:06:07.234567890', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789000', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456789', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654321', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789000', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567890', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678900', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321000', 7)";
        assertUpdate("INSERT INTO test_day_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_day_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876543210', TIMESTAMP '1969-12-25 15:13:12.876543210', 8, 8), " +
                "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345678900', TIMESTAMP '1969-12-30 18:47:33.345678900', 9, 9), " +
                "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000000', TIMESTAMP '1969-12-31 05:06:07.234567890', 10, 11), " +
                "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456789000', TIMESTAMP '1970-01-01 12:03:08.456789000', 12, 12), " +
                "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123456789', TIMESTAMP '2015-01-01 12:55:00.456789000', 1, 3), " +
                "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234567890', TIMESTAMP '2015-05-15 14:21:02.345678900', 4, 5), " +
                "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876543210', TIMESTAMP '2020-02-21 16:12:12.654321000', 6, 7)";
        String expectedTimestampStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-12-25 15:13:12.876543', '2020-02-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        assertQuery("SELECT partition.d_day, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_day_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_day_transform_timestamp WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-31 00:00:00.000000000', 10)");

        assertThat(query("SHOW STATS FOR test_day_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedTimestampStats);

        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d >= DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-15'"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 00:00:00.000000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date()
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date(d) = DATE '2015-05-15'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date_trunc('day', d) = DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_day_transform_timestamp");
    }

    @Test
    public void testDayTransformTimestampNanoWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_day_transform_timestamptz (d timestamp(9) with time zone, b integer) WITH (partitioning = ARRAY['day(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-25 15:13:12.876543210 UTC', 8)," +
                "(TIMESTAMP '1969-12-30 18:47:33.345678900 UTC', 9)," +
                "(TIMESTAMP '1969-12-31 00:00:00.000000000 UTC', 10)," +
                "(TIMESTAMP '1969-12-31 05:06:07.234567890 UTC', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789000 UTC', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456789 UTC', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654321 UTC', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789000 UTC', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567890 UTC', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678900 UTC', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', 7)";
        assertUpdate("INSERT INTO test_day_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_day_transform_timestamptz"))
                .matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876543210 UTC', TIMESTAMP '1969-12-25 15:13:12.876543210 UTC', 8, 8), " +
                "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345678900 UTC', TIMESTAMP '1969-12-30 18:47:33.345678900 UTC', 9, 9), " +
                "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000000 UTC', TIMESTAMP '1969-12-31 05:06:07.234567890 UTC', 10, 11), " +
                "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456789000 UTC', TIMESTAMP '1970-01-01 12:03:08.456789000 UTC', 12, 12), " +
                "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123456789 UTC', TIMESTAMP '2015-01-01 12:55:00.456789000 UTC', 1, 3), " +
                "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234567890 UTC', TIMESTAMP '2015-05-15 14:21:02.345678900 UTC', 4, 5), " +
                "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', 6, 7)";
        String expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, '1969-12-25 15:13:12.876 UTC', '2020-02-21 16:12:12.654 UTC'";
        String expectedIntegerStats = "NULL, 13e0, 0e0, NULL, '1', '101'";

        assertThat(query("SELECT partition.d_day, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_day_transform_timestamptz$partitions\""))
                .skippingTypesCheck()
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE day_of_week(d) = 3 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '1969-12-31 00:00:00.000000000 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_day_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedIntegerStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        // Tests run with non-UTC session, so timestamp_tz > a_date will not align with partition boundaries. Use with_timezone to align it.
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d >= with_timezone(DATE '2015-05-15', 'UTC')"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-15' AND d < TIMESTAMP '2015-05-15 02:00:00 Europe/Warsaw'"))
                // Engine can eliminate the table scan after connector accepts the filter pushdown
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)))
                .returnsEmptyResult();

        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 00:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 00:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date()
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date(d) = DATE '2015-05-15'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date_trunc('day', d) = TIMESTAMP '2015-05-15 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date_trunc('month', d) = TIMESTAMP '2015-05-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_day_transform_timestamptz");
    }

    @Test
    public void testMonthTransformTimestampNano()
    {
        assertUpdate("CREATE TABLE test_month_transform_timestamp (d TIMESTAMP(9), b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-11-15 15:13:12.876543210', 8)," +
                "(TIMESTAMP '1969-11-19 18:47:33.345678900', 9)," +
                "(TIMESTAMP '1969-12-01 00:00:00.000000000', 10)," +
                "(TIMESTAMP '1969-12-01 05:06:07.234567890', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789000', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456789', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654321', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789000', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567890', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678900', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321000', 7)";
        assertUpdate("INSERT INTO test_month_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_month_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(-2, 2,  TIMESTAMP '1969-11-15 15:13:12.876543210', TIMESTAMP '1969-11-19 18:47:33.345678900', 8, 9), " +
                "(-1, 2,  TIMESTAMP '1969-12-01 00:00:00.000000000', TIMESTAMP '1969-12-01 05:06:07.234567890', 10, 11), " +
                "(0,  1,  TIMESTAMP '1970-01-01 12:03:08.456789000', TIMESTAMP '1970-01-01 12:03:08.456789000', 12, 12), " +
                "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123456789', TIMESTAMP '2015-01-01 12:55:00.456789000', 1, 3), " +
                "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234567890', TIMESTAMP '2015-05-15 14:21:02.345678900', 4, 5), " +
                "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876543210', TIMESTAMP '2020-02-21 16:12:12.654321000', 6, 7)";
        String expectedTimestampStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-11-15 15:13:12.876543', '2020-02-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        assertQuery("SELECT partition.d_month, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_month_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_month_transform_timestamp WHERE day_of_week(d) = 1 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-01 00:00:00.000000000', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedTimestampStats);

        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= DATE '2015-05-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-02'"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= TIMESTAMP '2015-05-01 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= TIMESTAMP '2015-05-01 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_month_transform_timestamp");
    }

    @Test
    public void testMonthTransformTimestampNanoWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_month_transform_timestamptz (d timestamp(9) with time zone, b integer) WITH (partitioning = ARRAY['month(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-11-15 15:13:12.876543210 UTC', 8)," +
                "(TIMESTAMP '1969-11-19 18:47:33.345678900 UTC', 9)," +
                "(TIMESTAMP '1969-12-01 00:00:00.000000000 UTC', 10)," +
                "(TIMESTAMP '1969-12-01 05:06:07.234567890 UTC', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789000 UTC', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456789 UTC', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654321 UTC', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789000 UTC', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567890 UTC', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678900 UTC', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', 7)";
        assertUpdate("INSERT INTO test_month_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_month_transform_timestamptz"))
                .matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876543210 UTC', TIMESTAMP '1969-11-19 18:47:33.345678900 UTC', 8, 9), " +
                "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000000000 UTC', TIMESTAMP '1969-12-01 05:06:07.234567890 UTC', 10, 11), " +
                "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456789000 UTC', TIMESTAMP '1970-01-01 12:03:08.456789000 UTC', 12, 12), " +
                "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123456789 UTC', TIMESTAMP '2015-01-01 12:55:00.456789000 UTC', 1, 3), " +
                "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234567890 UTC', TIMESTAMP '2015-05-15 14:21:02.345678900 UTC', 4, 5), " +
                "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', TIMESTAMP '2020-02-21 16:12:12.654321000 UTC', 6, 7)";
        String expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, '1969-11-15 15:13:12.876 UTC', '2020-02-21 16:12:12.654 UTC'";
        String expectedIntegerStats = "NULL, 13e0, 0e0, NULL, '1', '101'";

        assertThat(query("SELECT partition.d_month, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_month_transform_timestamptz$partitions\""))
                .skippingTypesCheck()
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE day_of_week(d) = 1 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '1969-12-01 00:00:00.000000000 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedIntegerStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        // Tests run with non-UTC session, so timestamp_tz > a_date will not align with partition boundaries. Use with_timezone to align it.
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= with_timezone(DATE '2015-05-01', 'UTC')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= with_timezone(DATE '2015-05-02', 'UTC')"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-01' AND d < TIMESTAMP '2015-05-01 02:00:00 Europe/Warsaw'"))
                // Engine can eliminate the table scan after connector accepts the filter pushdown
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)))
                .returnsEmptyResult();

        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-01 00:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-01 00:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE date_trunc('month', d) = TIMESTAMP '2015-05-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_month_transform_timestamptz");
    }

    @Test
    public void testYearTransformTimestampNano()
    {
        assertUpdate("CREATE TABLE test_year_transform_timestamp (d TIMESTAMP(9), b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1968-03-15 15:13:12.876543210', 1)," +
                "(TIMESTAMP '1968-11-19 18:47:33.345678900', 2)," +
                "(TIMESTAMP '1969-01-01 00:00:00.000000000', 3)," +
                "(TIMESTAMP '1969-01-01 05:06:07.234567890', 4)," +
                "(TIMESTAMP '1970-01-18 12:03:08.456789000', 5)," +
                "(TIMESTAMP '1970-03-14 10:01:23.123456789', 6)," +
                "(TIMESTAMP '1970-08-19 11:10:02.987654321', 7)," +
                "(TIMESTAMP '1970-12-31 12:55:00.456789000', 8)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567890', 9)," +
                "(TIMESTAMP '2015-09-15 14:21:02.345678900', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210', 11)," +
                "(TIMESTAMP '2020-08-21 16:12:12.654321000', 12)";
        assertUpdate("INSERT INTO test_year_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_year_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876543210', TIMESTAMP '1968-11-19 18:47:33.345678900', 1, 2), " +
                "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000000', TIMESTAMP '1969-01-01 05:06:07.234567890', 3, 4), " +
                "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456789000', TIMESTAMP '1970-12-31 12:55:00.456789000', 5, 8), " +
                "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234567890', TIMESTAMP '2015-09-15 14:21:02.345678900', 9, 10), " +
                "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876543210', TIMESTAMP '2020-08-21 16:12:12.654321000', 11, 12)";
        String expectedTimestampStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1968-03-15 15:13:12.876543', '2020-08-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        assertQuery("SELECT partition.d_year, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_year_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_year_transform_timestamp WHERE day_of_week(d) = 2 AND b % 7 = 3",
                "VALUES (TIMESTAMP '2015-09-15 14:21:02.345678900', 10)");

        assertThat(query("SHOW STATS FOR test_year_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedTimestampStats);

        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= TIMESTAMP '2015-01-01 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= TIMESTAMP '2015-01-01 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_year_transform_timestamp");
    }

    @Test
    public void testYearTransformTimestampNanoWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_year_transform_timestamptz (d timestamp(9) with time zone, b integer) WITH (partitioning = ARRAY['year(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1968-03-15 15:13:12.876543210 UTC', 1)," +
                "(TIMESTAMP '1968-11-19 18:47:33.345678900 UTC', 2)," +
                "(TIMESTAMP '1969-01-01 00:00:00.000000000 UTC', 3)," +
                "(TIMESTAMP '1969-01-01 05:06:07.234567890 UTC', 4)," +
                "(TIMESTAMP '1970-01-18 12:03:08.456789000 UTC', 5)," +
                "(TIMESTAMP '1970-03-14 10:01:23.123456789 UTC', 6)," +
                "(TIMESTAMP '1970-08-19 11:10:02.987654321 UTC', 7)," +
                "(TIMESTAMP '1970-12-31 12:55:00.456789000 UTC', 8)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567890 UTC', 9)," +
                "(TIMESTAMP '2015-09-15 14:21:02.345678900 UTC', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', 11)," +
                "(TIMESTAMP '2020-08-21 16:12:12.654321000 UTC', 12)";
        assertUpdate("INSERT INTO test_year_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_year_transform_timestamptz"))
                .matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876543210 UTC', TIMESTAMP '1968-11-19 18:47:33.345678900 UTC', 1, 2), " +
                "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000000 UTC', TIMESTAMP '1969-01-01 05:06:07.234567890 UTC', 3, 4), " +
                "(0, 4,  TIMESTAMP '1970-01-18 12:03:08.456789000 UTC', TIMESTAMP '1970-12-31 12:55:00.456789000 UTC', 5, 8), " +
                "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234567890 UTC', TIMESTAMP '2015-09-15 14:21:02.345678900 UTC', 9, 10), " +
                "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876543210 UTC', TIMESTAMP '2020-08-21 16:12:12.654321000 UTC', 11, 12)";
        String expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, '1968-03-15 15:13:12.876 UTC', '2020-08-21 16:12:12.654 UTC'";
        String expectedIntegerStats = "NULL, 13e0, 0e0, NULL, '1', '101'";

        assertThat(query("SELECT partition.d_year, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_year_transform_timestamptz$partitions\""))
                .skippingTypesCheck()
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE day_of_week(d) = 2 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '2015-09-15 14:21:02.345678900 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_year_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedIntegerStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        // Tests run with non-UTC session, so timestamp_tz > a_date will not align with partition boundaries. Use with_timezone to align it.
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= with_timezone(DATE '2015-01-01', 'UTC')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= with_timezone(DATE '2015-01-02', 'UTC')"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-01-01' AND d < TIMESTAMP '2015-01-01 01:00:00 Europe/Warsaw'"))
                // Engine can eliminate the table scan after connector accepts the filter pushdown
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)))
                .returnsEmptyResult();

        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= TIMESTAMP '2015-01-01 00:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= TIMESTAMP '2015-01-01 00:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_year_transform_timestamptz");
    }

    @Test
    void testTimestampNanoWithTimeZone()
    {
        testTimestampNanoWithTimeZone("PARQUET");
        testTimestampNanoWithTimeZone("ORC");
        testTimestampNanoWithTimeZone("AVRO");
    }

    private void testTimestampNanoWithTimeZone(String format)
    {
        try (TestTable table = newTrinoTable("test_nano", "(id int, x timestamp(9) with time zone) WITH (format = '" + format + "', format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, timestamp '2022-07-26 12:13:14.123456789 America/Los_Angeles')", 1);

            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES timestamp '2022-07-26 19:13:14.123456789 UTC'");
            assertThat(query("SELECT 1 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456789 America/Los_Angeles'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT 1 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456 America/Los_Angeles'"))
                    .returnsEmptyResult();
        }
    }

    @Test
    void testTimestampNanoWithTimeZonePartition()
    {
        try (TestTable table = newTrinoTable("test_nano", "(id int, x timestamp(9) with time zone) WITH (partitioning = ARRAY['x'], format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, timestamp '2022-07-26 12:13:14.123456789 America/Los_Angeles')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, timestamp '2022-07-26 12:13:14.012345 America/Los_Angeles')", 1);

            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES timestamp '2022-07-26 19:13:14.123456789 UTC', timestamp '2022-07-26 19:13:14.012345 UTC'");
            assertThat(query("SELECT 1 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.123456789 America/Los_Angeles'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT 2 FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.012345 America/Los_Angeles'"))
                    .matches("VALUES 2");
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE x = timestamp '2022-07-26 12:13:14.012345 America/Los_Angeles'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testUnsupportedTableEncryption()
    {
        String tableName = "test_encryption" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(x int)");
        try {
            BaseTable icebergTable = loadTable(tableName);
            icebergTable.updateProperties().set(ENCRYPTION_TABLE_KEY, "test_key").commit();

            assertQueryFails("SELECT * FROM " + tableName, "Table encryption is not supported for: .*");
        }
        finally {
            metastore.dropTable("tpch", tableName, true);
        }
    }

    @Test
    void testShowEmptyStatsForRowLineageColumn()
    {
        try (TestTable table = newTrinoTable("test_stats_row_lineage", "AS SELECT * FROM tpch.tiny.nation")) {
            assertThat(query("SHOW STATS FOR " + table.getName()))
                    .matches("""
                            VALUES
                            (varchar 'nationkey', cast(null AS double), cast(25.0 AS double), cast(0.0 AS double), cast(null AS double), varchar '0', varchar '24'),
                            ('name', 583.0, 25.0, 0.0, null, null, null),
                            ('regionkey', null, 5.0, 0.0, null, '0', '4'),
                            ('comment', 2162.0, 25.0, 0.0, null, null, null),
                            (null, null, null, null, 25.0, null, null)
                            """);

            // show stats with row lineage column $row_id, returns null for all stats
            assertThat(query("SHOW STATS FOR (SELECT * FROM " + table.getName() + " WHERE \"$row_id\" = 1)"))
                    .matches("""
                            VALUES
                            (varchar 'nationkey', cast(null AS double), cast(null AS double), cast(null AS double), cast(null AS double), cast(null AS varchar), cast(null AS varchar)),
                            ('regionkey', null, null, null, null, null, null),
                            ('comment', null, null, null, null, null, null),
                            ('name', null, null, null, null, null, null),
                            (null, null, null, null, null, null, null)
                            """);

            // show stats with row lineage column $last_updated_sequence_number, returns null for all stats
            assertThat(query("SHOW STATS FOR (SELECT * FROM " + table.getName() + " WHERE \"$last_updated_sequence_number\" = 1)"))
                    .matches("""
                            VALUES
                            (varchar 'nationkey', cast(null AS double), cast(null AS double), cast(null AS double), cast(null AS double), cast(null AS varchar), cast(null AS varchar)),
                            ('regionkey', null, null, null, null, null, null),
                            ('comment', null, null, null, null, null, null),
                            ('name', null, null, null, null, null, null),
                            (null, null, null, null, null, null, null)
                            """);
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }

    private Set<String> manifestFiles(String tableName)
    {
        return computeActual("SELECT path FROM \"" + tableName + "$manifests\"").getOnlyColumnAsSet().stream()
                .map(path -> (String) path)
                .collect(toImmutableSet());
    }

    private static Session withSingleWriterPerTask(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("task_min_writer_count", "1")
                .build();
    }

    private List<String> getActiveFiles(String tableName)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", tableName)).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeScalar(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", tableName));
    }

    private long getCommittedAtInEpochMilliSeconds(String tableName, long snapshotId)
    {
        return ((ZonedDateTime) computeScalar(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id=%s", tableName, snapshotId)))
                .toInstant().toEpochMilli();
    }

    private static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
