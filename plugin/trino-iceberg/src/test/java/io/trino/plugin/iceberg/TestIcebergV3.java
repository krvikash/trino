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

import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
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

import java.util.List;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.listFiles;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
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
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.max-format-version", "3")
                .build();

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

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
