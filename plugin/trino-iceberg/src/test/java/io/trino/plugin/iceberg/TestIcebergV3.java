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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.listFiles;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
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
    void testUpgradeTableToV3FromTrino()
    {
        String tableName = "test_upgrade_table_to_v3_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(1);

        // v1 -> v2
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2");
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");

        // v2 -> v3
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 3");
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(3);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    void testUpgradeTableFromV1ToV3()
    {
        String tableName = "test_upgrade_table_from_v1_to_v3_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(1);

        // v1 -> v3
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 3");
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(3);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    void testDowngradingFromV3Fails()
    {
        String tableName = "test_downgrading_from_v3_fails_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 3) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(3);

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
        }
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
