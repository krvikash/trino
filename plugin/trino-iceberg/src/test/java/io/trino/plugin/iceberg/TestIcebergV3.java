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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
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
