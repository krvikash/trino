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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_UNITY;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_UNITY_CREDENTIALS_VENDING;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_UNITY_PROXY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDatabricksUnityCompatibility
        extends ProductTest
{
    private final String schemaName = "test_delta_basic_" + randomNameSuffix();
    private String unityCatalogName;
    private String externalLocationPath;

    @BeforeMethodWithContext
    public void setUp()
    {
        unityCatalogName = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
        externalLocationPath = requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION");
        onDelta().executeQuery(format("CREATE SCHEMA %s.%s", unityCatalogName, schemaName));
    }

    @AfterMethodWithContext
    public void cleanUp()
    {
        onDelta().executeQuery(format("DROP SCHEMA IF EXISTS %s.%s CASCADE", unityCatalogName, schemaName));
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, DELTA_LAKE_DATABRICKS_UNITY_CREDENTIALS_VENDING, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTableReadWriteExternalTable()
    {
        String tableName = "test_read_write_external_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino().executeQuery("CREATE TABLE " + deltaTableName + "(c1 integer, c2 varchar) WITH (location = '" + tableLocation + "')");
        onTrino().executeQuery("INSERT INTO " + deltaTableName + " VALUES (1, 'one')");

        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName.toLowerCase(ENGLISH)));

        // select
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(row(1, "one"));

        // test view
        String viewName = "test_view_" + randomNameSuffix();
        String unityViewName = "%s.%s.%s".formatted(unityCatalogName, schemaName, viewName);
        onDelta().executeQuery("CREATE VIEW " + unityViewName + " AS SELECT * FROM " + unityTableName);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityViewName))
                .containsOnly(row(1, "one"));

        // insert
        List<Row> expectedRowsForInsert = ImmutableList.of(row(1, "one"), row(2, "two"));
        onTrino().executeQuery("INSERT INTO " + deltaTableName + " VALUES (2, 'two')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForInsert);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForInsert);

        // update
        List<Row> expectedRowsForUpdate = ImmutableList.of(row(1, "one"), row(2, "two hundred"));
        onTrino().executeQuery("UPDATE " + deltaTableName + " SET c2 = 'two hundred' WHERE c1 = 2");
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForUpdate);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForUpdate);

        // delete
        List<Row> expectedRowsForDelete = ImmutableList.of(row(2, "two hundred"));
        onTrino().executeQuery("DELETE FROM " + deltaTableName + " WHERE c2 = 'one'");
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForDelete);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForDelete);

        // merge
        List<Row> expectedRowsForMerge = ImmutableList.of(row(1, "one"), row(2, "two"), row(3, "three"));
        String sourceTableName = "test_source_" + randomNameSuffix();
        String tableLocation2 = format("%s/%s/%s", externalLocationPath, schemaName, sourceTableName);
        onDelta().executeQuery(format("CREATE TABLE %s.%s.%s (c1 int, c2 string) using delta location '%s'", unityCatalogName, schemaName, sourceTableName, tableLocation2));
        onDelta().executeQuery(format("INSERT INTO %s.%s.%s values (1, 'one'), (2, 'two'), (3, 'three')", unityCatalogName, schemaName, sourceTableName));

        onTrino().executeQuery(format("MERGE INTO delta.%s.%s t USING delta.%s.%s s on t.c1 = s.c1 " +
                "WHEN MATCHED THEN UPDATE SET c2 = s.c2 " +
                "WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (s.c1, s.c2)", schemaName, tableName, schemaName, sourceTableName));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForMerge);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForMerge);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, DELTA_LAKE_DATABRICKS_UNITY_CREDENTIALS_VENDING, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadWriteCatalogManagedTable()
    {
        String tableName = "test_catalog_managed_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String createTableSql = format("CREATE TABLE %s (id int) USING delta TBLPROPERTIES('delta.feature.catalogManaged' = 'supported', " +
                // Disable the writing features `rowTracking` and `v2Checkpoint` and `domainMetadata`
                "'delta.enableRowTracking' = 'false', 'delta.checkpointPolicy' = 'classic'" +
                ")", unityTableName);

        try {
            onDelta().executeQuery(createTableSql);

            onDelta().executeQuery("INSERT INTO " + unityTableName + " VALUES 1");

            assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1));

            assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                    .containsOnly(row(tableName));

            assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1));

            // insert
            assertThat(onTrino().executeQuery("INSERT INTO " + deltaTableName + " VALUES 2"))
                    .containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1), row(2));
            assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1), row(2));

            // update
            assertThat(onTrino().executeQuery("UPDATE " + deltaTableName + " SET id = -2 WHERE id = 2"))
                    .containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1), row(-2));
            assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1), row(-2));

            // delete
            assertThat(onTrino().executeQuery("DELETE FROM " + deltaTableName + " WHERE id = -2"))
                    .containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1));
            assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1));

            // insert again
            assertThat(onTrino().executeQuery("INSERT INTO " + deltaTableName + " VALUES 2, 3"))
                    .containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1), row(2), row(3));
            assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1), row(2), row(3));
        }
        finally {
            dropDeltaTableWithRetry(unityTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, DELTA_LAKE_DATABRICKS_UNITY_CREDENTIALS_VENDING, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTableReadWriteManagedTable()
    {
        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM delta"))
                .contains(row(schemaName.toLowerCase(ENGLISH)));

        String tableName = "test_read_write_managed_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);

        onDelta().executeQuery("CREATE TABLE " + unityTableName + " (c1 int, c2 string)");
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onDelta().executeQuery("INSERT INTO " + unityTableName + " VALUES (1, 'one')");

        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName.toLowerCase(ENGLISH)));

        // select
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));

        // insert
        assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO " + deltaTableName + " VALUES (2, 'two')"))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // update
        assertQueryFailure(() -> onTrino().executeQuery("UPDATE " + deltaTableName + " SET c2 = 'two' WHERE c1 = 1"))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // delete
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM " + deltaTableName + " WHERE c2 = 'one'"))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM " + deltaTableName))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // truncate
        assertQueryFailure(() -> onTrino().executeQuery("TRUNCATE TABLE " + deltaTableName))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // merge
        String sourceTableName = "test_source_" + randomNameSuffix();
        String tableLocation2 = format("%s/%s/%s", externalLocationPath, schemaName, sourceTableName);
        onDelta().executeQuery(format("CREATE TABLE %s.%s.%s (c1 int, c2 string) using delta location '%s'", unityCatalogName, schemaName, sourceTableName, tableLocation2));
        onDelta().executeQuery(format("INSERT INTO %s.%s.%s values (1, 'one'), (2, 'two'), (3, 'three')", unityCatalogName, schemaName, sourceTableName));

        assertQueryFailure(() -> onTrino().executeQuery(format("MERGE INTO delta.%s.%s t USING delta.%s.%s s on t.c1 = s.c1 " +
                "WHEN MATCHED THEN UPDATE SET c2 = s.c2 " +
                "WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (s.c1, s.c2)", schemaName, tableName, schemaName, sourceTableName)))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnTypes()
    {
        String tableName = "test_column_types" + randomNameSuffix();
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        // TODO add all the native Databricks supported types https://starburstdata.atlassian.net/browse/CONNECT-427
        onDelta().executeQuery(format("CREATE TABLE %s (" +
                "int_col INT," +
                "string_col STRING," +
                "tinyint_col TINYINT," +
                "smallint_col SMALLINT," +
                "bigint_col BIGINT," +
                "decimal_col DECIMAL," +
                "decimal_prec_short_col DECIMAL(4,2)," +
                "decimal_prec_long_col DECIMAL(19,9)," +
                "float_col FLOAT," +
                "double_col DOUBLE," +
                "date_col DATE," +
                "timestamp_col TIMESTAMP," +
                "binary_col BINARY," +
                "bool_col BOOLEAN," +
                "array_int_col ARRAY<int>," +
                "map_col MAP<TIMESTAMP,INT>," +
                "struct_col struct<a: LONG, b: String NOT NULL>" +
                ") " +
                "USING DELTA " +
                "LOCATION '%s'", unityTableName, tableLocation));
        assertThat(
                onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName));
        assertThat(
                onTrino().executeQuery("SHOW COLUMNS IN " + deltaTableName))
                .containsOnly(
                        row("int_col", "integer", "", ""),
                        row("string_col", "varchar", "", ""),
                        row("tinyint_col", "tinyint", "", ""),
                        row("smallint_col", "smallint", "", ""),
                        row("bigint_col", "bigint", "", ""),
                        row("decimal_col", "decimal(10,0)", "", ""),
                        row("decimal_prec_short_col", "decimal(4,2)", "", ""),
                        row("decimal_prec_long_col", "decimal(19,9)", "", ""),
                        row("float_col", "real", "", ""),
                        row("double_col", "double", "", ""),
                        row("date_col", "date", "", ""),
                        row("timestamp_col", "timestamp(3) with time zone", "", ""),
                        row("binary_col", "varbinary", "", ""),
                        row("bool_col", "boolean", "", ""),
                        row("array_int_col", "array(integer)", "", ""),
                        row("map_col", "map(timestamp(3) with time zone, integer)", "", ""),
                        row("struct_col", "row(\"a\" bigint, \"b\" varchar)", "", ""));
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPartitionedTables()
    {
        String managedTableName = "test_partitioned_managed_table_" + randomNameSuffix();
        String unityManagedTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, managedTableName);
        String deltaManagedTableName = "delta.%s.%s".formatted(schemaName, managedTableName);

        onDelta().executeQuery("CREATE TABLE " + unityManagedTableName + " (c1 int, c2 string) PARTITIONED BY (c1)");
        // writes are not supported on trino for managed table on Unity
        onDelta().executeQuery("INSERT INTO " + unityManagedTableName + "(c1, c2) VALUES (1, 'one')");
        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .contains(row(managedTableName));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaManagedTableName))
                .containsOnly(row(1, "one"));

        String externalTableName = "test_partitioned_external_table_" + randomNameSuffix();
        String unityExternalTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, externalTableName);
        String deltaExternalTableName = "delta.%s.%s".formatted(schemaName, externalTableName);
        String unityExternalTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, externalTableName);

        onDelta().executeQuery("CREATE TABLE " + unityExternalTableName + " (c1 int, c2 string) PARTITIONED BY (c1) location '" + unityExternalTableLocation + "'");
        onTrino().executeQuery("INSERT INTO " + deltaExternalTableName + "(c1, c2) VALUES (2, 'two')");
        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .contains(row(externalTableName));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaExternalTableName))
                .containsOnly(row(2, "two"));
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY_PROXY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testShowSchemas()
    {
        assertThat(onTrino().executeQuery(format("SHOW SCHEMAS FROM delta LIKE '%s'", schemaName)))
                .contains(row(schemaName));
    }
}
