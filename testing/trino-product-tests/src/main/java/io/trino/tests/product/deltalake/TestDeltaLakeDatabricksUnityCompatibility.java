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

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_UNITY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
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

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadExternalTable()
    {
        String tableName = "test_read_" + randomNameSuffix();
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        onDelta().executeQuery("CREATE TABLE " + unityTableName + " (c1 int, c2 string) USING delta LOCATION '" + tableLocation + "'");
        onDelta().executeQuery("INSERT INTO " + unityTableName + " VALUES (1, 'one')");

        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM delta"))
                .contains(row(schemaName.toLowerCase(ENGLISH)));
        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName.toLowerCase(ENGLISH)));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadManagedTable()
    {
        String tableName = "test_managed_table_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);

        onDelta().executeQuery("CREATE TABLE " + unityTableName + " (c1 int, c2 string)");
        onDelta().executeQuery("INSERT INTO " + unityTableName + " VALUES (1, 'one')");
        assertThat(onTrino().executeQuery("SHOW CREATE SCHEMA delta." + schemaName))
                .containsOnly(row("CREATE SCHEMA delta." + schemaName));
        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .contains(row(tableName));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));
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
                    row("struct_col", "row(a bigint, b varchar)", "", ""));
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPartitionedTables()
    {
        String managedTableName = "test_partitioned_managed_table_" + randomNameSuffix();
        String unityManagedTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, managedTableName);
        String deltaManagedTableName = "delta.%s.%s".formatted(schemaName, managedTableName);

        onDelta().executeQuery("CREATE TABLE " + unityManagedTableName + " (c1 int, c2 string) PARTITIONED BY (c1)");
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
        onDelta().executeQuery("INSERT INTO " + unityExternalTableName + "(c1, c2) VALUES (2, 'two')");
        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .contains(row(externalTableName));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaExternalTableName))
                .containsOnly(row(2, "two"));
    }
}
