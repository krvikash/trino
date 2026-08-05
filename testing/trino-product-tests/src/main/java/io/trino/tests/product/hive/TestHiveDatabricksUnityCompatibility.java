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
package io.trino.tests.product.hive;

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_DATABRICKS_UNITY;
import static io.trino.tests.product.TestGroups.HIVE_DATABRICKS_UNITY_PROXY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveDatabricksUnityCompatibility
        extends ProductTest
{
    private String unityCatalogName;
    private String externalLocationPath;
    private final String schemaName = "test_basic_hive_" + randomNameSuffix();

    @BeforeMethodWithContext
    public void setUp()
    {
        unityCatalogName = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
        externalLocationPath = requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION");
        String schemaLocation = format("%s/%s", externalLocationPath, schemaName);
        onTrino().executeQuery("CREATE SCHEMA hive." + schemaName + " WITH (location = '" + schemaLocation + "')");
    }

    @AfterMethodWithContext
    public void cleanUp()
    {
        onDelta().executeQuery("DROP SCHEMA IF EXISTS " + unityCatalogName + "." + schemaName + " CASCADE");
    }

    @Test(groups = {HIVE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnTypes()
    {
        String tableName = "test_column_types" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

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
                "USING PARQUET " +
                "LOCATION '%s'", unityTableName, tableLocation));
        assertThat(
                onTrino().executeQuery("SHOW TABLES IN hive." + schemaName))
                .containsOnly(row(tableName));
        assertThat(
                onTrino().executeQuery("SHOW COLUMNS IN " + hiveTableName))
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
                        row("timestamp_col", "timestamp(3)", "", ""),
                        row("binary_col", "varbinary", "", ""),
                        row("bool_col", "boolean", "", ""),
                        row("array_int_col", "array(integer)", "", ""),
                        row("map_col", "map(timestamp(3), integer)", "", ""),
                        row("struct_col", "row(\"a\" bigint, \"b\" varchar)", "", ""));
    }

    @Test(groups = {HIVE_DATABRICKS_UNITY_PROXY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testShowSchemas()
    {
        assertThat(onTrino().executeQuery(format("SHOW SCHEMAS FROM hive LIKE '%s'", schemaName)))
                .contains(row(schemaName));
    }
}
