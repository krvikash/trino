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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_DATABRICKS_UNITY;
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
    public void testTableReadWriteExternalTable()
    {
        String tableName = "test_read_write_" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        onTrino().executeQuery("CREATE TABLE " + hiveTableName + "(c1 integer, c2 varchar) WITH (external_location = '" + tableLocation + "', format = 'PARQUET')");
        onTrino().executeQuery("INSERT INTO " + hiveTableName + " VALUES (1, 'one')");

        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM hive"))
                .contains(row(schemaName));

        String viewName = "test_view_" + randomNameSuffix();
        String unityViewName = "%s.%s.%s".formatted(unityCatalogName, schemaName, viewName);
        onDelta().executeQuery("CREATE VIEW " + unityViewName + " AS SELECT * FROM " + unityTableName);

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM " + schemaName + "." + viewName))
                .hasRootCauseMessage("Unsupported table type: VIEW");
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityViewName))
                .containsOnly(row(1, "one"));

        assertThat(onTrino().executeQuery("SHOW TABLES IN hive." + schemaName).column(1))
                .contains(tableName)
                .doesNotContain(viewName);

        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTableName))
                .containsOnly(row(1, "one"));

        // insert through Trino and query through Trino
        List<Row> expectedRowsForInsert = ImmutableList.of(row(1, "one"), row(2, "two"));
        onTrino().executeQuery("INSERT INTO " + hiveTableName + " VALUES (2, 'two')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTableName))
                .containsOnly(expectedRowsForInsert);
        onDelta().executeQuery("REFRESH TABLE " + unityTableName); // Required to get latest data from Databricks
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForInsert);

        // Update is not supported for non-transactional table
        assertQueryFailure(() -> onTrino().executeQuery("UPDATE " + hiveTableName + " SET c2 = 'oneone' WHERE c1 = 1"))
                .hasRootCauseMessage("Modifying Hive table rows is only supported for transactional tables");
        // Delete is not supported  for non-transactional table
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM " + hiveTableName + " WHERE c1 = 1"))
                .hasRootCauseMessage("Modifying Hive table rows is only supported for transactional tables");
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

    @Test(groups = {HIVE_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDifferentTableFormats()
    {
        // PARQUET has been tested in various tests

        // AVRO
        String avroTableName = "test_table_" + randomNameSuffix();
        String hiveAvroTableName = "hive.%s.%s".formatted(schemaName, avroTableName);
        String unityAvroTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, avroTableName);
        String avroTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, avroTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveAvroTableName + " WITH (external_location = '" + avroTableLocation + "', format = 'AVRO') AS SELECT 1 AS c1, 'one' AS c2");
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveAvroTableName))
                .containsOnly(row(1, "one"));
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityAvroTableName))
                .containsOnly(row(1, "one"));

        // ORC
        String orcTableName = "test_table_" + randomNameSuffix();
        String hiveOrcTableName = "hive.%s.%s".formatted(schemaName, orcTableName);
        String unityOrcTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, orcTableName);
        String orcTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, orcTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveOrcTableName + " WITH (external_location = '" + orcTableLocation + "', format = 'ORC') AS SELECT 2 AS c1, 'two' AS c2");
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveOrcTableName))
                .containsOnly(row(2, "two"));
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityOrcTableName))
                .containsOnly(row(2, "two"));

        // JSON
        String jsonTableName = "test_table_" + randomNameSuffix();
        String hiveJsonTableName = "hive.%s.%s".formatted(schemaName, jsonTableName);
        String unityJsonTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, jsonTableName);
        String jsonTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, jsonTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveJsonTableName + " WITH (external_location = '" + jsonTableLocation + "', format = 'JSON') AS SELECT 3 AS c1, 'three' AS c2");
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveJsonTableName))
                .containsOnly(row(3, "three"));
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityJsonTableName))
                .containsOnly(row(3, "three"));

        // CSV
        String csvTableName = "test_table_" + randomNameSuffix();
        String hiveCsvTableName = "hive.%s.%s".formatted(schemaName, csvTableName);
        String unityCsvTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, csvTableName);
        String csvTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, csvTableName);
        // Hive CSV storage format only supports VARCHAR (unbounded)
        onTrino().executeQuery("CREATE TABLE " + hiveCsvTableName + " WITH (external_location = '" + csvTableLocation + "', format = 'CSV') AS SELECT VARCHAR '4' AS c1, VARCHAR 'four' AS c2");
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveCsvTableName))
                .containsOnly(row("4", "four"));
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityCsvTableName))
                .containsOnly(row("4", "four"));

        // TEXT
        String textTableName = "test_table_" + randomNameSuffix();
        String hiveTextTableName = "hive.%s.%s".formatted(schemaName, textTableName);
        String unityTextTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, textTableName);
        String textTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, textTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveTextTableName + " WITH (external_location = '" + textTableLocation + "', format = 'TEXTFILE') AS SELECT '5-five' AS c1");
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTextTableName))
                .containsOnly(row("5-five"));
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTextTableName))
                .containsOnly(row("5-five"));

        assertThat(onTrino().executeQuery("SHOW TABLES IN hive." + schemaName).column(1))
                .contains(avroTableName, orcTableName, jsonTableName, csvTableName, textTableName);
    }
}
