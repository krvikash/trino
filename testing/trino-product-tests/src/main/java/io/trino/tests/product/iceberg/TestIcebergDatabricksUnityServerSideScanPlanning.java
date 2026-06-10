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
package io.trino.tests.product.iceberg;

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG_DATABRICKS_UNITY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergDatabricksUnityServerSideScanPlanning
        extends ProductTest
{
    private final String schemaName = "test_iceberg_server_side_scan_planning_" + randomNameSuffix();
    private String unityCatalogName;

    @BeforeMethodWithContext
    public void setUp()
    {
        unityCatalogName = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
        onDelta().executeQuery(format("CREATE SCHEMA %s.%s", unityCatalogName, schemaName));
    }

    @AfterMethodWithContext
    public void cleanUp()
    {
        onDelta().executeQuery(format("DROP SCHEMA IF EXISTS %s.%s CASCADE", unityCatalogName, schemaName));
    }

    @Test(groups = {ICEBERG_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    public void testReadWithColumnMask()
    {
        String tableName = "table_with_mask";
        String unityTable = format("%s.%s.%s", unityCatalogName, schemaName, tableName);
        String trinoTable = format("iceberg.%s.%s", schemaName, tableName);
        String maskFunction = format("%s.%s.redact_name", unityCatalogName, schemaName);

        createTestTable(unityTable);

        onDelta().executeQuery(format("CREATE OR REPLACE FUNCTION %s(s STRING) RETURN '***'", maskFunction));
        onDelta().executeQuery(format("ALTER TABLE %s ALTER COLUMN name SET MASK %s", unityTable, maskFunction));

        assertThat(onDelta().executeQuery("SELECT DISTINCT name FROM " + unityTable))
                .containsOnly(row("***"));
        assertThat(onTrino().executeQuery("SELECT DISTINCT name FROM " + trinoTable))
                .containsOnly(row("***"));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTable))
                .containsOnly(row(10));
    }

    @Test(groups = {ICEBERG_DATABRICKS_UNITY, PROFILE_SPECIFIC_TESTS})
    public void testReadWithRowFilter()
    {
        String tableName = "table_with_filter";
        String unityTable = format("%s.%s.%s", unityCatalogName, schemaName, tableName);
        String trinoTable = format("iceberg.%s.%s", schemaName, tableName);
        String filterFunction = format("%s.%s.id_lt_ten", unityCatalogName, schemaName);

        createTestTable(unityTable);

        onDelta().executeQuery(format("CREATE OR REPLACE FUNCTION %s(k INT) RETURN k < 5", filterFunction));
        onDelta().executeQuery(format("ALTER TABLE %s SET ROW FILTER %s ON (id)", unityTable, filterFunction));

        assertThat(onDelta().executeQuery("SELECT max(id) FROM " + unityTable))
                .containsOnly(row(4));
        assertThat(onDelta().executeQuery("SELECT count(*) FROM " + unityTable))
                .containsOnly(row(5));
        assertThat(onTrino().executeQuery("SELECT max(id) FROM " + trinoTable))
                .containsOnly(row(4));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTable))
                .containsOnly(row(5));
    }

    private void createTestTable(String unityTableName)
    {
        // Use a Unity Catalog managed Iceberg table, as server-side scan planning is not supported otherwise
        onDelta().executeQuery(format(
                "CREATE TABLE %s (id INT, name STRING) USING ICEBERG TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')",
                unityTableName));
        onDelta().executeQuery(format(
                "INSERT INTO %s VALUES" +
                        "(0, 'abc')," +
                        "(1, 'def')," +
                        "(2, 'ghi')," +
                        "(3, 'jkl')," +
                        "(4, 'mno')," +
                        "(5, 'pqr')," +
                        "(6, 'stu')," +
                        "(7, 'vwx')," +
                        "(8, 'yza')," +
                        "(9, 'bcd')",
                unityTableName));
    }
}
