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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(IcebergDatabricksUnityEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.IcebergDatabricksUnity
@TestGroup.ProfileSpecificTests
class TestIcebergDatabricksUnityServerSideScanPlanning
{
    private final String schemaName = "test_iceberg_server_side_scan_planning_" + randomNameSuffix();
    private String unityCatalogName;

    @BeforeEach
    void setUp(IcebergDatabricksUnityEnvironment env)
    {
        unityCatalogName = env.unityCatalogName();
        env.executeDatabricksSql(format("CREATE SCHEMA %s.%s", unityCatalogName, schemaName));
    }

    @AfterEach
    void cleanUp(IcebergDatabricksUnityEnvironment env)
    {
        env.executeDatabricksSql(format("DROP SCHEMA IF EXISTS %s.%s CASCADE", unityCatalogName, schemaName));
    }

    @Test
    void testReadWithColumnMask(IcebergDatabricksUnityEnvironment env)
    {
        String tableName = "table_with_mask";
        String unityTable = format("%s.%s.%s", unityCatalogName, schemaName, tableName);
        String trinoTable = format("iceberg.%s.%s", schemaName, tableName);
        String maskFunction = format("%s.%s.redact_name", unityCatalogName, schemaName);

        createTestTable(env, unityTable);

        env.executeDatabricksSql(format("CREATE OR REPLACE FUNCTION %s(s STRING) RETURN '***'", maskFunction));
        env.executeDatabricksSql(format("ALTER TABLE %s ALTER COLUMN name SET MASK %s", unityTable, maskFunction));

        assertThat(env.executeDatabricksSql("SELECT DISTINCT name FROM " + unityTable))
                .containsOnly(row("***"));
        assertThat(env.executeTrinoSql("SELECT DISTINCT name FROM " + trinoTable))
                .containsOnly(row("***"));
        assertThat(env.executeTrinoSql("SELECT count(*) FROM " + trinoTable))
                .containsOnly(row(10L));
    }

    @Test
    void testReadWithRowFilter(IcebergDatabricksUnityEnvironment env)
    {
        String tableName = "table_with_filter";
        String unityTable = format("%s.%s.%s", unityCatalogName, schemaName, tableName);
        String trinoTable = format("iceberg.%s.%s", schemaName, tableName);
        String filterFunction = format("%s.%s.id_lt_ten", unityCatalogName, schemaName);

        createTestTable(env, unityTable);

        env.executeDatabricksSql(format("CREATE OR REPLACE FUNCTION %s(k INT) RETURN k < 5", filterFunction));
        env.executeDatabricksSql(format("ALTER TABLE %s SET ROW FILTER %s ON (id)", unityTable, filterFunction));

        assertThat(env.executeDatabricksSql("SELECT max(id) FROM " + unityTable))
                .containsOnly(row(4));
        assertThat(env.executeDatabricksSql("SELECT count(*) FROM " + unityTable))
                .containsOnly(row(5L));
        assertThat(env.executeTrinoSql("SELECT max(id) FROM " + trinoTable))
                .containsOnly(row(4));
        assertThat(env.executeTrinoSql("SELECT count(*) FROM " + trinoTable))
                .containsOnly(row(5L));
    }

    private static void createTestTable(IcebergDatabricksUnityEnvironment env, String unityTableName)
    {
        // Use a Unity Catalog managed Iceberg table, as server-side scan planning is not supported otherwise
        env.executeDatabricksSql(format(
                "CREATE TABLE %s (id INT, name STRING) USING ICEBERG TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')",
                unityTableName));
        env.executeDatabricksSql(format(
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
