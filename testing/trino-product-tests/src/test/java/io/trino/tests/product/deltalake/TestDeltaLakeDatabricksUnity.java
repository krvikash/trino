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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(DatabricksUnityS3Environment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricksUnity
@TestGroup.ProfileSpecificTests
class TestDeltaLakeDatabricksUnity
        extends BaseTestDeltaLakeDatabricksUnity
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testColumnTypes(DatabricksUnityEnvironment env)
    {
        String tableName = "test_column_types" + randomNameSuffix();
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        // TODO add all the native Databricks supported types https://starburstdata.atlassian.net/browse/CONNECT-427
        env.executeDatabricksSql(format("CREATE TABLE %s (" +
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
                env.executeTrinoSql("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName));
        assertThat(
                env.executeTrinoSql("SHOW COLUMNS IN " + deltaTableName))
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

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testPartitionedTables(DatabricksUnityEnvironment env)
    {
        String managedTableName = "test_partitioned_managed_table_" + randomNameSuffix();
        String unityManagedTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, managedTableName);
        String deltaManagedTableName = "delta.%s.%s".formatted(schemaName, managedTableName);

        env.executeDatabricksSql("CREATE TABLE " + unityManagedTableName + " (c1 int, c2 string) PARTITIONED BY (c1)");
        // writes are not supported on trino for managed table on Unity
        env.executeDatabricksSql("INSERT INTO " + unityManagedTableName + "(c1, c2) VALUES (1, 'one')");
        assertThat(env.executeTrinoSql("SHOW TABLES IN delta." + schemaName))
                .contains(row(managedTableName));
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaManagedTableName))
                .containsOnly(row(1, "one"));

        String externalTableName = "test_partitioned_external_table_" + randomNameSuffix();
        String unityExternalTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, externalTableName);
        String deltaExternalTableName = "delta.%s.%s".formatted(schemaName, externalTableName);
        String unityExternalTableLocation = format("%s/%s/%s", externalLocationPath, schemaName, externalTableName);

        env.executeDatabricksSql("CREATE TABLE " + unityExternalTableName + " (c1 int, c2 string) PARTITIONED BY (c1) location '" + unityExternalTableLocation + "'");
        env.executeTrinoSql("INSERT INTO " + deltaExternalTableName + "(c1, c2) VALUES (2, 'two')");
        assertThat(env.executeTrinoSql("SHOW TABLES IN delta." + schemaName))
                .contains(row(externalTableName));
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaExternalTableName))
                .containsOnly(row(2, "two"));
    }
}
