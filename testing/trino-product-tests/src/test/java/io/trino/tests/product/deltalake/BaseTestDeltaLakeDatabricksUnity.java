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
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class BaseTestDeltaLakeDatabricksUnity
{
    protected final String schemaName = "test_delta_basic_" + randomNameSuffix();
    protected String unityCatalogName;
    protected String externalLocationPath;

    @BeforeEach
    void setUp(DatabricksUnityEnvironment env)
    {
        unityCatalogName = env.unityCatalogName();
        externalLocationPath = env.unityExternalLocation();
        env.executeDatabricksSql(format("CREATE SCHEMA %s.%s", unityCatalogName, schemaName));
    }

    @AfterEach
    void cleanUp(DatabricksUnityEnvironment env)
    {
        env.executeDatabricksSql(format("DROP SCHEMA IF EXISTS %s.%s CASCADE", unityCatalogName, schemaName));
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTableReadWriteExternalTable(DatabricksUnityEnvironment env)
    {
        String tableName = "test_read_write_external_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        env.executeTrinoSql("CREATE TABLE " + deltaTableName + "(c1 integer, c2 varchar) WITH (location = '" + tableLocation + "')");
        env.executeTrinoSql("INSERT INTO " + deltaTableName + " VALUES (1, 'one')");

        assertThat(env.executeTrinoSql("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName.toLowerCase(ENGLISH)));

        // select
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));
        assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                .containsOnly(row(1, "one"));

        // test view
        String viewName = "test_view_" + randomNameSuffix();
        String unityViewName = "%s.%s.%s".formatted(unityCatalogName, schemaName, viewName);
        env.executeDatabricksSql("CREATE VIEW " + unityViewName + " AS SELECT * FROM " + unityTableName);
        assertThat(env.executeDatabricksSql("SELECT * FROM " + unityViewName))
                .containsOnly(row(1, "one"));

        // insert
        List<Row> expectedRowsForInsert = ImmutableList.of(row(1, "one"), row(2, "two"));
        env.executeTrinoSql("INSERT INTO " + deltaTableName + " VALUES (2, 'two')");
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForInsert);
        assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForInsert);

        // update
        List<Row> expectedRowsForUpdate = ImmutableList.of(row(1, "one"), row(2, "two hundred"));
        env.executeTrinoSql("UPDATE " + deltaTableName + " SET c2 = 'two hundred' WHERE c1 = 2");
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForUpdate);
        assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForUpdate);

        // delete
        List<Row> expectedRowsForDelete = ImmutableList.of(row(2, "two hundred"));
        env.executeTrinoSql("DELETE FROM " + deltaTableName + " WHERE c2 = 'one'");
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForDelete);
        assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForDelete);

        // merge
        List<Row> expectedRowsForMerge = ImmutableList.of(row(1, "one"), row(2, "two"), row(3, "three"));
        String sourceTableName = "test_source_" + randomNameSuffix();
        String tableLocation2 = format("%s/%s/%s", externalLocationPath, schemaName, sourceTableName);
        env.executeDatabricksSql(format("CREATE TABLE %s.%s.%s (c1 int, c2 string) using delta location '%s'", unityCatalogName, schemaName, sourceTableName, tableLocation2));
        env.executeDatabricksSql(format("INSERT INTO %s.%s.%s values (1, 'one'), (2, 'two'), (3, 'three')", unityCatalogName, schemaName, sourceTableName));

        env.executeTrinoSql(format("MERGE INTO delta.%s.%s t USING delta.%s.%s s on t.c1 = s.c1 " +
                "WHEN MATCHED THEN UPDATE SET c2 = s.c2 " +
                "WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (s.c1, s.c2)", schemaName, tableName, schemaName, sourceTableName));
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForMerge);
        assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForMerge);
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testReadWriteCatalogManagedTable(DatabricksUnityEnvironment env)
    {
        String tableName = "test_catalog_managed_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String createTableSql = format("CREATE TABLE %s (id int) USING delta TBLPROPERTIES('delta.feature.catalogManaged' = 'supported', " +
                // Disable the writing features `rowTracking` and `v2Checkpoint` and `domainMetadata`
                "'delta.enableRowTracking' = 'false', 'delta.checkpointPolicy' = 'classic'" +
                ")", unityTableName);

        try {
            env.executeDatabricksSql(createTableSql);

            env.executeDatabricksSql("INSERT INTO " + unityTableName + " VALUES 1");

            assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1));

            assertThat(env.executeTrinoSql("SHOW TABLES IN delta." + schemaName))
                    .containsOnly(row(tableName));

            assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1));

            // insert
            assertThat(env.executeTrinoSql("INSERT INTO " + deltaTableName + " VALUES 2"))
                    .containsOnly(row(1));
            assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1), row(2));
            assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1), row(2));

            // update
            assertThat(env.executeTrinoSql("UPDATE " + deltaTableName + " SET id = -2 WHERE id = 2"))
                    .containsOnly(row(1));
            assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1), row(-2));
            assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1), row(-2));

            // delete
            assertThat(env.executeTrinoSql("DELETE FROM " + deltaTableName + " WHERE id = -2"))
                    .containsOnly(row(1));
            assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1));
            assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1));

            // insert again
            assertThat(env.executeTrinoSql("INSERT INTO " + deltaTableName + " VALUES 2, 3"))
                    .containsOnly(row(2));
            assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                    .containsOnly(row(1), row(2), row(3));
            assertThat(env.executeDatabricksSql("SELECT * FROM " + unityTableName))
                    .containsOnly(row(1), row(2), row(3));
        }
        finally {
            dropDeltaTableWithRetry(env, unityTableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTableReadWriteManagedTable(DatabricksUnityEnvironment env)
    {
        assertThat(env.executeTrinoSql("SHOW SCHEMAS FROM delta"))
                .contains(row(schemaName.toLowerCase(ENGLISH)));

        String tableName = "test_read_write_managed_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);

        env.executeDatabricksSql("CREATE TABLE " + unityTableName + " (c1 int, c2 string)");
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        env.executeDatabricksSql("INSERT INTO " + unityTableName + " VALUES (1, 'one')");

        assertThat(env.executeTrinoSql("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName.toLowerCase(ENGLISH)));

        // select
        assertThat(env.executeTrinoSql("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));

        // insert
        assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO " + deltaTableName + " VALUES (2, 'two')"))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // update
        assertThatThrownBy(() -> env.executeTrinoSql("UPDATE " + deltaTableName + " SET c2 = 'two' WHERE c1 = 1"))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // delete
        assertThatThrownBy(() -> env.executeTrinoSql("DELETE FROM " + deltaTableName + " WHERE c2 = 'one'"))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");
        assertThatThrownBy(() -> env.executeTrinoSql("DELETE FROM " + deltaTableName))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // truncate
        assertThatThrownBy(() -> env.executeTrinoSql("TRUNCATE TABLE " + deltaTableName))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");

        // merge
        String sourceTableName = "test_source_" + randomNameSuffix();
        String tableLocation2 = format("%s/%s/%s", externalLocationPath, schemaName, sourceTableName);
        env.executeDatabricksSql(format("CREATE TABLE %s.%s.%s (c1 int, c2 string) using delta location '%s'", unityCatalogName, schemaName, sourceTableName, tableLocation2));
        env.executeDatabricksSql(format("INSERT INTO %s.%s.%s values (1, 'one'), (2, 'two'), (3, 'three')", unityCatalogName, schemaName, sourceTableName));

        assertThatThrownBy(() -> env.executeTrinoSql(format("MERGE INTO delta.%s.%s t USING delta.%s.%s s on t.c1 = s.c1 " +
                "WHEN MATCHED THEN UPDATE SET c2 = s.c2 " +
                "WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (s.c1, s.c2)", schemaName, tableName, schemaName, sourceTableName)))
                .hasStackTraceContaining("Writes are not supported on managed tables for Unity metastore");
    }
}
