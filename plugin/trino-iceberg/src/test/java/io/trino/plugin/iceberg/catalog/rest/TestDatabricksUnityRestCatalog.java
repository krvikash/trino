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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDatabricksUnityRestCatalog
        extends AbstractTestQueryFramework
{
    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("DATABRICKS_UNITY_JDBC_URL");
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_LOGIN = requireEnv("DATABRICKS_LOGIN");
    private static final String DATABRICKS_TOKEN = requireEnv("DATABRICKS_TOKEN");
    private static final String DATABRICKS_UNITY_CATALOG_NAME = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
    private static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");
    private static final String DATABRICKS_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_AWS_ACCESS_KEY_ID");
    private static final String DATABRICKS_AWS_SECRET_ACCESS_KEY = requireEnv("DATABRICKS_AWS_SECRET_ACCESS_KEY");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder("default")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", "https://%s/api/2.1/unity-catalog/iceberg-rest".formatted(DATABRICKS_HOST))
                .addIcebergProperty("iceberg.rest-catalog.warehouse", DATABRICKS_UNITY_CATALOG_NAME)
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.server-uri", "https://%s/oidc/v1/token".formatted(DATABRICKS_HOST))
                .addIcebergProperty("iceberg.rest-catalog.oauth2.token", DATABRICKS_TOKEN)
                .addIcebergProperty("iceberg.rest-catalog.oauth2.scope", "all-apis")
                .addIcebergProperty("fs.native-s3.enabled", "true")
                .addIcebergProperty("s3.region", DATABRICKS_AWS_REGION)
                .addIcebergProperty("s3.aws-access-key", DATABRICKS_AWS_ACCESS_KEY_ID)
                .addIcebergProperty("s3.aws-secret-key", DATABRICKS_AWS_SECRET_ACCESS_KEY)
                .build();
    }

    @Test
    void testDeletionVectors()
            throws Exception
    {
        try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, DATABRICKS_LOGIN, DATABRICKS_TOKEN);
                Statement statement = connection.createStatement()) {
            statement.execute("SET spark.databricks.delta.dbiManagedIcebergTable.v3.enabled = true");
            statement.execute("SET spark.databricks.delta.uniform.iceberg.v3.enabled = true");
            statement.execute("SET spark.databricks.delta.uniform.iceberg.sync.convert.enabled = true");
            statement.execute("SET spark.databricks.delta.dbiManagedIcebergTable.metadataValidationRate = 0");
            statement.execute("SET spark.databricks.delta.optimizeWrite.enabled = false");
            statement.execute("SET spark.databricks.delta.autoCompact.enabled = false");

            try (TestTable table = new TestTable(
                    (sql) -> onDatabricks(statement, sql),
                    DATABRICKS_UNITY_CATALOG_NAME + ".default.test_dv",
                    "(x INT) TBLPROPERTIES ('delta.universalFormat.enabledFormats'='iceberg', 'delta.enableIcebergCompatV3'='true')",
                    ImmutableList.of("1", "2", "3"))) {
                statement.execute("DELETE FROM " + table.getName() + " WHERE x = 2");

                String trinoTableName = table.getName().substring(table.getName().lastIndexOf('.') + 1);
                assertThat(computeActual("SELECT file_format FROM default.\"" + trinoTableName + "$files\"").getOnlyColumnAsSet())
                        .contains("PUFFIN");
                assertThat(query("SELECT * FROM default." + trinoTableName))
                        .matches("VALUES 1, 3");

                assertQueryFails("INSERT INTO default." + trinoTableName + " VALUES 2", ".*not a Managed Iceberg table.*");
            }
        }
    }

    private static void onDatabricks(Statement statement, String sql)
    {
        try {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
