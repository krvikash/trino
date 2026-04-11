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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.deltalake.BaseDeltaLakeSharedMetastoreWithTableRedirectionsTest;
import io.trino.plugin.deltalake.DeltaLakePlugin;
import io.trino.plugin.hive.HivePlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeSharedUnityWithTableRedirections
        extends BaseDeltaLakeSharedMetastoreWithTableRedirectionsTest
{
    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("DATABRICKS_UNITY_JDBC_URL");
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_LOGIN = requireEnv("DATABRICKS_LOGIN");
    private static final String DATABRICKS_TOKEN = requireEnv("DATABRICKS_TOKEN");
    private static final String DATABRICKS_UNITY_CATALOG_NAME = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
    private static final String DATABRICKS_UNITY_EXTERNAL_LOCATION = requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION");

    private static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");
    private static final String DATABRICKS_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_AWS_ACCESS_KEY_ID");
    private static final String DATABRICKS_AWS_SECRET_ACCESS_KEY = requireEnv("DATABRICKS_AWS_SECRET_ACCESS_KEY");

    private final String schemaLocation = DATABRICKS_UNITY_EXTERNAL_LOCATION + "/" + schema;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog("delta_with_redirections")
                .setSchema(schema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        queryRunner.installPlugin(new DeltaLakePlugin());
        queryRunner.createCatalog(
                "delta_with_redirections",
                CONNECTOR_NAME,
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "unity")
                        .put("hive.metastore.unity.host", DATABRICKS_HOST)
                        .put("hive.metastore.unity.token", DATABRICKS_TOKEN)
                        .put("hive.metastore.unity.catalog-name", DATABRICKS_UNITY_CATALOG_NAME)
                        .put("delta.security", "read-only")
                        .put("delta.hive-catalog-name", "hive_with_redirections")
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.s3.enabled", "true")
                        .put("s3.region", DATABRICKS_AWS_REGION)
                        .put("s3.aws-access-key", DATABRICKS_AWS_ACCESS_KEY_ID)
                        .put("s3.aws-secret-key", DATABRICKS_AWS_SECRET_ACCESS_KEY)
                        .put("s3.path-style-access", "true")
                        .buildOrThrow());

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "unity")
                        .put("hive.metastore.unity.host", DATABRICKS_HOST)
                        .put("hive.metastore.unity.token", DATABRICKS_TOKEN)
                        .put("hive.metastore.unity.catalog-name", DATABRICKS_UNITY_CATALOG_NAME)
                        .put("hive.security", "read-only")
                        .put("hive.delta-lake-catalog-name", "delta_with_redirections")
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.s3.enabled", "true")
                        .put("s3.region", DATABRICKS_AWS_REGION)
                        .put("s3.aws-access-key", DATABRICKS_AWS_ACCESS_KEY_ID)
                        .put("s3.aws-secret-key", DATABRICKS_AWS_SECRET_ACCESS_KEY)
                        .put("s3.path-style-access", "true")
                        .buildOrThrow());

        Properties properties = new Properties();
        properties.put("user", DATABRICKS_LOGIN);
        properties.put("password", DATABRICKS_TOKEN);
        try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, properties);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS %s.%s MANAGED LOCATION '%s'".formatted(DATABRICKS_UNITY_CATALOG_NAME, schema, schemaLocation));

            statement.execute("CREATE TABLE %s.%s.%s (a_integer int) USING PARQUET LOCATION '%s'"
                    .formatted(DATABRICKS_UNITY_CATALOG_NAME, schema, "hive_table", "%s/%s".formatted(schemaLocation, "hive_table")));
            statement.execute("INSERT INTO %s.%s.%s VALUES (1), (2), (3)"
                    .formatted(DATABRICKS_UNITY_CATALOG_NAME, schema, "hive_table"));

            statement.execute("CREATE TABLE %s.%s.%s (a_varchar string) USING DELTA"
                    .formatted(DATABRICKS_UNITY_CATALOG_NAME, schema, "delta_table"));
            statement.execute("INSERT INTO %s.%s.%s VALUES ('a'), ('b'), ('c')"
                    .formatted(DATABRICKS_UNITY_CATALOG_NAME, schema, "delta_table"));
        }

        closeAfterClass(() -> {
            try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, properties);
                    Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS %s.%s CASCADE".formatted(DATABRICKS_UNITY_CATALOG_NAME, schema));
            }
        });

        return queryRunner;
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";
        return format(expectedHiveCreateSchema, catalogName, schema, schemaLocation);
    }

    @Override
    protected String getExpectedDeltaLakeCreateSchema(String catalogName)
    {
        String expectedDeltaLakeCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";
        return format(expectedDeltaLakeCreateSchema, catalogName, schema, schemaLocation);
    }
}
