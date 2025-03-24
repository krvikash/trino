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
package io.trino.plugin.hive.metastore.unity;

import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.TestHiveConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestS3AndUnityMetastoreHiveConnectorSmokeTest
        extends TestHiveConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestS3AndUnityMetastoreHiveConnectorSmokeTest.class);
    private static final String SCHEMA_NAME = TPCH_SCHEMA + "_hive_ci_external";
    private static final String DELTA_TABLE_NAME = "delta_table";

    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("DATABRICKS_UNITY_JDBC_URL");
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_LOGIN = requireEnv("DATABRICKS_LOGIN");
    private static final String DATABRICKS_TOKEN = requireEnv("DATABRICKS_TOKEN");
    private static final String DATABRICKS_UNITY_CATALOG_NAME = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
    private static final String DATABRICKS_UNITY_EXTERNAL_LOCATION = requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION");

    private static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");
    private static final String DATABRICKS_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_AWS_ACCESS_KEY_ID");
    private static final String DATABRICKS_AWS_SECRET_ACCESS_KEY = requireEnv("DATABRICKS_AWS_SECRET_ACCESS_KEY");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog(HIVE_CATALOG)
                        .setSchema(SCHEMA_NAME)
                        .build())
                .addHiveProperty("hive.metastore", "unity")
                .addHiveProperty("hive.metastore.unity.host", DATABRICKS_HOST)
                .addHiveProperty("hive.metastore.unity.token", DATABRICKS_TOKEN)
                .addHiveProperty("hive.metastore.unity.catalog-name", DATABRICKS_UNITY_CATALOG_NAME)
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("hive.non-managed-table-writes-enabled", "true")
                .addHiveProperty("fs.hadoop.enabled", "false")
                .addHiveProperty("fs.native-s3.enabled", "true")
                .addHiveProperty("s3.region", DATABRICKS_AWS_REGION)
                .addHiveProperty("s3.aws-access-key", DATABRICKS_AWS_ACCESS_KEY_ID)
                .addHiveProperty("s3.aws-secret-key", DATABRICKS_AWS_SECRET_ACCESS_KEY)
                .setCreateTpchSchemas(false)
                .build();
        createTpchTables(queryRunner);
        return queryRunner;
    }

    private static void createTpchTables(QueryRunner queryRunner)
            throws Exception
    {
        Properties properties = new Properties();
        properties.put("user", DATABRICKS_LOGIN);
        properties.put("password", DATABRICKS_TOKEN);

        if (queryRunner.execute("SHOW SCHEMAS LIKE '" + SCHEMA_NAME + "'").getMaterializedRows().isEmpty()) {
            String schemaLocation = format("%s/%s", DATABRICKS_UNITY_EXTERNAL_LOCATION, SCHEMA_NAME);
            LOG.info("Creating schema '%s' as it doesn't exist", SCHEMA_NAME);
            try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, properties);
                    Statement statement = connection.createStatement()) {
                statement.execute(format("CREATE SCHEMA IF NOT EXISTS %s.%s MANAGED LOCATION '%s'", DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, schemaLocation));
            }
        }
        for (TpchTable<?> tpchTable : REQUIRED_TPCH_TABLES) {
            String tableName = tpchTable.getTableName();
            if (queryRunner.execute("SHOW TABLES LIKE '" + tableName + "'").getMaterializedRows().isEmpty()) {
                LOG.info("Creating table '%s.%s' as it doesn't exist", SCHEMA_NAME, tableName);
                createTable(tableName, properties, queryRunner);
            }
            else {
                long actualRows = (Long) queryRunner.execute("SELECT count(*) FROM tpch.tiny." + tableName).getMaterializedRows().getFirst().getField(0);
                long expectedRows = (Long) queryRunner.execute("SELECT count(*) FROM " + SCHEMA_NAME + "." + tableName)
                        .getMaterializedRows().getFirst().getField(0);
                if (actualRows != expectedRows) {
                    LOG.info("Recreating table '%s.%s' as actual rows [%s] are different than the expected rows [%s]", SCHEMA_NAME, tableName, expectedRows, actualRows);
                    createTable(tableName, properties, queryRunner);
                }
            }
        }
        try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, properties);
                Statement statement = connection.createStatement()) {
            statement.execute("""
                CREATE TABLE IF NOT EXISTS %s.%s.%s
                USING DELTA
                AS SELECT 1 col
                """.formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, DELTA_TABLE_NAME));
        }
    }

    private static void createTable(String tableName, Properties connectionProperties, QueryRunner queryRunner)
            throws SQLException
    {
        String createNationTable =
                """
                CREATE OR REPLACE TABLE %1$s.%1$s.nation
                USING PARQUET
                LOCATION '%3$s'
                AS SELECT n_nationkey as nationkey, n_name as name, n_regionkey as regionkey, n_comment as comment from SAMPLES.TPCH.nation
                """;
        String createRegionTable =
                """
                CREATE OR REPLACE TABLE %1$s.%2$s.region
                USING PARQUET
                LOCATION '%3$s'
                AS SELECT r_regionkey as regionkey, r_name as name, r_comment as comment FROM SAMPLES.TPCH.region
                """;
        try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, connectionProperties);
                Statement statement = connection.createStatement()) {
            String schemaLocation = getTpchSchemaLocation(queryRunner);
            if (tableName.equals(NATION.getTableName())) {
                statement.execute(createNationTable
                        .formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, "%s/%s".formatted(schemaLocation, tableName)));
            }
            else if (tableName.equals(REGION.getTableName())) {
                statement.execute(createRegionTable
                        .formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, "%s/%s".formatted(schemaLocation, tableName), tableName));
            }
            else {
                throw new IllegalArgumentException("Unsupported table name: " + tableName);
            }
        }
    }

    private static String getTpchSchemaLocation(QueryRunner queryRunner)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher matcher = locationPattern.matcher((String) queryRunner.execute("SHOW CREATE SCHEMA " + SCHEMA_NAME).getOnlyValue());
        if (matcher.find()) {
            String location = matcher.group(1);
            verify(!matcher.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE SCHEMA result");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ADD_FIELD,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_VIEW,
                 SUPPORTS_COMMENT_ON_VIEW_COLUMN,
                 SUPPORTS_CREATE_FUNCTION,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_DROP_FIELD_IN_ARRAY,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MERGE,
                 SUPPORTS_MULTI_STATEMENT_WRITES,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_FIELD,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_REPORTING_WRITTEN_BYTES,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_ROW_LEVEL_UPDATE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testShowTablesWithoutTableScanRedirection()
    {
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(NATION.getTableName(), REGION.getTableName())
                .doesNotContain(DELTA_TABLE_NAME);
    }

    @Test
    void testDropSchema()
    {
        assertThat(query("DROP SCHEMA " + SCHEMA_NAME + " CASCADE"))
                .failure()
                .hasStackTraceContaining("dropTable is not supported for Unity metastore");
    }

    @Test
    void testDropTable()
    {
        assertThat(query("DROP TABLE " + "nation"))
                .failure()
                .hasStackTraceContaining("dropTable is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("createTable is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("createTable is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testTruncateTable()
    {
        abort("io.trino.testing.BaseConnectorSmokeTest.testTruncateTable truncates the static table used in the test");
    }

    @Override
    @Test
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("createDatabase is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createTable is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessageContaining("Table rename is not supported with current metastore configuration");
    }

    @Override // to showcase insert path through a separate table without impacting static region table created for this test 
    @Test
    public void testInsert()
    {
        Properties properties = new Properties();
        properties.put("user", DATABRICKS_LOGIN);
        properties.put("password", DATABRICKS_TOKEN);
        String tableName = "hive_table_" + randomNameSuffix();
        Connection connection = null;
        Statement statement = null;
        String schemaLocation = getTpchSchemaLocation(getQueryRunner());
        try {
            connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, properties);
            statement = connection.createStatement();
            statement.execute("""
                CREATE TABLE IF NOT EXISTS %s.%s.%s (c int)
                USING PARQUET
                LOCATION '%s'
                """.formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, tableName, "%s/%s".formatted(schemaLocation, tableName)));
            assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES 1");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.execute("DROP TABLE IF EXISTS %s.%s.%s".formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, tableName));
                    statement.close();
                    connection.close();
                }
                catch (SQLException ignore) {}
            }
        }
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                           CREATE TABLE hive.%s.region (
                              regionkey bigint,
                              name varchar,
                              comment varchar
                           )
                           WITH (
                              external_location = '%s/%s/region',
                              format = 'PARQUET'
                           )""".formatted(SCHEMA_NAME, DATABRICKS_UNITY_EXTERNAL_LOCATION, SCHEMA_NAME));
    }

    @Override
    @Test
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameDatabase is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessageContaining("Modifying Hive table rows is only supported for transactional tables");
    }

    @Override
    @Test
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessageContaining("Assumption failed: assumption is not true");
    }

    @Override
    @Test
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessageContaining("Modifying Hive table rows is only supported for transactional tables");
    }

    @Override
    @Test
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied: Cannot create schema");
    }

    @Override
    @Test
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining("Assumption failed: assumption is not true");
    }
}
