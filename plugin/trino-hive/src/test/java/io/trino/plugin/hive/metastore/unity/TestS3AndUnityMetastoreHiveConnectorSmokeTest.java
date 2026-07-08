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

import com.google.common.collect.ImmutableList;
import dev.failsafe.Failsafe;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.TestHiveConnectorSmokeTest;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY;
import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
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

    private static final DatabricksSqlExecutor DATABRICKS = new DatabricksSqlExecutor(DATABRICKS_UNITY_JDBC_URL, DATABRICKS_LOGIN, DATABRICKS_TOKEN);

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
                .addHiveProperty("fs.s3.enabled", "true")
                .addHiveProperty("s3.region", DATABRICKS_AWS_REGION)
                .addHiveProperty("s3.aws-access-key", DATABRICKS_AWS_ACCESS_KEY_ID)
                .addHiveProperty("s3.aws-secret-key", DATABRICKS_AWS_SECRET_ACCESS_KEY)
                .setCreateTpchSchemas(false)
                .build();
        Failsafe.with(DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY, DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .run(() -> createTpchTables(queryRunner));
        return queryRunner;
    }

    private static void createTpchTables(QueryRunner queryRunner)
    {
        if (queryRunner.execute("SHOW SCHEMAS LIKE '" + SCHEMA_NAME + "'").getMaterializedRows().isEmpty()) {
            String schemaLocation = format("%s/%s", DATABRICKS_UNITY_EXTERNAL_LOCATION, SCHEMA_NAME);
            LOG.info("Creating schema '%s' as it doesn't exist", SCHEMA_NAME);
            DATABRICKS.execute(format("CREATE SCHEMA IF NOT EXISTS %s.%s MANAGED LOCATION '%s'", DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, schemaLocation));
        }
        for (TpchTable<?> tpchTable : REQUIRED_TPCH_TABLES) {
            String tableName = tpchTable.getTableName();
            if (queryRunner.execute("SHOW TABLES LIKE '" + tableName + "'").getMaterializedRows().isEmpty()) {
                LOG.info("Creating table '%s.%s' as it doesn't exist", SCHEMA_NAME, tableName);
                createTable(tableName, queryRunner);
            }
            else {
                long actualRows = (Long) queryRunner.execute("SELECT count(*) FROM tpch.tiny." + tableName).getMaterializedRows().getFirst().getField(0);
                long expectedRows = (Long) queryRunner.execute("SELECT count(*) FROM " + SCHEMA_NAME + "." + tableName)
                        .getMaterializedRows().getFirst().getField(0);
                if (actualRows != expectedRows) {
                    LOG.info("Recreating table '%s.%s' as actual rows [%s] are different than the expected rows [%s]", SCHEMA_NAME, tableName, expectedRows, actualRows);
                    createTable(tableName, queryRunner);
                }
            }
        }
        DATABRICKS.execute(
                """
                CREATE TABLE IF NOT EXISTS %s.%s.%s
                USING DELTA
                AS SELECT 1 col
                """.formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, DELTA_TABLE_NAME));
    }

    private static void createTable(String tableName, QueryRunner queryRunner)
    {
        String createNationTable =
                """
                CREATE OR REPLACE TABLE %1$s.%2$s.nation
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
        String schemaLocation = getTpchSchemaLocation(queryRunner);
        if (tableName.equals(NATION.getTableName())) {
            DATABRICKS.execute(createNationTable
                    .formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, "%s/%s".formatted(schemaLocation, tableName)));
        }
        else if (tableName.equals(REGION.getTableName())) {
            DATABRICKS.execute(createRegionTable
                    .formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, "%s/%s".formatted(schemaLocation, tableName), tableName));
        }
        else {
            throw new IllegalArgumentException("Unsupported table name: " + tableName);
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

    @Override
    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("Invalid table type: MANAGED_TABLE, create table is supported only for external tables");
    }

    @Override
    @Test
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("Invalid table type: MANAGED_TABLE, create table is supported only for external tables");
    }

    @Disabled("https://starburstdata.atlassian.net/browse/ENG-19790")
    @Test
    void testCreateExternalTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", DATABRICKS_UNITY_EXTERNAL_LOCATION, SCHEMA_NAME, tableName);
        assertUpdate("CREATE TABLE " + tableName +
                " (col_boolean boolean, " +
                "col_tinyint tinyint, " +
                "col_smallint smallint, " +
                "col_integer integer, " +
                "col_bigint bigint, " +
                "col_real real, " +
                "col_double double, " +
                "col_decimal decimal(10,0), " +
                "col_decimal_prec_short decimal(4,2), " +
                "col_decimal_prec_long decimal(19,9), " +
                "col_char char," +
                "col_varchar varchar, " +
                "col_varbinary varbinary, " +
                "col_date date, " +
                "col_timestamp timestamp(3), " +
                "col_array array(integer), " +
                "col_map map(timestamp(3), integer), " +
                "col_row row(a bigint, b varchar)) WITH (external_location='" + tableLocation + "')");
        try {
            assertThat(query("SELECT * FROM " + tableName))
                    .returnsEmptyResult();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testTruncateTable()
    {
        abort("io.trino.testing.BaseConnectorSmokeTest.testTruncateTable truncates the static table used in the test");
    }

    @Disabled("https://starburstdata.atlassian.net/browse/ENG-19790")
    @Test
    void testCreateExternalTableAsSelect()
    {
        String tableName = "test_create_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", DATABRICKS_UNITY_EXTERNAL_LOCATION, SCHEMA_NAME, tableName);
        assertUpdate("CREATE TABLE " + tableName + " WITH (external_location='" + tableLocation + "') " +
                "AS SELECT CAST(array[row(1, row(10), array[row(11)], map(array[2], array[row(1)]))] " +
                "AS array(row(a integer, b row(x integer), c array(row(v integer)), d map(integer, row(field integer))))) AS col_1", 1);
        try {
            assertThat(query("SELECT * FROM " + tableName)).matches("SELECT CAST(array[row(1, row(10), array[row(11)], map(array[2], array[row(1)]))] " +
                    "AS array(row(a integer, b row(x integer), c array(row(v integer)), d map(integer, row(field integer)))))");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("Invalid table type: VIRTUAL_VIEW, create table is supported only for external tables");
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
        String tableName = "hive_table_" + randomNameSuffix();
        String schemaLocation = getTpchSchemaLocation(getQueryRunner());
        try {
            DATABRICKS.execute(
                    """
                    CREATE TABLE IF NOT EXISTS %s.%s.%s (c int)
                    USING PARQUET
                    LOCATION '%s'
                    """.formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, tableName, "%s/%s".formatted(schemaLocation, tableName)));
            assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES 1");
        }
        finally {
            DATABRICKS.execute("DROP TABLE IF EXISTS %s.%s.%s".formatted(DATABRICKS_UNITY_CATALOG_NAME, SCHEMA_NAME, tableName));
        }
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(
                        """
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
        // Override it again to BaseConnectorSmokeTest.testCreateSchemaWithNonLowercaseOwnerName
        // because hive.security = true, Unity metastore doesn't have implementation of listTablePrivileges
        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.ofUser("ADMIN"))
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        try {
            assertThat(query(newSession, "SHOW SCHEMAS"))
                    .skippingTypesCheck()
                    .containsAll(format("VALUES '%s'", schemaName));
        }
        finally {
            assertUpdate(newSession, "DROP SCHEMA " + schemaName);
        }
    }

    @Override
    @Test
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining("Assumption failed: assumption is not true");
    }

    @Disabled("https://starburstdata.atlassian.net/browse/ENG-19790")
    @Test
    public void testColumnNameExternalTable()
    {
        for (String columnName : testColumnNameTestData()) {
            testColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    @Override
    protected String getCreateTableDefaultDefinition()
    {
        return "(col_boolean boolean, " +
                "col_tinyint tinyint, " +
                "col_smallint smallint, " +
                "col_integer integer, " +
                "col_bigint bigint, " +
                "col_real real, " +
                "col_double double, " +
                "col_decimal decimal(10,0), " +
                "col_decimal_prec_short decimal(4,2), " +
                "col_decimal_prec_long decimal(19,9), " +
                "col_char char," +
                "col_varchar varchar, " +
                "col_varbinary varbinary, " +
                "col_timestamp timestamp(3), " +
                "col_date date, " +
                "col_array array(integer), " +
                "col_map map(timestamp(3), integer), " +
                "col_row row(a bigint, b varchar))";
    }

    private void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", DATABRICKS_UNITY_EXTERNAL_LOCATION, SCHEMA_NAME, tableName);

        try {
            assertUpdate("CREATE TABLE " + tableName + "(key varchar(50), " + nameInSql + " varchar(50))" + " WITH (external_location='" + tableLocation + "') ");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

            // SELECT *
            assertQuery("SELECT * FROM " + tableName, "VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')");

            // projection
            assertQuery("SELECT " + nameInSql + " FROM " + tableName, "VALUES (NULL), ('abc'), ('xyz')");

            // predicate
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    private boolean isColumnNameRejected(Exception exception, String columnName)
    {
        return switch (columnName) {
            case " aleadingspace" -> "Hive column names must not start with a space: ' aleadingspace'".equals(exception.getMessage());
            case "atrailingspace " -> "Hive column names must not end with a space: 'atrailingspace '".equals(exception.getMessage());
            case "a,comma" -> "Hive column names must not contain commas: 'a,comma'".equals(exception.getMessage());
            default -> false;
        };
    }

    private static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    private List<String> testColumnNameTestData()
    {
        return ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("an_underscore")
                .add("a-hyphen-minus") // ASCII '-' is HYPHEN-MINUS in Unicode
                .add("a space")
                .add("atrailingspace ")
                .add(" aleadingspace")
                .add("a.dot")
                .add("a,comma")
                .add("a:colon")
                .add("a;semicolon")
                .add("an@at")
                .add("a\"quote")
                .add("an'apostrophe")
                .add("a`backtick`")
                .add("a/slash`")
                .add("a\\backslash`")
                .add("adigit0")
                .add("0startwithdigit")
                .add("カラム")
                .build();
    }
}
