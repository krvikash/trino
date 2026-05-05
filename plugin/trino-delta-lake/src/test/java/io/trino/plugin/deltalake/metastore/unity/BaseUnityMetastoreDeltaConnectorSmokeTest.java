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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

abstract class BaseUnityMetastoreDeltaConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(BaseUnityMetastoreDeltaConnectorSmokeTest.class);
    protected static final String SCHEMA_NAME = TPCH_SCHEMA + "_delta_ci_external";
    private static final String HIVE_TABLE_NAME = "hive_table";

    private static final Pattern DATABRICKS_COMMUNICATION_FAILURE_MATCH = Pattern.compile(
            "\\Q[Databricks][\\E(DatabricksJDBCDriver|JDBCDriver)\\Q](500593) Communication link failure. Failed to connect to server. Reason: " +
            "TemporarilyUnavailableRetry timeout of 900 seconds has been hit.*");
    private static final String DATABRICKS_CLUSTER_PENDING_MATCH = "The current cluster state is Pending";
    private static final String DATABRICKS_CLUSTER_TERMINATED_MATCH = "The current cluster state is Terminated";
    private static final RetryPolicy<Object> DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(BaseUnityMetastoreDeltaConnectorSmokeTest::isDatabricksCommunicationFailure)
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(30)
            .onRetry(event -> LOG.warn(event.getLastException(), "Query failed on attempt %d, will retry (communication failure).", event.getAttemptCount()))
            .build();
    private static final RetryPolicy<Object> DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(BaseUnityMetastoreDeltaConnectorSmokeTest::isClusterUnavailable)
            .withDelay(Duration.of(30, ChronoUnit.SECONDS))
            .withMaxRetries(40)
            .onRetry(event -> LOG.warn(event.getLastException(), "Query failed on attempt %d, will retry (cluster unavailable).", event.getAttemptCount()))
            .build();

    protected abstract Map<String, String> getDeltaLakeProperties();

    protected abstract SqlExecutor onDatabricks();

    protected abstract String getDatabricksUnityExternalLocation();

    protected abstract String getDatabricksUnityCatalogName();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder(SCHEMA_NAME)
                .addDeltaProperty("hive.metastore", "unity")
                .addDeltaProperty("hive.metastore.unity.catalog-name", getDatabricksUnityCatalogName())
                .addDeltaProperty("delta.security", "allow-all")
                .addDeltaProperty("fs.hadoop.enabled", "false")
                .addDeltaProperties(getDeltaLakeProperties())
                .setCreateTpchSchemas(false)
                .build();

        Failsafe.with(DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY, DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .run(() -> createTpchTables(queryRunner));
        return queryRunner;
    }

    private static boolean isDatabricksCommunicationFailure(Throwable throwable)
    {
        if (isClusterUnavailable(throwable)) {
            return false;
        }
        Throwable rootCause = Throwables.getRootCause(throwable);
        return rootCause instanceof SQLException &&
                rootCause.getMessage() != null &&
                DATABRICKS_COMMUNICATION_FAILURE_MATCH.matcher(rootCause.getMessage()).find();
    }

    private static boolean isClusterUnavailable(Throwable throwable)
    {
        String stackTrace = getStackTraceAsString(throwable);
        return stackTrace.contains(DATABRICKS_CLUSTER_PENDING_MATCH) || stackTrace.contains(DATABRICKS_CLUSTER_TERMINATED_MATCH)
                // safe to retry 502 only during session open — no statement was executed
                || (stackTrace.contains("HTTP request failed by code: 502") && stackTrace.contains("TOpenSessionReq"));
    }

    private void createTpchTables(QueryRunner queryRunner)
    {
        if (queryRunner.execute("SHOW SCHEMAS LIKE '" + SCHEMA_NAME + "'").getMaterializedRows().isEmpty()) {
            String schemaLocation = format("%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME);
            LOG.info("Creating schema '%s' as it doesn't exist", SCHEMA_NAME);
            onDatabricks().execute(format("CREATE SCHEMA IF NOT EXISTS %s.%s MANAGED LOCATION '%s'", getDatabricksUnityCatalogName(), SCHEMA_NAME, schemaLocation));
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
        String schemaLocation = getTpchSchemaLocation(queryRunner);
        onDatabricks().execute("""
                CREATE TABLE IF NOT EXISTS %s.%s.%s
                USING PARQUET
                LOCATION '%s'
                AS SELECT n_nationkey as nationkey, n_name as name, n_regionkey as regionkey, n_comment as comment from SAMPLES.TPCH.nation
                """.formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, HIVE_TABLE_NAME, "%s/%s".formatted(schemaLocation, HIVE_TABLE_NAME)));
    }

    private void createTable(String tableName, QueryRunner queryRunner)
    {
        String createNationTable =
                """
                        CREATE OR REPLACE TABLE %1$s.%2$s.nation
                        USING DELTA
                        LOCATION '%3$s'
                        AS SELECT n_nationkey as nationkey, n_name as name, n_regionkey as regionkey, n_comment as comment from SAMPLES.TPCH.nation
                        """;
        String createRegionTable =
                """
                        CREATE OR REPLACE TABLE %1$s.%2$s.region
                        USING DELTA
                        LOCATION '%3$s'
                        AS SELECT r_regionkey as regionkey, r_name as name, r_comment as comment FROM SAMPLES.TPCH.region
                        """;
        String schemaLocation = getTpchSchemaLocation(queryRunner);
        if (tableName.equals(NATION.getTableName())) {
            onDatabricks().execute(createNationTable
                    .formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, "%s/%s".formatted(schemaLocation, tableName)));
        }
        else if (tableName.equals(REGION.getTableName())) {
            onDatabricks().execute(createRegionTable
                    .formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, "%s/%s".formatted(schemaLocation, tableName)));
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
    void testCatalogManagedTable()
    {
        String tableName = "catalog_managed_table_" + randomNameSuffix();
        onDatabricks().execute("""
                CREATE TABLE IF NOT EXISTS %s.%s.%s (id int)
                USING DELTA
                TBLPROPERTIES('delta.feature.catalogManaged' = 'supported', 'delta.enableRowTracking' = 'false', 'delta.checkpointPolicy' = 'classic')
                """.formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        try {
            assertQueryReturnsEmptyResult("TABLE " + tableName);
            onDatabricks().execute("INSERT INTO %s.%s.%s VALUES 1, 2, 3".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
            assertThat(query("TABLE " + tableName)).matches("VALUES 1, 2, 3");

            assertUpdate("INSERT INTO " + tableName + " VALUES 10, 20", 2);
            assertThat(query("TABLE " + tableName)).matches("VALUES 1, 2, 3, 10, 20");
            assertUpdate("UPDATE " + tableName + " SET id = -10 WHERE id = 10", 1);
            assertThat(query("TABLE " + tableName)).matches("VALUES 1, 2, 3, -10, 20");
            assertUpdate("DELETE FROM " + tableName + " WHERE id = -10", 1);
            assertThat(query("TABLE " + tableName)).matches("VALUES 1, 2, 3, 20");
        }
        finally {
            onDatabricks().execute("DROP TABLE IF EXISTS %s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        }
    }

    @Test
    void testReadTimestampNtz()
    {
        String tableName = "read_timestamp_ntz_" + randomNameSuffix();
        try {
            onDatabricks().execute("""
                    CREATE TABLE %s.%s.%s (id int, ts_ntz timestamp_ntz)
                    USING DELTA
                    """.formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
            onDatabricks().execute("INSERT INTO %s.%s.%s VALUES (1, timestamp_ntz '2023-10-01 12:34:56.123456'), (2, timestamp_ntz '2025-01-01 12:34:56.123456')".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES (1, TIMESTAMP '2023-10-01 12:34:56.123456'), (2, TIMESTAMP '2025-01-01 12:34:56.123456')");
        }
        finally {
            onDatabricks().execute("DROP TABLE IF EXISTS %s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        }
    }

    @Test
    void testExternalTableReadWriteTimestampNtz()
    {
        String tableName = "external_read_write_timestamp_ntz_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME, tableName);
        try {
            onDatabricks().execute("""
                    CREATE TABLE %s.%s.%s (id int, ts_ntz timestamp_ntz)
                    USING DELTA
                    LOCATION '%s'
                    """.formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName, tableLocation));

            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2023-10-01 12:34:56.123456'), (2, TIMESTAMP '2025-01-01 12:34:56.123456')", 2);
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES (1, TIMESTAMP '2023-10-01 12:34:56.123456'), (2, TIMESTAMP '2025-01-01 12:34:56.123456')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testFieldNameWithHyphen()
    {
        String tableName = "test_field_name_with_hyphen" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName);
        onDatabricks().execute("CREATE TABLE " + unityTableName + " USING delta AS SELECT named_struct('a-hyphen', 123) x");
        try {
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT ROW(123 AS \"a-hyphen\")");
        }
        finally {
            onDatabricks().execute("DROP TABLE " + unityTableName);
        }
    }

    @Test
    void testShowTablesWithoutTableScanRedirection()
    {
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(NATION.getTableName(), REGION.getTableName())
                .doesNotContain(HIVE_TABLE_NAME);
    }

    @Override
    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("Writes are not supported on managed tables for Unity metastore");
    }

    @Override
    @Test
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("Failed to write Delta Lake transaction log entry");
    }

    @Test
    void testCreateExternalTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME, tableName);
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
                     "col_row row(a bigint, b varchar)) WITH (location='" + tableLocation + "')");
        try {
            assertThat(query("SELECT * FROM " + tableName))
                    .returnsEmptyResult();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateExternalTableAsSelect()
    {
        String tableName = "test_create_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME, tableName);
        assertUpdate("CREATE TABLE " + tableName + " WITH (location='" + tableLocation + "') " +
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

    @Test
    public void testVariantForExternalTable()
    {
        String tableName = "test_variant_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME, tableName);
        // exercise the creation with variant type column
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, v json) WITH (location='" + tableLocation + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, NULL), (2, json '{\"key\":\"value\"}')", 2);
        // test databricks can read Trino written value
        // TODO: check the value
        onDatabricks().execute("SELECT * FROM %s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testVariantForExternalTableCtas()
    {
        String tableName = "test_variant_ctas_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME, tableName);
        // exercise the creation with variant type column
        assertUpdate("CREATE TABLE " + tableName + " WITH (location='" + tableLocation + "') AS SELECT 1 id, json '{\"key\":\"value\"}' v", 1);
        // test databricks can read Trino written value
        // TODO: check the value
        onDatabricks().execute("SELECT * FROM %s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        assertUpdate("DROP TABLE " + tableName);
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
                .hasMessageContaining("renameTable is not supported for Unity metastore");
    }

    @Override
    @Test
    public void testInsert()
    {
        String tableName = "delta_table_" + randomNameSuffix();
        try {
            onDatabricks().execute("""
                    CREATE TABLE IF NOT EXISTS %s.%s.%s (c int)
                    USING DELTA
                    """.formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
            assertQueryFails("INSERT INTO " + tableName + " VALUES (1)", "Writes are not supported on managed tables for Unity metastore");
        }
        finally {
            onDatabricks().execute("DROP TABLE IF EXISTS %s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        }
    }

    @Test
    void testDisallowOtherWriteOperations()
    {
        String tableName = "delta_table_disallow_" + randomNameSuffix();
        try {
            onDatabricks().execute("""
                    CREATE TABLE %s.%s.%s (c int, d int NOT NULL)
                    USING DELTA
                    TBLPROPERTIES ('delta.enableRowTracking'='false')
                    """.formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
            assertQueryFails("COMMENT ON TABLE " + tableName + " IS 'comment'", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("COMMENT ON COLUMN " + tableName + ".c IS 'comment'", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN x int", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN c", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("ALTER TABLE " + tableName + " RENAME COLUMN c TO b", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN d DROP NOT NULL", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = false", "Writes are not supported on managed tables for Unity metastore");
            assertQueryFails("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE", "Writes are not supported on managed tables for Unity metastore");
        }
        finally {
            onDatabricks().execute("DROP TABLE IF EXISTS %s.%s.%s".formatted(getDatabricksUnityCatalogName(), SCHEMA_NAME, tableName));
        }
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                        CREATE TABLE delta.%s.region (
                           regionkey bigint,
                           name varchar,
                           comment varchar
                        )
                        WITH (
                           location = '%s/%s/region'
                        )""".formatted(SCHEMA_NAME, getDatabricksUnityExternalLocation(), SCHEMA_NAME));
    }

    @Override
    @Test
    public void testMerge()
    {
        abort("io.trino.testing.BaseConnectorSmokeTest.testMerge updates the static table used in the test");
    }

    @Override
    @Test
    public void testTruncateTable()
    {
        abort("io.trino.testing.BaseConnectorSmokeTest.testTruncateTable truncates the static table used in the test");
    }

    @Override
    @Test
    public void testRowLevelUpdate()
    {
        abort("io.trino.testing.BaseConnectorSmokeTest.testMerge updates the static table used in the test");
    }

    @Test
    public void testColumnNameExternalTable()
    {
        for (String columnName : testColumnNameTestData()) {
            testColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    private void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", getDatabricksUnityExternalLocation(), SCHEMA_NAME, tableName);

        assertUpdate("CREATE TABLE " + tableName + "(key varchar(50), " + nameInSql + " varchar(50))" + " WITH (location='" + tableLocation + "') ");

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
