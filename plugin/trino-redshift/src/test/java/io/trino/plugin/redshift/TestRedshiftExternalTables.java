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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static io.trino.plugin.redshift.TestingRedshiftServer.executeInRedshift;
import static io.trino.plugin.redshift.TestingRedshiftServer.executeInRedshiftWithRetry;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRedshiftExternalTables
        extends AbstractTestQueryFramework
{
    private static final String S3_EXTERNAL_ROOT = requiredNonEmptySystemProperty("test.redshift.s3.external.tables.root");
    private static final String AWS_REGION = requiredNonEmptySystemProperty("test.redshift.aws.region");
    private static final String AWS_ACCESS_KEY = requiredNonEmptySystemProperty("test.redshift.assume.iam.role.aws.access-key");
    private static final String AWS_SECRET_KEY = requiredNonEmptySystemProperty("test.redshift.assume.iam.role.aws.secret-key");

    private final String schemaName = "test_redshift_external_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner runner = RedshiftQueryRunner.builder()
                .setConnectorProperties(Map.of("redshift.external-tables.enabled", "true"))
                .build();
        runner.installPlugin(new HivePlugin());
        runner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.metastore", "glue")
                .put("hive.metastore.glue.region", AWS_REGION)
                .put("hive.metastore.glue.aws-access-key", AWS_ACCESS_KEY)
                .put("hive.metastore.glue.aws-secret-key", AWS_SECRET_KEY)
                .put("hive.metastore.glue.default-warehouse-dir", S3_EXTERNAL_ROOT + "/" + schemaName)
                .put("hive.metastore.glue.sts.region", AWS_REGION)
                .put("hive.metastore.glue.iam-role", IAM_ROLE)
                .put("fs.s3.enabled", "true")
                .put("s3.region", AWS_REGION)
                .put("s3.aws-access-key", AWS_ACCESS_KEY)
                .put("s3.aws-secret-key", AWS_SECRET_KEY)
                .put("s3.sts.region", AWS_REGION)
                .put("s3.iam-role", IAM_ROLE)
                .buildOrThrow());

        runner.installPlugin(new IcebergPlugin());
        runner.createCatalog("iceberg", "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.security", "allow-all")
                .put("iceberg.catalog.type", "glue")
                .put("hive.metastore.glue.region", AWS_REGION)
                .put("hive.metastore.glue.aws-access-key", AWS_ACCESS_KEY)
                .put("hive.metastore.glue.aws-secret-key", AWS_SECRET_KEY)
                .put("hive.metastore.glue.default-warehouse-dir", S3_EXTERNAL_ROOT + "/" + schemaName)
                .put("hive.metastore.glue.sts.region", AWS_REGION)
                .put("hive.metastore.glue.iam-role", IAM_ROLE)
                .put("fs.s3.enabled", "true")
                .put("s3.region", AWS_REGION)
                .put("s3.aws-access-key", AWS_ACCESS_KEY)
                .put("s3.aws-secret-key", AWS_SECRET_KEY)
                .put("s3.sts.region", AWS_REGION)
                .put("s3.iam-role", IAM_ROLE)
                .buildOrThrow());

        // Create Redshift external schema backed by an AWS Glue catalog database
        executeInRedshiftWithRetry(
                """
                CREATE EXTERNAL SCHEMA %s
                FROM DATA CATALOG DATABASE '%s'
                IAM_ROLE '%s'
                CREATE EXTERNAL DATABASE IF NOT EXISTS""".formatted(schemaName, schemaName, IAM_ROLE));

        return runner;
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        executeInRedshift(format("DROP SCHEMA IF EXISTS %s CASCADE", schemaName));
        getQueryRunner().execute("DROP SCHEMA IF EXISTS hive." + schemaName + " CASCADE");
        getFileSystemFactory().create(SESSION)
                .deleteDirectory(Location.of(S3_EXTERNAL_ROOT + "/" + schemaName + "/"));
    }

    private TrinoFileSystemFactory getFileSystemFactory()
    {
        return ((HiveConnector) getDistributedQueryRunner().getCoordinator().getConnector("hive"))
                .getInjector()
                .getInstance(TrinoFileSystemFactory.class);
    }

    @Test
    void testExternalParquetTableRead()
    {
        String tableName = "nation";
        String s3Location = s3Location(schemaName, tableName);

        try {
            // Write Parquet data to S3 via Hive connector
            assertUpdate(
                    "CREATE TABLE hive." + schemaName + "." + tableName +
                            " (nationkey BIGINT, name VARCHAR(25))" +
                            " WITH (format = 'PARQUET', external_location = '" + s3Location + "')");
            assertUpdate(
                    "INSERT INTO hive." + schemaName + "." + tableName + " VALUES" +
                            " (0, 'ALGERIA')," +
                            " (1, 'ARGENTINA')," +
                            " (2, 'BRAZIL')",
                    3);

            // Verify schema discovery
            assertThat(computeActual("SHOW SCHEMAS FROM redshift").getOnlyColumnAsSet())
                    .contains(schemaName);

            // Verify table discovery
            assertThat(computeActual("SHOW TABLES FROM redshift." + schemaName).getOnlyColumnAsSet())
                    .contains(tableName);

            // Verify data matches what was written through the Hive connector
            assertQuery(
                    format("SELECT nationkey, name FROM redshift.%s.%s", schemaName, tableName),
                    "VALUES (0, 'ALGERIA'), (1, 'ARGENTINA'), (2, 'BRAZIL')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS redshift.%s.%s", schemaName, tableName));
        }
    }

    @Test
    void testExternalPartitionedParquetTableRead()
    {
        String tableName = "nation_by_region";
        String s3Location = s3Location(schemaName, tableName);

        try {
            // Write partitioned Parquet data to S3 via Hive connector
            assertUpdate(
                    "CREATE TABLE hive." + schemaName + "." + tableName +
                            " (nationkey BIGINT, name VARCHAR(25), region VARCHAR(10))" +
                            " WITH (format = 'PARQUET', external_location = '" + s3Location + "', partitioned_by = ARRAY['region'])");
            assertUpdate(
                    "INSERT INTO hive." + schemaName + "." + tableName + " VALUES" +
                            " (0, 'ALGERIA', 'AFRICA')," +
                            " (4, 'ETHIOPIA', 'AFRICA')," +
                            " (1, 'ARGENTINA', 'AMERICA')," +
                            " (2, 'BRAZIL', 'AMERICA')",
                    4);

            // Verify table discovery
            assertThat(computeActual("SHOW TABLES FROM redshift." + schemaName).getOnlyColumnAsSet())
                    .contains(tableName);

            // Verify all partitions are readable
            assertQuery(
                    format("SELECT nationkey, name, region FROM redshift.%s.%s ORDER BY nationkey", schemaName, tableName),
                    "VALUES (0, 'ALGERIA', 'AFRICA'), (1, 'ARGENTINA', 'AMERICA'), (2, 'BRAZIL', 'AMERICA'), (4, 'ETHIOPIA', 'AFRICA')");

            // Verify partition predicate pushdown
            assertQuery(
                    format("SELECT nationkey, name FROM redshift.%s.%s WHERE region = 'AFRICA' ORDER BY nationkey", schemaName, tableName),
                    "VALUES (0, 'ALGERIA'), (4, 'ETHIOPIA')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS redshift.%s.%s", schemaName, tableName));
        }
    }

    @Test
    void testExternalParquetTableAllTypesRead()
    {
        String tableName = "all_types";
        String s3Location = s3Location(schemaName, tableName);

        try {
            assertUpdate(
                    "CREATE TABLE hive." + schemaName + "." + tableName + " (" +
                            "  c_boolean BOOLEAN," +
                            "  c_smallint SMALLINT," +
                            "  c_int INTEGER," +
                            "  c_bigint BIGINT," +
                            "  c_real REAL," +
                            "  c_double DOUBLE," +
                            "  c_decimal_10_0 DECIMAL(10,0)," +
                            "  c_decimal_10_2 DECIMAL(10,2)," +
                            "  c_decimal_38_5 DECIMAL(38,5)," +
                            "  c_varchar_10 VARCHAR(10)," +
                            "  c_varchar VARCHAR," +
                            "  c_varbinary VARBINARY," +
                            "  c_date DATE," +
                            "  c_timestamp TIMESTAMP(3))" +
                            " WITH (format = 'PARQUET', external_location = '" + s3Location + "')");
            assertUpdate(
                    "INSERT INTO hive." + schemaName + "." + tableName + " VALUES (" +
                            "  true," +
                            "  SMALLINT '32767'," +
                            "  2147483647," +
                            "  BIGINT '9223372036854775807'," +
                            "  REAL '123.345'," +
                            "  234.567," +
                            "  DECIMAL '346'," +
                            "  DECIMAL '12345678.91'," +
                            "  DECIMAL '1234567890123456789012.34567'," +
                            "  'ala ma kot'," +
                            "  'ala ma kota'," +
                            "  CAST('bcd1' AS VARBINARY)," +
                            "  DATE '2015-05-10'," +
                            "  TIMESTAMP '2015-05-10 12:15:35.123')",
                    1);

            assertQuery(
                    """
                    SELECT c_boolean, c_smallint, c_int, c_bigint, c_real, c_double,
                    c_decimal_10_0, c_decimal_10_2, c_decimal_38_5,
                    c_varchar_10, c_varchar, c_varbinary, c_date
                    FROM redshift.%s.%s""".formatted(schemaName, tableName),
                    "VALUES (" +
                            "  true," +
                            "  CAST(32767 AS SMALLINT)," +
                            "  2147483647," +
                            "  CAST(9223372036854775807 AS BIGINT)," +
                            "  CAST(123.345 AS REAL)," +
                            "  234.567," +
                            "  CAST(346 AS DECIMAL(10,0))," +
                            "  CAST(12345678.91 AS DECIMAL(10,2))," +
                            "  CAST(1234567890123456789012.34567 AS DECIMAL(38,5))," +
                            "  'ala ma kot'," +
                            "  'ala ma kota'," +
                            "  X'62636431'," +
                            "  DATE '2015-05-10')");
            // TIMESTAMP exact-value comparison is skipped: the Hive Parquet writer and Redshift Spectrum
            // apply different timezone interpretations to INT64 timestamps, producing a cluster- and
            // JVM-timezone-dependent offset that cannot be reliably normalized in a test.
            assertThat(computeScalar(format("SELECT c_timestamp FROM redshift.%s.%s", schemaName, tableName)))
                    .isNotNull();
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS redshift.%s.%s", schemaName, tableName));
        }
    }

    @Test
    void testCreateRegularTableInExternalSchema()
    {
        String tableName = "external_regular_table_" + randomNameSuffix();
        assertQueryFails(
                format("CREATE TABLE redshift.%s.%s (id BIGINT, name VARCHAR(50))", schemaName, tableName),
                ".*connector does not support DDL operations on Redshift external schemas.*");
        assertQueryFails(
                format("CREATE TABLE redshift.%s.%s AS SELECT 1 AS id, 'test' AS name", schemaName, tableName),
                ".*connector does not support DDL operations on Redshift external schemas.*");
    }

    @Test
    void testDmlOnExternalParquetTable()
    {
        String tableName = "external_dml_test_" + randomNameSuffix();
        String s3Location = s3Location(schemaName, tableName);

        try {
            assertUpdate(
                    "CREATE TABLE hive." + schemaName + "." + tableName +
                            " (id BIGINT, name VARCHAR(50))" +
                            " WITH (format = 'PARQUET', external_location = '" + s3Location + "')");

            assertTrinoExceptionThrownBy(() -> getQueryRunner().execute(format("INSERT INTO redshift.%s.%s VALUES (2, 'Bob')", schemaName, tableName)))
                    .hasErrorCode(NOT_SUPPORTED);
            assertTrinoExceptionThrownBy(() -> getQueryRunner().execute(format("UPDATE redshift.%s.%s SET name = 'Alicia' WHERE id = 1", schemaName, tableName)))
                    .hasErrorCode(NOT_SUPPORTED);
            assertTrinoExceptionThrownBy(() -> getQueryRunner().execute(format("DELETE FROM redshift.%s.%s WHERE id = 1", schemaName, tableName)))
                    .hasErrorCode(NOT_SUPPORTED);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS hive." + schemaName + "." + tableName);
        }
    }

    @Test
    void testExternalIcebergTableRead()
    {
        String tableName = "iceberg_dml_test_" + randomNameSuffix();
        String s3Location = s3Location(schemaName, tableName);

        try {
            assertUpdate(format(
                    "CREATE TABLE iceberg.%s.%s (id BIGINT, name VARCHAR(50)) WITH (location = '%s')",
                    schemaName,
                    tableName,
                    s3Location));

            assertUpdate(format("INSERT INTO iceberg.%s.%s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", schemaName, tableName), 3);
            assertQuery(
                    format("SELECT id, name FROM redshift.%s.%s", schemaName, tableName),
                    "VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            assertUpdate(format("UPDATE iceberg.%s.%s SET name = 'Bobby' WHERE id = 2", schemaName, tableName), 1);
            assertQuery(
                    format("SELECT id, name FROM redshift.%s.%s", schemaName, tableName),
                    "VALUES (1, 'Alice'), (2, 'Bobby'), (3, 'Charlie')");

            assertUpdate(format("DELETE FROM iceberg.%s.%s WHERE id = 3", schemaName, tableName), 1);
            assertQuery(
                    format("SELECT id, name FROM redshift.%s.%s", schemaName, tableName),
                    "VALUES (1, 'Alice'), (2, 'Bobby')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS iceberg.%s.%s", schemaName, tableName));
        }
    }

    @Test
    void testDropExternalSchema()
    {
        String externalSchemaName = "test_redshift_external_drop_schema_" + randomNameSuffix();

        try {
            executeInRedshiftWithRetry(
                    """
                    CREATE EXTERNAL SCHEMA %s
                    FROM DATA CATALOG DATABASE '%s'
                    IAM_ROLE '%s'
                    CREATE EXTERNAL DATABASE IF NOT EXISTS""".formatted(externalSchemaName, externalSchemaName, IAM_ROLE));

            // Verify the schema is visible through Trino
            assertThat(computeActual("SHOW SCHEMAS FROM redshift").getOnlyColumnAsSet())
                    .contains(externalSchemaName);

            // Dropping an external schema via Trino is allowed
            assertUpdate(format("DROP SCHEMA redshift.%s", externalSchemaName));

            // Verify the schema is no longer visible
            assertThat(computeActual("SHOW SCHEMAS FROM redshift").getOnlyColumnAsSet())
                    .doesNotContain(externalSchemaName);
        }
        finally {
            executeInRedshift(format("DROP SCHEMA IF EXISTS %s CASCADE", externalSchemaName));
            getQueryRunner().execute("DROP SCHEMA IF EXISTS hive." + externalSchemaName + " CASCADE");
        }
    }

    @Test
    void testDropExternalTable()
    {
        String tableName = "external_drop_test_" + randomNameSuffix();

        try {
            // Create an external table directly in Redshift (no S3 data required for DDL)
            executeInRedshiftWithRetry(
                    """
                    CREATE EXTERNAL TABLE %s.%s (id BIGINT, name VARCHAR(50))
                    STORED AS PARQUET LOCATION '%s'""".formatted(schemaName, tableName, s3Location(schemaName, tableName)));

            assertThat(computeActual(format("SHOW TABLES FROM redshift.%s", schemaName)).getOnlyColumnAsSet())
                    .contains(tableName);

            assertUpdate(format("DROP TABLE redshift.%s.%s", schemaName, tableName));
            assertThat(computeActual(format("SHOW TABLES FROM redshift.%s", schemaName)).getOnlyColumnAsSet())
                    .doesNotContain(tableName);
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS redshift.%s.%s", schemaName, tableName));
        }
    }

    @Test
    void testRegularTableReadWrite()
    {
        try (TestTable table = new TestTable(
                new TrinoSqlExecutorWithRetries(getQueryRunner()),
                "regular_table_",
                "(id BIGINT, name VARCHAR(50))",
                ImmutableList.of("1, 'Alice'", "2, 'Bob'"))) {
            // Verify the data can be read back through Trino
            assertQuery("SELECT id, name FROM " + table.getName() + " ORDER BY id", "VALUES (1, 'Alice'), (2, 'Bob')");

            // Verify that INSERT, UPDATE, and DELETE work on regular tables
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'Charlie')", 1);
            assertUpdate("UPDATE " + table.getName() + " SET name = 'Bobby' WHERE id = 2", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 3", 1);
            assertQuery("SELECT id, name FROM " + table.getName() + " ORDER BY id", "VALUES (1, 'Alice'), (2, 'Bobby')");
        }
    }

    @Test
    void testTableAndColumnCommentsOnRegularRedshiftTable()
    {
        String tableComment = "test regular table comment";
        String columnComment = "test regular column comment";

        try (TestTable table = new TestTable(
                new TrinoSqlExecutorWithRetries(getQueryRunner()),
                "comment_regular_",
                "(id BIGINT)")) {
            assertUpdate(format("COMMENT ON TABLE %s IS '%s'", table.getName(), tableComment));
            assertUpdate(format("COMMENT ON COLUMN %s.id IS '%s'", table.getName(), columnComment));

            assertThat((String) computeScalar(format("SHOW CREATE TABLE %s", table.getName())))
                    .contains("COMMENT '%s'".formatted(tableComment));
            assertThat((String) computeScalar(format(
                    "SELECT comment FROM redshift.information_schema.columns" +
                            " WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'id'",
                    TEST_SCHEMA,
                    table.getName())))
                    .isEqualTo(columnComment);
        }
    }

    private static String s3Location(String schemaName, String tableName)
    {
        return S3_EXTERNAL_ROOT + "/" + schemaName + "/" + tableName + "/";
    }
}
