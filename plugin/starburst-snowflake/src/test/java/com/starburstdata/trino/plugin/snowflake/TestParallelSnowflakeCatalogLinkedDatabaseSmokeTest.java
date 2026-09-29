/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.DATABASE_SEPARATOR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Regression test for <a href="https://starburstdata.atlassian.net/browse/ENG-20718">ENG-20718</a>
 * against a real catalog-linked database (CLDB). Unlike
 * {@code TestSnowflakeCatalogLinkedDatabaseIntegration}, which simulates the stored-case mismatch
 * with quoted lowercase names on a regular database, this test exercises the full engine path
 * (schema listing, {@code schemaExists}, table resolution, column listing, SELECT) against a
 * database whose objects are synced from an external catalog and stored lowercase.
 * <p>
 * The CLDB is a permanent, manually provisioned fixture — tests cannot create one (it requires an
 * AWS Glue catalog integration) and must not modify it, so everything here is read-only. Expected
 * setup in the test account, overridable with the {@code snowflake.test.cldb.*} system properties
 * (an override must point at a table with the same columns and rows):
 * <ul>
 * <li>a catalog-linked database {@code CORK_CICD_CLDB} (stored uppercase),</li>
 * <li>containing a Glue-synced schema {@code cork_cicd_snowflake_cldb} (stored lowercase),</li>
 * <li>containing an Iceberg table {@code employees (emp_id int, emp_name string, department string)}
 * (stored lowercase). </li>
 * </ul>
 * CLDBs are AWS Glue-backed, so the whole class is skipped outright against the Azure test
 * account, which has no such fixture and no way to get one. Against AWS, the fixture is expected
 * to exist, so a missing or inaccessible fixture fails the test with a diagnostic message instead
 * of skipping -- silently skipping could hide a fixture that was actually broken or de-provisioned.
 */
public class TestParallelSnowflakeCatalogLinkedDatabaseSmokeTest
        extends AbstractTestQueryFramework
{
    private static final String CLDB_DATABASE = System.getProperty("snowflake.test.cldb.database", "CORK_CICD_CLDB");
    private static final String CLDB_SCHEMA = System.getProperty("snowflake.test.cldb.schema", "cork_cicd_snowflake_cldb");
    private static final String CLDB_TABLE = System.getProperty("snowflake.test.cldb.table", "employees");

    // Trino exposes remote names lowercased, whatever case Snowflake stores them in
    private final String trinoSchemaName = String.join(DATABASE_SEPARATOR, CLDB_DATABASE.toLowerCase(ENGLISH), CLDB_SCHEMA.toLowerCase(ENGLISH));
    private final String trinoTableName = CLDB_TABLE.toLowerCase(ENGLISH);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        requireFixtureProvisioned();
        return SnowflakeQueryRunner.parallelBuilder()
                .withConnectorProperties(ImmutableMap.of(
                        "snowflake.database-prefix-for-schema.enabled", "true",
                        "snowflake.role", SnowflakeServer.ROLE))
                .build();
    }

    @Test
    public void testShowSchemasListsCatalogLinkedSchema()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains(trinoSchemaName);
    }

    @Test
    public void testShowTablesListsCatalogLinkedTable()
    {
        assertThat(computeActual(format("SHOW TABLES FROM \"%s\"", trinoSchemaName)).getOnlyColumnAsSet())
                .contains(trinoTableName);
    }

    @Test
    public void testSelectFromCatalogLinkedTable()
    {
        // The exact ENG-20718 failure: the identifier mapping uppercases the whole
        // "database.schema" name, but the CLDB schema and table are stored lowercase, so without
        // stored-case resolution this fails with "Schema '...' does not exist"
        assertQuery(
                format("SELECT emp_id, emp_name, department FROM \"%s\".%s", trinoSchemaName, trinoTableName),
                "VALUES (1, 'Alice Smith', 'Engineering'), (2, 'Bob Jones', 'Marketing'), (3, 'Charlie Wayne', 'HR')");
    }

    @Test
    public void testPredicateOnCatalogLinkedTable()
    {
        assertQuery(
                format("SELECT emp_name FROM \"%s\".%s WHERE emp_id = 2", trinoSchemaName, trinoTableName),
                "VALUES 'Bob Jones'");
        assertQuery(
                format("SELECT emp_name FROM \"%s\".%s WHERE department = 'HR'", trinoSchemaName, trinoTableName),
                "VALUES 'Charlie Wayne'");
    }

    @Test
    public void testAggregationOnCatalogLinkedTable()
    {
        assertQuery(
                format("SELECT count(*) FROM \"%s\".%s", trinoSchemaName, trinoTableName),
                "VALUES 3");
        assertQuery(
                format("SELECT count(DISTINCT department) FROM \"%s\".%s", trinoSchemaName, trinoTableName),
                "VALUES 3");
    }

    @Test
    public void testDescribeCatalogLinkedTable()
    {
        assertThat(query(format("DESCRIBE \"%s\".%s", trinoSchemaName, trinoTableName)))
                .result()
                .projected("Column")
                .skippingTypesCheck()
                .matches("VALUES 'emp_id', 'emp_name', 'department'");
    }

    @Test
    public void testInformationSchemaColumnsForCatalogLinkedTable()
    {
        assertQuery(
                format(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'",
                        trinoSchemaName,
                        trinoTableName),
                "VALUES 'emp_id', 'emp_name', 'department'");
    }

    private static void requireFixtureProvisioned()
    {
        if (SnowflakeServer.JDBC_URL.contains(".azure.")) {
            abort("Catalog-linked database (CLDB) fixtures are AWS Glue-backed and not available against the Azure test account");
        }
        try (Connection connection = SnowflakeServer.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE ROLE " + SnowflakeServer.ROLE);
            // '_' in a LIKE pattern matches any single character, so check the exact name.
            try (ResultSet resultSet = statement.executeQuery(
                    format("SHOW ICEBERG TABLES LIKE '%s' IN SCHEMA \"%s\".\"%s\"", CLDB_TABLE, CLDB_DATABASE, CLDB_SCHEMA))) {
                boolean found = false;
                while (resultSet.next()) {
                    found = found || CLDB_TABLE.equals(resultSet.getString("name"));
                }
                if (!found) {
                    fail(fixtureNotReadyMessage(format("table %s.%s.%s is missing or not visible", CLDB_DATABASE, CLDB_SCHEMA, CLDB_TABLE)));
                }
            }
        }
        catch (SQLException e) {
            // Snowflake reports a missing object and a missing grant identically ("does not exist or
            // not authorized"), so both are treated as an unready fixture.
            fail(fixtureNotReadyMessage(format("schema %s.%s could not be listed: %s", CLDB_DATABASE, CLDB_SCHEMA, e.getMessage())), e);
        }
    }

    private static String fixtureNotReadyMessage(String problem)
    {
        return format(
                "Catalog-linked database (CLDB) fixture is not ready for role %s: %s. Provision the AWS Glue-backed CLDB %s " +
                        "with schema %s and Iceberg table %s, grant the role USAGE on the database and schema and SELECT on the table, " +
                        "or point the snowflake.test.cldb.* system properties at an equivalent fixture",
                SnowflakeServer.ROLE,
                problem,
                CLDB_DATABASE,
                CLDB_SCHEMA,
                CLDB_TABLE);
    }
}
