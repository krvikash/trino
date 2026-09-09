/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugin.snowflake.SnowflakeServer;
import com.starburstdata.trino.plugin.snowflake.TestDatabase;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.InternalTypeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Snowflake databases, schemas, and tables are normally stored uppercase, matching Trino's
 * default identifier mapping. Catalog-linked databases (Iceberg REST catalogs backed by AWS Glue,
 * Unity, etc.) are the practical case where that assumption breaks: objects -- including the
 * database itself -- are synced in and stored in their original, often lowercase, case. A quoted
 * lowercase database/schema/table on a regular (non catalog-linked) test account reproduces the
 * same driver-level case mismatch -- the fix doesn't know or care *why* an object's stored case
 * differs from what the identifier mapping guessed -- without requiring an actual catalog-linked
 * database, which isn't available in this test environment.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSnowflakeCatalogLinkedDatabaseIntegration
{
    private static final String TABLE_NAME = "test_table";

    private TestDatabase testDatabase;
    private String normalizedDatabaseName;
    private String schemaName;
    private String trinoSchemaName;

    // A second database created directly via quoted DDL (rather than through TestDatabase, which
    // always creates unquoted, uppercase-stored databases) so its own name -- not just its
    // schema/table names -- is stored lowercase, standing in for a catalog-linked database's name.
    private String lowerCaseDatabaseName;
    private String lowerCaseDatabaseSchemaName;
    private String lowerCaseDatabaseTrinoSchemaName;

    @BeforeAll
    void setUp()
    {
        testDatabase = SnowflakeServer.createTestDatabase();
        normalizedDatabaseName = testDatabase.getName().toLowerCase(ENGLISH);
        schemaName = "cldb_schema_" + randomNameSuffix();
        trinoSchemaName = normalizedDatabaseName + "." + schemaName;

        // Quoting forces Snowflake to store these names verbatim (lowercase) instead of
        // uppercasing them, standing in for how catalog-linked databases store names.
        SnowflakeServer.safeExecuteOnDatabase(
                testDatabase.getName(),
                "CREATE SCHEMA \"" + schemaName + "\"",
                "CREATE TABLE \"" + schemaName + "\".\"" + TABLE_NAME + "\" (id BIGINT, name VARCHAR)");

        lowerCaseDatabaseName = "cldb_db_" + randomNameSuffix();
        lowerCaseDatabaseSchemaName = "cldb_schema_" + randomNameSuffix();
        lowerCaseDatabaseTrinoSchemaName = lowerCaseDatabaseName + "." + lowerCaseDatabaseSchemaName;

        SnowflakeServer.safeExecuteOnDatabase(
                SnowflakeServer.TEST_DATABASE,
                "CREATE DATABASE \"" + lowerCaseDatabaseName + "\"");
        SnowflakeServer.safeExecuteOnDatabase(
                "\"" + lowerCaseDatabaseName + "\"",
                "CREATE SCHEMA \"" + lowerCaseDatabaseSchemaName + "\"",
                "CREATE TABLE \"" + lowerCaseDatabaseSchemaName + "\".\"" + TABLE_NAME + "\" (id BIGINT, name VARCHAR)");
    }

    @AfterAll
    void tearDown()
            throws IOException
    {
        try {
            testDatabase.close();
        }
        finally {
            SnowflakeServer.safeExecuteOnDatabase(SnowflakeServer.TEST_DATABASE, "DROP DATABASE IF EXISTS \"" + lowerCaseDatabaseName + "\"");
        }
    }

    @Test
    void testGetAllTableCommentsFindsTableInLowerCaseStoredSchema()
    {
        SnowflakeClient client = createClient();

        List<RelationCommentMetadata> result = client.getAllTableComments(
                TestingConnectorSession.builder().build(),
                Optional.of(trinoSchemaName));

        assertThat(result)
                .extracting(RelationCommentMetadata::name)
                .contains(new SchemaTableName(trinoSchemaName, TABLE_NAME));
    }

    @Test
    void testGetAllTableCommentsReturnsEmptyForNonexistentSchema()
    {
        SnowflakeClient client = createClient();

        List<RelationCommentMetadata> result = client.getAllTableComments(
                TestingConnectorSession.builder().build(),
                Optional.of(normalizedDatabaseName + ".nonexistent_schema_" + randomNameSuffix()));

        assertThat(result).isEmpty();
    }

    @Test
    void testGetTableHandleFindsTableInLowerCaseStoredSchema()
    {
        SnowflakeClient client = createClient();

        Optional<JdbcTableHandle> handle = client.getTableHandle(
                TestingConnectorSession.builder().build(),
                new SchemaTableName(trinoSchemaName, TABLE_NAME));

        assertThat(handle).isPresent();
        assertThat(handle.get().asPlainTable().getSchemaTableName())
                .isEqualTo(new SchemaTableName(trinoSchemaName, TABLE_NAME));
    }

    @Test
    void testGetTableHandleReturnsEmptyForNonexistentSchema()
    {
        SnowflakeClient client = createClient();

        Optional<JdbcTableHandle> handle = client.getTableHandle(
                TestingConnectorSession.builder().build(),
                new SchemaTableName(normalizedDatabaseName + ".nonexistent_schema_" + randomNameSuffix(), TABLE_NAME));

        assertThat(handle).isEmpty();
    }

    @Test
    void testGetAllTableColumnsResolvesActualStoredCase()
            throws SQLException
    {
        SnowflakeClient client = createClient();
        try (Connection connection = openRealConnection();
                ResultSet columns = client.getAllTableColumns(connection, Optional.of(trinoSchemaName.toUpperCase(ENGLISH)))) {
            List<String> columnNames = new ArrayList<>();
            while (columns.next()) {
                // Trino's default identifier mapping would uppercase the schema name; the fix
                // must resolve it back to the quoted-lowercase form it's actually stored as, or
                // the driver would silently drop every row here instead.
                assertThat(columns.getString("TABLE_CAT")).isEqualToIgnoringCase(normalizedDatabaseName);
                assertThat(columns.getString("TABLE_SCHEM")).isEqualTo(schemaName);
                columnNames.add(columns.getString("COLUMN_NAME"));
            }
            assertThat(columnNames).contains("ID", "NAME");
        }
    }

    @Test
    void testGetAllTableColumnsReturnsEmptyForNonexistentSchema()
            throws SQLException
    {
        SnowflakeClient client = createClient();
        try (Connection connection = openRealConnection();
                ResultSet columns = client.getAllTableColumns(connection, Optional.of(normalizedDatabaseName + ".nonexistent_schema_" + randomNameSuffix()))) {
            assertThat(columns.next()).isFalse();
        }
    }

    @Test
    void testGetTableHandleFindsTableInLowerCaseStoredCatalogAndSchema()
    {
        SnowflakeClient client = createClient();

        Optional<JdbcTableHandle> handle = client.getTableHandle(
                TestingConnectorSession.builder().build(),
                new SchemaTableName(lowerCaseDatabaseTrinoSchemaName.toUpperCase(ENGLISH), TABLE_NAME.toUpperCase(ENGLISH)));

        assertThat(handle).isPresent();
        assertThat(handle.get().asPlainTable().getRemoteTableName().getCatalogName()).contains(lowerCaseDatabaseName);
        assertThat(handle.get().asPlainTable().getRemoteTableName().getSchemaName()).contains(lowerCaseDatabaseSchemaName);
    }

    @Test
    void testGetAllTableColumnsResolvesActualStoredCatalogCase()
            throws SQLException
    {
        SnowflakeClient client = createClient();
        try (Connection connection = openRealConnection();
                ResultSet columns = client.getAllTableColumns(connection, Optional.of(lowerCaseDatabaseTrinoSchemaName.toUpperCase(ENGLISH)))) {
            List<String> columnNames = new ArrayList<>();
            while (columns.next()) {
                // Trino's default identifier mapping would uppercase the whole "database.schema"
                // string; the fix must resolve the *database* name back to its actual
                // quoted-lowercase form too, not just the schema, or the driver would silently
                // drop every row here (or reject the catalog outright) instead.
                assertThat(columns.getString("TABLE_CAT")).isEqualTo(lowerCaseDatabaseName);
                assertThat(columns.getString("TABLE_SCHEM")).isEqualTo(lowerCaseDatabaseSchemaName);
                columnNames.add(columns.getString("COLUMN_NAME"));
            }
            assertThat(columnNames).contains("ID", "NAME");
        }
    }

    @Test
    void testGetAllTableColumnsReturnsEmptyForNonexistentCatalog()
            throws SQLException
    {
        SnowflakeClient client = createClient();
        try (Connection connection = openRealConnection();
                ResultSet columns = client.getAllTableColumns(connection, Optional.of("nonexistent_database_" + randomNameSuffix() + "." + schemaName))) {
            assertThat(columns.next()).isFalse();
        }
    }

    private SnowflakeClient createClient()
    {
        return new SnowflakeClient(
                new BaseJdbcConfig(),
                new SnowflakeConfig().setDatabasePrefixForSchemaEnabled(true),
                new JdbcStatisticsConfig(),
                _ -> openRealConnection(),
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                InternalTypeManager.TESTING_TYPE_MANAGER,
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);
    }

    private static Connection openRealConnection()
            throws SQLException
    {
        Connection connection = SnowflakeServer.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("USE ROLE " + SnowflakeServer.ROLE);
            stmt.execute("USE WAREHOUSE " + SnowflakeServer.TEST_WAREHOUSE);
        }
        return connection;
    }
}
