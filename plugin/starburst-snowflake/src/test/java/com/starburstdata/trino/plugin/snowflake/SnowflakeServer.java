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

import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SnowflakeServer
{
    private static final Logger LOG = Logger.get(SnowflakeServer.class);

    public static final String ROLE = requireNonNull(System.getProperty("snowflake.test.server.role"), "snowflake.test.server.role is not set");

    public static final String JDBC_URL = requireNonNull(System.getProperty("snowflake.test.server.url"), "snowflake.test.server.url is not set");
    public static final String USER = requireNonNull(System.getProperty("snowflake.test.server.user"), "snowflake.test.server.user is not set");
    public static final String PASSWORD = requireNonNull(System.getProperty("snowflake.test.server.password"), "snowflake.test.server.password is not set");
    public static final String PRIVATE_KEY = requireNonNull(System.getProperty("snowflake.test.server.private-key"), "snowflake.test.server.private-key is not set");
    public static final String PRIVATE_KEY_2 = requireNonNull(System.getProperty("snowflake.test.server.private-key-2"), "snowflake.test.server.private-key-2 is not set");
    public static final String PRIVATE_KEY_2_PASSPHRASE = requireNonNull(System.getProperty("snowflake.test.server.private-key-2-passphrase"), "snowflake.test.server.private-key-2-passphrase is not set");
    public static final String TEST_WAREHOUSE = requireNonNull(System.getProperty("snowflake.test.warehouse"), "snowflake.test.warehouse is not set");
    public static final String TEST_DATABASE = "TEST_DB";

    private SnowflakeServer() {}

    static {
        LOG.info("Using %s", JDBC_URL);

        // make sure Snowflake is accessible
        try {
            execute("SELECT 1");
        }
        catch (SQLException e) {
            throw new RuntimeException("Snowflake is not accessible", e);
        }
    }

    public static TestDatabase createDatabase(String databaseSuffix)
    {
        return new TestDatabase(SnowflakeServer::safeExecute, databaseSuffix);
    }

    public static TestDatabase createTestDatabase()
    {
        return createDatabase("TEST");
    }

    public static void createSchema(String databaseName, String schemaName)
            throws SQLException
    {
        executeOnDatabase(databaseName, format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
    }

    static void execute(String... sqls)
            throws SQLException
    {
        executeOnDatabase(TEST_DATABASE, sqls);
    }

    public static void executeOnDatabase(String database, String... sqls)
            throws SQLException
    {
        executeOnDatabaseWithResultSetConsumer(database, _ -> {}, sqls);
    }

    public static void executeOnDatabaseWithResultSetConsumer(String database, Consumer<ResultSet> consumer, String... sqls)
            throws SQLException
    {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            LOG.info("Using role: %s, warehouse: %s, database: %s", ROLE, TEST_WAREHOUSE, database);
            stmt.execute(format("USE ROLE %s", ROLE));
            stmt.execute(format("USE WAREHOUSE %s", TEST_WAREHOUSE));
            stmt.execute(format("USE DATABASE %s", database));

            for (String sql : sqls) {
                LOG.info("Executing [%s]: %s", USER, sql);
                stmt.execute(sql);
            }
            consumer.accept(stmt.getResultSet());
        }
    }

    private static void safeExecute(String sql)
    {
        try {
            execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void safeExecuteOnDatabase(String database, String... sqls)
    {
        try {
            executeOnDatabase(database, sqls);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection()
            throws SQLException
    {
        return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
    }
}
