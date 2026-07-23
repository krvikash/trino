/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.sql.SqlExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class SynapseServer
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(SynapseServer.class);

    private static final String ENDPOINT = requireNonNull(System.getProperty("test.synapse.jdbc.endpoint"), "test.synapse.jdbc.endpoint is not set");
    static final String USERNAME = requireNonNull(System.getProperty("test.synapse.jdbc.user"), "test.synapse.jdbc.user is not set");
    static final String PASSWORD = requireNonNull(System.getProperty("test.synapse.jdbc.password"), "test.synapse.jdbc.password is not set");
    private static final String DATABASE = System.getProperty("test.synapse.jdbc.sqlpool", "SQLPOOL2");

    private static final String TEST_SUFFIX = System.getProperty("test.synapse.suffix", randomNameSuffix());
    static final String TEST_SCHEMA = "s_" + TEST_SUFFIX;
    private static final String TEST_USER = "u_" + TEST_SUFFIX;

    private static final String PORT = "1433";

    static final String JDBC_URL = "jdbc:sqlserver://" + ENDPOINT + ":" + PORT + ";database=" + DATABASE;

    private static final int ERROR_USER_EXISTS = 15023;
    private static final int ERROR_SCHEMA_EXISTS = 2714;

    private static final RetryPolicy<Object> INIT_CONNECTION_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(e ->
                    e.getMessage() != null &&
                            e.getMessage().contains("Failed to initialize pool: The connection is closed"))
            .withBackoff(Duration.ofSeconds(10), Duration.ofMinutes(5), 1.5)
            .withJitter(0.25)
            .withMaxRetries(5)
            .onRetry(event -> LOG.warn(
                    "Retrying SynapseServer initialization (attempt %d) due to: %s",
                    event.getAttemptCount(),
                    event.getLastException().getMessage()))
            .build();

    private final HikariDataSource dataSource;

    public SynapseServer()
    {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(JDBC_URL);
        hikariConfig.setUsername(USERNAME);
        hikariConfig.setPassword(PASSWORD);
        hikariConfig.setRegisterMbeans(false);
        hikariConfig.setMaxLifetime(MINUTES.toMillis(1));

        this.dataSource = Failsafe.with(INIT_CONNECTION_RETRY_POLICY).get(() -> new HikariDataSource(hikariConfig));

        executeAsOwner("CREATE SCHEMA " + TEST_SCHEMA, ERROR_SCHEMA_EXISTS);
        executeAsOwner(format("CREATE USER %s WITHOUT LOGIN WITH DEFAULT_SCHEMA = %s", TEST_USER, TEST_SCHEMA), ERROR_USER_EXISTS);
        executeAsOwner("GRANT ALTER TO " + TEST_USER, null);
        executeAsOwner(format("GRANT CONTROL ON SCHEMA :: %s TO %s", TEST_SCHEMA, TEST_USER), null);
    }

    public SqlExecutor getSqlExecutor()
    {
        return new SqlExecutor()
        {
            @Override
            public void execute(String sql)
            {
                SynapseServer.this.execute(sql);
            }

            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }
        };
    }

    public void execute(String query)
    {
        executeIgnoringErrors(query, /* ignoredErrorCode= */ null);
    }

    private void executeAsOwner(String query, Integer ignoredErrorCode)
    {
        executeIgnoringErrors(query, ignoredErrorCode, /* asUser= */ false);
    }

    public void executeIgnoringErrors(String query, Integer ignoredErrorCode)
    {
        executeIgnoringErrors(query, ignoredErrorCode, /* asUser= */ true);
    }

    private void executeIgnoringErrors(String query, Integer ignoredErrorCode, boolean asUser)
    {
        try (Connection conn = getConnection();
                Statement statement = conn.createStatement()) {
            if (asUser) {
                executeAsUser(statement);
            }
            statement.execute(query);
        }
        catch (SQLException e) {
            if (!(e instanceof SQLServerException sqlServerExn && Objects.equals(sqlServerExn.getErrorCode(), ignoredErrorCode))) {
                throw new RuntimeException("Failed to execute statement: " + query, e);
            }
            LOG.info("Ignoring expected error: %s", e);
        }
    }

    public <T> T executeQuery(String query, Function<ResultSet, T> resultConsumer)
    {
        LOG.debug("Executing query %s", query);
        try (Connection conn = getConnection();
                Statement statement = conn.createStatement()) {
            executeAsUser(statement);
            try (ResultSet resultSet = statement.executeQuery(query)) {
                return resultConsumer.apply(resultSet);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement: " + query, e);
        }
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection conn = dataSource.getConnection();
        try {
            try (Statement statement = conn.createStatement()) {
                // Revert any potential leftover EXECUTE AS USER context from a previous use of this pooled connection
                statement.execute("REVERT");
            }
        }
        catch (SQLException | RuntimeException e) {
            conn.close();
            throw e;
        }
        return conn;
    }

    private void executeAsUser(Statement statement)
            throws SQLException
    {
        statement.execute(format("EXECUTE AS USER = '%s'", TEST_USER));
    }

    @Override
    public void close()
    {
        dataSource.close();
    }
}
