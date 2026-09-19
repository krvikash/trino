/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce.testing.sql;

import io.airlift.log.Logger;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// Extension of TestTable to be able to create the table without a suffix and delete this test run's rows on close
public class SalesforceTestTable
        extends TestTable
{
    private static final Logger log = Logger.get(SalesforceTestTable.class);

    // Tables are shared by concurrent runs, so each row is tagged with its run id and a run only reads or deletes its own rows
    public static final String TEST_RUN_ID_COLUMN = "Name";
    public static final String TEST_RUN_ID = Optional.ofNullable(System.getenv("GITHUB_RUN_ID"))
            .map(runId -> "gha-" + runId + "-")
            .orElse("local-") + UUID.randomUUID();

    // Longer than the CI job timeout, so older rows are leftovers from cancelled runs
    private static final Duration STALE_ROWS_AGE = Duration.ofDays(1);

    private final String jdbcUrl;
    private final String tableName;

    public SalesforceTestTable(String jdbcUrl, String tableName, String tableDefinition)
    {
        // Pass in a no-op SqlExecutor as the base class attempts to create the table, but we want to override that functionality here
        super(_ -> {}, tableName, tableDefinition);

        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        JdbcSqlExecutor sqlExecutor = new JdbcSqlExecutor(jdbcUrl);

        // Salesforce has limits on custom objects and tables are soft-deleted and then actually deleted weeks later
        // We've had issues with CI builds where we fill up the quota and then the builds fail
        // Instead we will create the table if it doesn't exist and delete this test run's rows
        // We can't use CREATE TABLE IF NOT EXISTS, an error is thrown because the table exists with a __c suffix and the
        // driver will try to create it anyway
        // Use DatabaseMetaData.getTables() for existence check — sys_tables can return 0 for existing tables,
        // causing the JDBC driver to silently delete and recreate the object.
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                ResultSet results = connection.getMetaData().getTables(null, null, tableName + "__c", null)) {
            boolean exists = results.next();

            if (!exists) {
                log.info("Table %s does not exist, creating via JDBC with definition '%s'", tableName, tableDefinition);
                sqlExecutor.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
                sqlExecutor.execute("RESET SCHEMA CACHE");
            }
            else {
                log.info("Table %s exists, deleting rows of test run %s", tableName, TEST_RUN_ID);
                deleteTestRunRows();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error checking if table exists: " + tableName, e);
        }
    }

    @Override
    public String getName()
    {
        // Override to return table name without random suffix
        return tableName;
    }

    @Override
    public void close()
    {
        // Don't drop the table to avoid hitting object limits in Salesforce.
        // The constructor deletes this run's rows again, so a failed cleanup here must not fail a passing test
        try {
            deleteTestRunRows();
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to delete rows of test run %s from table %s", TEST_RUN_ID, tableName);
        }
    }

    private void deleteTestRunRows()
    {
        deleteRows(format("%s = ?", TEST_RUN_ID_COLUMN), TEST_RUN_ID);
        verifyNoTestRunRows();

        // A concurrent run may delete the same stale rows, so failing here must not fail this run
        try {
            deleteRows("CreatedDate < ?", Timestamp.from(Instant.now().minus(STALE_ROWS_AGE)));
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to delete stale rows from table %s, a concurrent run may have deleted them", tableName);
        }
    }

    private void deleteRows(String predicate, Object parameter)
    {
        // Salesforce driver does not support SQL TRUNCATE
        // Instead we delete records by querying for the 'Id' column of the table (a Salesforce column) and then inserting
        // into a temp table which is stored in the driver. We then delete from the main table by selecting from the temp table
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                PreparedStatement selectIds = connection.prepareStatement(format("SELECT Id FROM %s__c WHERE %s", tableName, predicate));
                PreparedStatement insertTempIds = connection.prepareStatement(format("INSERT INTO %s__c#TEMP (Id) VALUES (?)", tableName))) {
            selectIds.setObject(1, parameter);
            boolean hasData = false;
            try (ResultSet results = selectIds.executeQuery()) {
                while (results.next()) {
                    hasData = true;
                    insertTempIds.setObject(1, results.getObject(1));
                    insertTempIds.execute();
                }
            }

            if (hasData) {
                try (PreparedStatement delete = connection.prepareStatement(format("DELETE FROM %s__c WHERE EXISTS SELECT Id FROM %s__c#TEMP", tableName, tableName))) {
                    delete.execute();
                }
                log.info("Deleted rows matching '%s' from table %s", predicate, tableName);
            }
            else {
                log.info("Table %s had no rows matching '%s'", tableName, predicate);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(format("Error deleting rows matching '%s' from table %s", predicate, tableName), e);
        }
    }

    private void verifyNoTestRunRows()
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                PreparedStatement countRows = connection.prepareStatement(format("SELECT COUNT(*) FROM %s__c WHERE %s = ?", tableName, TEST_RUN_ID_COLUMN))) {
            countRows.setString(1, TEST_RUN_ID);
            try (ResultSet countResults = countRows.executeQuery()) {
                countResults.next();
                int numRows = countResults.getInt(1);
                if (numRows != 0) {
                    throw new RuntimeException(format("Table %s has %s rows of test run %s but expected 0", tableName, numRows, TEST_RUN_ID));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error counting rows of test run " + TEST_RUN_ID + " in table " + tableName, e);
        }
    }
}
