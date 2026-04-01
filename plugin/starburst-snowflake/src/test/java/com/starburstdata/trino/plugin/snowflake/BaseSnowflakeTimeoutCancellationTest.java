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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test implementations verifying that Snowflake queries are cleaned up if the Trino query times out or is canceled.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public abstract class BaseSnowflakeTimeoutCancellationTest
        extends AbstractTestQueryFramework
{
    protected TestDatabase testDatabase;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testDatabase = closeAfterClass(SnowflakeServer.createTestDatabase());

        return getSnowflakeQueryRunnerBuilder()
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .build();
    }

    protected abstract SnowflakeQueryRunner.Builder<?> getSnowflakeQueryRunnerBuilder();

    @Test
    @Timeout(60)
    public void testTrinoCancelCancelsSnowflakeQueries()
            throws Exception
    {
        testSnowflakeQueriesCanceled(
                "test_trino_cancel",
                getQueryRunner().getDefaultSession(),
                ".*Query killed. Message: Canceled by test.*",
                queryId -> assertUpdate(format("CALL system.runtime.kill_query(query_id => '%s', message => 'Canceled by test')", queryId.id())));
    }

    @Test
    @Timeout(60)
    public void testTrinoTimeoutCancelsSnowflakeQueries()
            throws Exception
    {
        Session sessionWithTimeout = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("query_max_execution_time", "5s")
                .build();
        testSnowflakeQueriesCanceled(
                "test_trino_timeout",
                sessionWithTimeout,
                ".*Query exceeded the maximum execution time limit.*",
                _ -> {
                    // Wait for the timeout to expire (no explicit cancel)
                });
    }

    private void testSnowflakeQueriesCanceled(
            String name,
            Session session,
            String expectedErrorPattern,
            Consumer<QueryId> doCancel)
            throws Exception
    {
        String uniqueMarker = name + "_" + randomNameSuffix();
        QueryRunner queryRunner = getQueryRunner();

        try (ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(
                format("BaseSnowflakeTimeoutCancellationTest-%s-%%s", name)))) {
            CompletionService<?> completionService = new ExecutorCompletionService<>(executor);

            String systemQuery = format("SELECT COUNT(*) AS %s FROM TABLE(GENERATOR(TIMELIMIT => 120))", uniqueMarker);
            String query = format("SELECT * FROM TABLE(system.query(query => '%s'))", systemQuery);

            completionService.submit(() -> {
                assertQueryFails(session, query, expectedErrorPattern);
                return null;
            });
            QueryId trinoQueryId = BaseJdbcConnectorTest.getQueryId(queryRunner, query);

            ImmutableList<String> snowflakeQueryIds = waitForSnowflakeQueries(uniqueMarker);

            doCancel.accept(trinoQueryId);
            completionService.take().get();

            assertSnowflakeQueriesCanceled(snowflakeQueryIds);
        }
    }

    private ImmutableList<String> waitForSnowflakeQueries(String uniqueMarker)
            throws InterruptedException
    {
        for (int i = 0; i < 100; i++) {
            ImmutableList<String> queryIds = getSnowflakeQueryHistory("QUERY_ID", format("QUERY_TEXT ILIKE '%%%s%%'", uniqueMarker))
                    .stream()
                    .flatMap(Optional::stream)
                    .collect(toImmutableList());
            if (!queryIds.isEmpty()) {
                return queryIds;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Snowflake queries not found for marker: " + uniqueMarker);
    }

    private void assertSnowflakeQueriesCanceled(Collection<String> queryIds)
    {
        String quotedIds = queryIds.stream()
                .map(id -> "'" + id + "'")
                .collect(Collectors.joining(", "));
        String condition = format("QUERY_ID IN (%s) AND EXECUTION_STATUS", quotedIds);
        assertEventually(() -> {
            // Sometimes, the canceled query is fully removed from the query history table, so we can't check
            // that there are a specific number of failures; instead, we check that all the queries that *are*
            // in the table were canceled. No error message => successful or still running query
            ImmutableList<Optional<String>> errors = getSnowflakeQueryHistory("ERROR_MESSAGE", condition);
            assertThat(errors).allMatch(e -> e.isPresent() && e.get().contains("canceled"));
        });
    }

    private ImmutableList<Optional<String>> getSnowflakeQueryHistory(String columnName, String condition)
    {
        String sql = format(
                "SELECT %s FROM TABLE(information_schema.query_history()) " +
                        // Don't include history queries in the results
                        "WHERE %s AND NOT QUERY_TEXT ILIKE '%%information_schema.query_history%%'",
                columnName, condition);

        ImmutableList.Builder<Optional<String>> results = ImmutableList.builder();
        try {
            SnowflakeServer.executeOnDatabaseWithResultSetConsumer(testDatabase.getName(), rs -> {
                try {
                    while (rs.next()) {
                        results.add(Optional.ofNullable(rs.getString(columnName)));
                    }
                }
                catch (SQLException e) {
                    throw new RuntimeException("Failed to read column from result set", e);
                }
            }, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to query Snowflake query history", e);
        }

        return results.build();
    }
}
