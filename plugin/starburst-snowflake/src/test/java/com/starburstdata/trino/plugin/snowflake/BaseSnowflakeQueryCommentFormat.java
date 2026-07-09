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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.SNOWFLAKE_CATALOG;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

// TODO: rename to TestParallelSnowflakeQueryCommentFormat
public class BaseSnowflakeQueryCommentFormat
        extends AbstractTestQueryFramework
{
    private SnowflakeQueryRecorder snowflakeQueryRecorder;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestDatabase testDatabase = closeAfterClass(SnowflakeServer.createTestDatabase());
        snowflakeQueryRecorder = new SnowflakeQueryRecorder(testDatabase);
        return parallelBuilder()
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(impersonationDisabled())
                        .put("query.comment-format", "query_id=$QUERY_ID user=$USER")
                        .buildOrThrow())
                .withTpchTables(List.of(NATION))
                .build();
    }

    @Test
    public void testShouldLogContextInComment()
    {
        String tableName = SNOWFLAKE_CATALOG + "." + TEST_SCHEMA + "." + "log_comment_test_" + randomNameSuffix();
        String nationTable = SNOWFLAKE_CATALOG + "." + TEST_SCHEMA + ".nation";

        try {
            String createQueryId = executeWithQueryId(
                    "CREATE TABLE %s AS (SELECT * FROM %s)".formatted(tableName, nationTable));
            assertEventually(new Duration(60, SECONDS), () ->
                    assertThat(snowflakeQueryRecorder.fetchQueriesContaining(createQueryId))
                            .allMatch(query -> query.endsWith("/*query_id=" + createQueryId + " user=" + USER + "*/"))
                            .size()
                            .isGreaterThanOrEqualTo(1));

            String selectQueryId = executeWithQueryId("SELECT * FROM " + tableName);
            assertEventually(new Duration(60, SECONDS), () ->
                    assertThat(snowflakeQueryRecorder.fetchQueriesContaining(selectQueryId))
                            .allMatch(query -> query.endsWith("/*query_id=" + selectQueryId + " user=" + USER + "*/"))
                            .size()
                            .isEqualTo(1));

            String deleteQueryId = executeWithQueryId("DELETE FROM " + tableName + " WHERE nationkey = 1");
            assertEventually(new Duration(60, SECONDS), () ->
                    assertThat(snowflakeQueryRecorder.fetchQueriesContaining(deleteQueryId))
                            .allMatch(query -> query.endsWith("/*query_id=" + deleteQueryId + " user=" + USER + "*/"))
                            .size()
                            .isEqualTo(1));

            String insertQueryId = executeWithQueryId("INSERT INTO " + tableName + " VALUES (1, 'nation', 1, 'nation')");
            assertEventually(new Duration(60, SECONDS), () ->
                    assertThat(snowflakeQueryRecorder.fetchQueriesContaining(insertQueryId))
                            .allMatch(query -> query.endsWith("/*query_id=" + insertQueryId + " user=" + USER + "*/"))
                            .size()
                            .isGreaterThanOrEqualTo(1));
        }
        finally {
            String dropQueryId = executeWithQueryId("DROP TABLE IF EXISTS " + tableName);
            assertEventually(new Duration(60, SECONDS), () ->
                    assertThat(snowflakeQueryRecorder.fetchQueriesContaining(dropQueryId))
                            .allMatch(query -> query.endsWith("/*query_id=" + dropQueryId + " user=" + USER + "*/"))
                            .size()
                            .isEqualTo(1));
        }
    }

    @Test
    public void testShouldLogContextInCommentForTableFunctionsQueryPassthrough()
    {
        String passthroughQueryId = executeWithQueryId(
                "SELECT * FROM TABLE(system.query(query => 'SELECT name FROM " + TEST_SCHEMA + ".nation WHERE nationkey = 0'))");
        assertEventually(new Duration(60, SECONDS), () ->
                assertThat(snowflakeQueryRecorder.fetchQueriesContaining(passthroughQueryId))
                        .allMatch(query -> query.contains("SELECT name FROM " + TEST_SCHEMA + ".nation WHERE nationkey = 0"))
                        .allMatch(query -> query.endsWith("/*query_id=" + passthroughQueryId + " user=" + USER + "*/"))
                        .size()
                        .isEqualTo(1));
    }

    private String executeWithQueryId(String sql)
    {
        return getQueryRunner().executeWithQueryId(getSession(), sql).queryId().toString();
    }

    private record SnowflakeQueryRecorder(TestDatabase testDatabase)
    {
        private SnowflakeQueryRecorder(TestDatabase testDatabase)
        {
            this.testDatabase = requireNonNull(testDatabase, "testDatabase is null");
        }

        private List<String> fetchQueriesContaining(String queryId)
        {
            String sql = format(
                    "SELECT QUERY_TEXT FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(" +
                            "END_TIME_RANGE_START => DATEADD(hour, -1, CURRENT_TIMESTAMP()), " +
                            "RESULT_LIMIT => 1000)) " +
                            "WHERE QUERY_TEXT ILIKE '%%%s%%' " +
                            "AND QUERY_TEXT NOT ILIKE '%%INFORMATION_SCHEMA%%'",
                    queryId);

            ImmutableList.Builder<String> results = ImmutableList.builder();
            try {
                SnowflakeServer.executeOnDatabaseWithResultSetConsumer(testDatabase.getName(), rs -> {
                    try {
                        while (rs.next()) {
                            results.add(rs.getString("QUERY_TEXT"));
                        }
                    }
                    catch (SQLException e) {
                        throw new RuntimeException("Failed to read QUERY_TEXT from result set", e);
                    }
                }, sql);
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to query Snowflake query history", e);
            }
            return results.build();
        }
    }
}
