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
import com.google.common.io.Closer;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingSession;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Strings.repeat;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
public abstract class BaseSnowflakeConnectorTest
        // Using BaseJdbcConnectorTest as a base class is not strictly accurate as we have to flavours of Snowflake connector: jdbc and distributed.
        // Still most of the extra testcases defined in BaseJdbcConnectorTest are applicable to both.
        extends BaseJdbcConnectorTest
{
    protected final Closer closer = Closer.create();
    protected final TestDatabase testDatabase = closer.register(SnowflakeServer.createTestDatabase());
    protected final SqlExecutor snowflakeExecutor = (sql) -> SnowflakeServer.safeExecuteOnDatabase(testDatabase.getName(), sql);

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE,
                 SUPPORTS_SET_COLUMN_TYPE -> false;
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    void testMergeWithMixedCasePrimaryKeys()
    {
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_merge_pk_different_cases_" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TABLE " + schema + "." + tableName + " (x int, \"pK\" int NOT NULL, CONSTRAINT pk_" + tableName + " PRIMARY KEY (\"pK\"))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2)", 2);

        assertUpdate("DELETE FROM " + tableName + " WHERE PK = 1", 1);
        assertThat(query("SELECT CAST(x as integer) FROM " + schema + "." + tableName))
                .matches("VALUES 2");

        assertUpdate("UPDATE " + tableName + " SET x = 100 WHERE pk = 2", 1);
        assertThat(query("SELECT CAST(x as integer) FROM " + schema + "." + tableName))
                .matches("VALUES 100");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateTableWithDifferentCaseColumnsFails()
    {
        String tableName = "test_mixed_case_columns_" + randomNameSuffix();
        String schema = getSession().getSchema().orElseThrow();

        boolean created = false;
        try {
            onRemoteDatabase().execute("CREATE TABLE " + schema + "." + tableName + "(col int, \"COL\" int)");
            created = true;
        }
        catch (Exception e) {
            assertThat(e).hasMessageContaining("duplicate column name 'COL'");
        }

        assertThat(created).isFalse();

        assertQueryFails("CREATE table " + tableName + "(col int, \"COL\" int)", "line 1:58: Column name '\"COL\"' specified more than once");
        assertQueryFails("CREATE table " + tableName + "(col int, \"Col\" int)", "line 1:58: Column name '\"Col\"' specified more than once");

        // success creates on remote but not able to read and write in Trino
        onRemoteDatabase().execute("CREATE TABLE " + schema + "." + tableName + "(col int, \"Col\" int)");
        assertThat(query("SELECT * FROM " + tableName))
                .nonTrinoExceptionFailure()
                .hasMessageContaining("Multiple entries with same key");
        assertThat(query("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2)"))
                .nonTrinoExceptionFailure()
                .hasMessageContaining("Multiple entries with same key");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // Snowflake does not have a CHAR type. They map it to varchar, which does not have the same fixed width semantics
        assertThatThrownBy(super::testCharVarcharComparison)
                .isInstanceOf(AssertionError.class);

        // Also assert that CHAR columns end up as VARCHAR in Snowflake
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_char_is_varchar_",
                "AS SELECT CHAR 'is_actually_a_varchar' AS a_char")) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue())
                    .matches("""
                            CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(
                               a_char varchar(21)
                            )""");
        }
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        // Snowflake does not support column names containing double quotes
        return columnName.contains("\"") && nullToEmpty(exception.getMessage()).matches(".*(Snowflake columns cannot contain quotes).*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        // Real: Snowflake does not have a REAL type, instead they are mapped to double. The round trip test fails because REAL '567.123' != DOUBLE '567.123'
        // Char: Snowflake does not have a CHAR type. They map it to varchar, which does not have the same fixed width semantics
        String name = dataMappingTestSetup.getTrinoTypeName();
        if (name.equals("real") || name.startsWith("char")) {
            return Optional.empty();
        }

        if (name.equals("time(6)")
                || name.equals("timestamp(6)")
                || name.equals("timestamp(6) with time zone")) {
            // TODO https://starburstdata.atlassian.net/browse/SEP-9302
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                (sql) -> SnowflakeServer.safeExecuteOnDatabase(testDatabase.getName(), sql),
                format("%s.test_table_with_default_columns", TEST_SCHEMA),
                """
                        (col_required BIGINT NOT NULL,
                        col_nullable BIGINT,
                        col_default BIGINT DEFAULT 43,
                        col_nonnull_default BIGINT NOT NULL DEFAULT 42,
                        col_required2 BIGINT NOT NULL)""");
    }

    @Override
    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertThat(actual).containsExactlyElementsOf(expectedParametrizedVarchar);
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT ? FROM nation WHERE nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "decimal(19,0)")
                .row(2, "varchar(25)")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' AND table_schema = 'test_schema_2' LIMIT 1",
                "SELECT 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_schema = 'test_schema_2' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer'");
    }

    @Override
    @Test
    public void testTableSampleBernoulli()
    {
        abort("This test takes more than 10 minutes to finish.");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual(
                getSession(), "DESC ORDERS").toTestTypes();

        MaterializedResult expectedColumns = resultBuilder(
                getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertThat(actualColumns).containsExactlyElementsOf(expectedColumns);
    }

    @Test
    public void testViews()
            throws SQLException
    {
        String viewName = "test_view_" + randomNameSuffix();
        SnowflakeServer.executeOnDatabase(testDatabase.getName(), format("CREATE VIEW %s.%s AS SELECT * FROM orders", TEST_SCHEMA, viewName));
        assertThat(getQueryRunner().tableExists(getSession(), viewName)).isTrue();
        assertQuery(format("SELECT orderkey FROM %s", viewName), "SELECT orderkey FROM orders");
        SnowflakeServer.executeOnDatabase(testDatabase.getName(), format("DROP VIEW %s.%s", TEST_SCHEMA, viewName));
    }

    @Test
    public void testPredicatePushdownForNumerics()
    {
        String tableName = TEST_SCHEMA + ".test_predicate_pushdown_numeric";
        try (TestTable testTable = new TestTable(
                sql -> SnowflakeServer.safeExecuteOnDatabase(testDatabase.getName(), sql),
                tableName,
                "(c_binary_float FLOAT, c_binary_double DOUBLE, c_number NUMBER(5,3))",
                ImmutableList.of("5.0, 20.233, 5.0"))) {
            // this expects the unqualified table name as an argument
            assertThat(getQueryRunner().tableExists(getSession(), testTable.getName().substring(TEST_SCHEMA.length() + 1))).isTrue();
            assertQuery(format("SELECT c_binary_double FROM %s WHERE c_binary_float = cast(5.0 as real)", testTable.getName()), "SELECT 20.233");
            assertQuery(format("SELECT c_binary_float FROM %s WHERE c_binary_double = cast(20.233 as double)", testTable.getName()), "SELECT 5.0");
            assertQuery(format("SELECT c_binary_float FROM %s WHERE c_number = cast(5.0 as decimal(5,3))", testTable.getName()), "SELECT 5.0");
        }
    }

    @Test
    public void testPredicatePushdownForChars()
    {
        String tableName = TEST_SCHEMA + ".test_predicate_pushdown_char";
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(c_char CHAR(7), c_varchar VARCHAR(20), c_long_char CHAR(2000), c_long_varchar VARCHAR(4000))",
                ImmutableList.of("'my_char', 'my_varchar', 'my_long_char', 'my_long_varchar'"))) {
            // this expects the unqualified table name as an argument
            assertThat(getQueryRunner().tableExists(getSession(), testTable.getName().substring(TEST_SCHEMA.length() + 1))).isTrue();
            assertQuery(format("SELECT c_char FROM %s WHERE c_varchar = cast('my_varchar' as varchar(20))", testTable.getName()), "SELECT 'my_char'");
            assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", testTable.getName()));
            assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", testTable.getName()));
        }
    }

    @Test
    public void testTooLargeDomainCompactionThreshold()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setCatalogSessionProperty("snowflake", "domain_compaction_threshold", "10000")
                        .build(),
                "SELECT * from nation", "Domain compaction threshold \\(10000\\) cannot exceed 1000");
    }

    @Test
    @Override
    public void testSelectInformationSchemaTables()
    {
        String schema = getSession().getSchema().get();
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String schema = getSession().getSchema().get();
        String ordersTableWithColumns = """
                VALUES
                ('orders', 'orderkey'),
                ('orders', 'custkey'),
                ('orders', 'orderstatus'),
                ('orders', 'totalprice'),
                ('orders', 'orderdate'),
                ('orders', 'orderpriority'),
                ('orders', 'clerk'),
                ('orders', 'shippriority'),
                ('orders', 'comment')""";

        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
    }

    @Test
    public void testTimeRounding()
    {
        String tableName = TEST_SCHEMA + ".test_time";
        for (ZoneId sessionZone : ImmutableList.of(ZoneOffset.UTC, ZoneId.systemDefault(), ZoneId.of("Europe/Vilnius"), ZoneId.of("Asia/Kathmandu"), ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();
            try (TestTable testTable = new TestTable(
                    snowflakeExecutor,
                    tableName,
                    "(x TIME)")) {
                assertUpdate(session, format("INSERT INTO %s VALUES (TIME '12:34:56.123')", testTable.getName()), 1);
                assertQuery(session, format("SELECT * FROM %s WHERE rand() > 42 OR x = TIME '12:34:56.123'", testTable.getName()), "SELECT '12:34:56.123' x");
            }
        }
    }

    @Test
    public void testCaseSensitiveColumnNames()
    {
        String tableName = TEST_SCHEMA + ".test_case_sensitive_column_names_";
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(id varchar, \"lowercase\" varchar, \"UPPERCASE\" varchar, \"MixedCase\" varchar)",
                ImmutableList.of("'lowercase', 'lowercase value', NULL, NULL", "'uppercase', NULL, 'uppercase value', NULL", "'mixedcase', NULL, NULL, 'mixedcase value'"))) {
            assertQuery(
                    "SELECT id, mixedcase, uppercase, lowercase FROM " + testTable.getName(),
                    "VALUES " +
                            " ('lowercase', NULL, NULL, 'lowercase value'), " +
                            " ('uppercase', NULL, 'uppercase value', NULL), " +
                            " ('mixedcase', 'mixedcase value', NULL, NULL)");
        }
    }

    @Test
    public void testTimestampWithTimezoneValues()
    {
        testTimestampWithTimezoneValues(true);
    }

    protected void testTimestampWithTimezoneValues(boolean includeNegativeYear)
    {
        String tableName = TEST_SCHEMA + ".test_tstz_";
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneOffset.UTC.getId()))
                .build();

        // Snowflake literals cannot have a 5-digit year, nor a negative year, so need to use DATEADD for some values
        ImmutableList.Builder<String> data = ImmutableList.<String>builder()
                .add("TO_TIMESTAMP_TZ('1970-01-01T00:00:00.000 +14:00')")
                .add("TO_TIMESTAMP_TZ('1970-01-01T00:00:00.000 -13:00')")
                .add("TO_TIMESTAMP_TZ('0001-01-01T00:00:00.000Z')")
                .add("DATEADD(YEAR, 2, TO_TIMESTAMP_TZ('9999-12-31T23:59:59.999Z'))")
                .add("DATEADD(YEAR, 70000, TO_TIMESTAMP_TZ('3326-09-11T20:14:45.247Z'))")
                .add("DATEADD(YEAR, 70000, TO_TIMESTAMP_TZ('3326-09-11T07:14:45.247 -13:00'))");

        if (includeNegativeYear) {
            data
                    .add("DATEADD(YEAR, -2, TO_TIMESTAMP_TZ('0001-01-01T00:00:00.000Z'))")
                    .add("DATEADD(YEAR, -70000, TO_TIMESTAMP_TZ('613-04-22T03:45:14.753Z'))")
                    .add("DATEADD(YEAR, -70000, TO_TIMESTAMP_TZ('613-04-22T17:45:14.753 +14:00'))");
        }

        MaterializedResult.Builder expected = resultBuilder(session, createTimestampWithTimeZoneType(3))
                .row(LocalDateTime.of(1970, 1, 1, 0, 0).atZone(ZoneOffset.ofHoursMinutes(14, 0)))
                .row(LocalDateTime.of(1970, 1, 1, 0, 0).atZone(ZoneOffset.ofHoursMinutes(-13, 0)))
                .row(LocalDateTime.of(9999 + 2, 12, 31, 23, 59, 59, 999_000_000).atZone(ZoneId.of("UTC")))
                .row(LocalDateTime.of(1, 1, 1, 0, 0, 0, 0).atZone(ZoneId.of("UTC")))
                // 73326-09-11T20:14:45.247Z[UTC] is the timestamp with tz farthest in the future Presto can represent (for UTC)
                .row(LocalDateTime.of(3326 + 70000, 9, 11, 20, 14, 45, 247_000_000).atZone(ZoneId.of("UTC")))
                // same instant as above for the negative offset with highest absolute value Snowflake allows
                .row(LocalDateTime.of(3326 + 70000, 9, 11, 7, 14, 45, 247_000_000).atZone(ZoneOffset.ofHoursMinutes(-13, 0)));

        if (includeNegativeYear) {
            expected
                    .row(LocalDateTime.of(-1, 1, 1, 0, 0, 0, 0).atZone(ZoneId.of("UTC")))
                    // -69387-04-22T03:45:14.753Z[UTC] is the timestamp with tz farthest in the past Presto can represent
                    .row(LocalDateTime.of(613 - 70000, 4, 22, 3, 45, 14, 753_000_000).atZone(ZoneId.of("UTC")))
                    // same instant as above for the max offset Snowflake allows
                    .row(LocalDateTime.of(613 - 70000, 4, 22, 17, 45, 14, 753_000_000).atZone(ZoneOffset.ofHoursMinutes(14, 0)));
        }

        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(a TIMESTAMP_TZ)",
                data.build())) {
            MaterializedResult actual = computeActual(session, "SELECT a FROM " + testTable.getName()).toTestTypes();

            assertEqualsIgnoreOrder(actual, expected.build());
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("""
                        CREATE TABLE \\w+\\.\\w+\\.orders \\Q(
                           orderkey decimal(19, 0),
                           custkey decimal(19, 0),
                           orderstatus varchar(1),
                           totalprice double,
                           orderdate date,
                           orderpriority varchar(15),
                           clerk varchar(15),
                           shippriority decimal(10, 0),
                           comment varchar(79)
                        )""");
    }

    @Test
    public void testPredicatePushdown()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor, getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown",
                """
                        (
                        bigint_column bigint,
                        short_decimal decimal(9, 3),
                        long_decimal decimal(30, 10),
                        varchar_column varchar(10))""")) {
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (100, 100.000, 100000000.000000000, 'ala')");
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (123, 123.321, 123456789.987654321, 'kot')");
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column = 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column > 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal = 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal > 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal > 100000000")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal = 100000000")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE varchar_column > 'ala'")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE varchar_column = 'ala'")).isFullyPushedDown();

            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column > 100 and varchar_column > 'ala'")).isFullyPushedDown();
        }
    }

    @Test
    public void testTimestampWithTimeZoneAggregationPushdown()
    {
        try (TestTable testTable = new TestTable(onRemoteDatabase(), getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown_timestamp_tz",
                "(timestamp_tz_column timestamp with time zone)")) {
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (TIMESTAMP '1901-02-03 04:05:06.789')");
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (TIMESTAMP '1911-02-03 04:05:06.789')");

            // Adding specific testcase for TIMESTAMP WITH TIME ZONE as it requires special rewrite handling when sending query to SF.
            assertThat(query("SELECT min(timestamp_tz_column) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(timestamp_tz_column) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        // Overridden because for approx_set(bigint) a ProjectNode is present above table scan because Snowflake requires a coercion from bigint to number
        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // multimap_agg doesn't have a deterministic order of values for a key
                .isNotFullyPushedDown(AggregationNode.class);
        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Test
    public void testSnowflakeTimestampWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor, getSession().getSchema().orElseThrow() + ".test_timestamp_with_precision",
                "(" +
                        "timestamp0 timestamp(0)," +
                        "timestamp1 timestamp(1)," +
                        "timestamp2 timestamp(2)," +
                        "timestamp3 timestamp(3)," +
                        "timestamp4 timestamp(4)," +
                        "timestamp5 timestamp(5)," +
                        "timestamp6 timestamp(6)," +
                        "timestamp7 timestamp(7)," +
                        "timestamp8 timestamp(8)," +
                        "timestamp9 timestamp(9))",
                ImmutableList.of("""
                        TIMESTAMP '1901-02-03 04:05:06',
                        TIMESTAMP '1901-02-03 04:05:06.1',
                        TIMESTAMP '1901-02-03 04:05:06.12',
                        TIMESTAMP '1901-02-03 04:05:06.123',
                        TIMESTAMP '1901-02-03 04:05:06.1234',
                        TIMESTAMP '1901-02-03 04:05:06.12345',
                        TIMESTAMP '1901-02-03 04:05:06.123456',
                        TIMESTAMP '1901-02-03 04:05:06.1234567',
                        TIMESTAMP '1901-02-03 04:05:06.12345678',
                        TIMESTAMP '1901-02-03 04:05:06.123456789'"""))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("""
                            CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(
                               timestamp0 timestamp(0),
                               timestamp1 timestamp(1),
                               timestamp2 timestamp(2),
                               timestamp3 timestamp(3),
                               timestamp4 timestamp(4),
                               timestamp5 timestamp(5),
                               timestamp6 timestamp(6),
                               timestamp7 timestamp(7),
                               timestamp8 timestamp(8),
                               timestamp9 timestamp(9)
                            )""");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("""
                            VALUES (
                            TIMESTAMP '1901-02-03 04:05:06',
                            TIMESTAMP '1901-02-03 04:05:06.1',
                            TIMESTAMP '1901-02-03 04:05:06.12',
                            TIMESTAMP '1901-02-03 04:05:06.123',
                            TIMESTAMP '1901-02-03 04:05:06.1234',
                            TIMESTAMP '1901-02-03 04:05:06.12345',
                            TIMESTAMP '1901-02-03 04:05:06.123456',
                            TIMESTAMP '1901-02-03 04:05:06.1234567',
                            TIMESTAMP '1901-02-03 04:05:06.12345678',
                            TIMESTAMP '1901-02-03 04:05:06.123456789')""");
        }
    }

    @Test
    public void testSnowflakeTimestampWithTimeZoneWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor, getSession().getSchema().orElseThrow() + ".test_timestamptz_with_precision",
                """
                        (
                        timestamptz0 timestamp_tz(0),
                        timestamptz1 timestamp_tz(1),
                        timestamptz2 timestamp_tz(2),
                        timestamptz3 timestamp_tz(3),
                        timestamptz4 timestamp_tz(4),
                        timestamptz5 timestamp_tz(5),
                        timestamptz6 timestamp_tz(6),
                        timestamptz7 timestamp_tz(7),
                        timestamptz8 timestamp_tz(8),
                        timestamptz9 timestamp_tz(9))""",
                ImmutableList.of("""
                        '1901-02-03 04:05:06 +02:00',
                        '1901-02-03 04:05:06.1 +02:00',
                        '1901-02-03 04:05:06.12 +02:00',
                        '1901-02-03 04:05:06.123 +02:00',
                        '1901-02-03 04:05:06.1234 +02:00',
                        '1901-02-03 04:05:06.12345 +02:00',
                        '1901-02-03 04:05:06.123456 +02:00',
                        '1901-02-03 04:05:06.1234567 +02:00',
                        '1901-02-03 04:05:06.12345678 +02:00',
                        '1901-02-03 04:05:06.123456789 +02:00'"""))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("""
                            CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(
                               timestamptz0 timestamp(0) with time zone,
                               timestamptz1 timestamp(1) with time zone,
                               timestamptz2 timestamp(2) with time zone,
                               timestamptz3 timestamp(3) with time zone,
                               timestamptz4 timestamp(4) with time zone,
                               timestamptz5 timestamp(5) with time zone,
                               timestamptz6 timestamp(6) with time zone,
                               timestamptz7 timestamp(7) with time zone,
                               timestamptz8 timestamp(8) with time zone,
                               timestamptz9 timestamp(9) with time zone
                            )""");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("""
                            VALUES (
                            TIMESTAMP '1901-02-03 04:05:06 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.1 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.12 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.123 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.1234 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.12345 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.123456 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.1234567 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.12345678 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.123456789 +02:00')""");
        }
    }

    @Test
    public void testSnowflakeTimeWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor, getSession().getSchema().orElseThrow() + ".test_time_with_precision",
                """
                        (
                        time0 time(0),
                        time1 time(1),
                        time2 time(2),
                        time3 time(3),
                        time4 time(4),
                        time5 time(5),
                        time6 time(6),
                        time7 time(7),
                        time8 time(8),
                        time9 time(9))""",
                ImmutableList.of("""
                        TIME '04:05:06',
                        TIME '04:05:06.1',
                        TIME '04:05:06.12',
                        TIME '04:05:06.123',
                        TIME '04:05:06.1234',
                        TIME '04:05:06.12345',
                        TIME '04:05:06.123456',
                        TIME '04:05:06.1234567',
                        TIME '04:05:06.12345678',
                        TIME '04:05:06.123456789'"""))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("""
                            CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(
                               time0 time(0),
                               time1 time(1),
                               time2 time(2),
                               time3 time(3),
                               time4 time(4),
                               time5 time(5),
                               time6 time(6),
                               time7 time(7),
                               time8 time(8),
                               time9 time(9)
                            )""");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (" +
                            "TIME '04:05:06'," +
                            "TIME '04:05:06.1'," +
                            "TIME '04:05:06.12'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.1234'," +
                            "TIME '04:05:06.12345'," +
                            "TIME '04:05:06.123456'," +
                            "TIME '04:05:06.1234567'," +
                            "TIME '04:05:06.12345678'," +
                            "TIME '04:05:06.123456789')");
        }
    }

    @Test
    public void testSnowflakeTimestampRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor, getSession().getSchema().orElseThrow() + ".test_timestamp_rounding",
                "(t timestamp(9))",
                ImmutableList.of(
                        "TIMESTAMP '1901-02-03 04:05:06.123499999'",
                        "TIMESTAMP '1901-02-03 04:05:06.123900000'",
                        "TIMESTAMP '1969-12-31 23:59:59.999999999'",
                        "TIMESTAMP '2001-02-03 04:05:06.123499999'",
                        "TIMESTAMP '2001-02-03 04:05:06.123900000'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("""
                            VALUES
                            TIMESTAMP '1901-02-03 04:05:06.123499999',
                            TIMESTAMP '1901-02-03 04:05:06.123900000',
                            TIMESTAMP '1969-12-31 23:59:59.999999999',
                            TIMESTAMP '2001-02-03 04:05:06.123499999',
                            TIMESTAMP '2001-02-03 04:05:06.123900000'""");
        }
    }

    @Test
    public void testSnowflakeTimestampWithTimeZoneRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor, getSession().getSchema().orElseThrow() + ".test_timestamptz_rounding",
                "(t timestamp_tz(9))",
                ImmutableList.of(
                        "'1901-02-03 04:05:06.123499999 +02:00'",
                        "'1901-02-03 04:05:06.123900000 +02:00'",
                        "'2001-02-03 04:05:06.123499999 +02:00'",
                        "'2001-02-03 04:05:06.123900000 +02:00'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("""
                            VALUES
                            TIMESTAMP '1901-02-03 04:05:06.123499999 +02:00',
                            TIMESTAMP '1901-02-03 04:05:06.123900000 +02:00',
                            TIMESTAMP '2001-02-03 04:05:06.123499999 +02:00',
                            TIMESTAMP '2001-02-03 04:05:06.123900000 +02:00'""");
        }
    }


    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return ".* failed on column NOT_NULL_COL with error: NULL result in a non-nullable column";
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return snowflakeExecutor;
    }

    @Override
    @Test
    public void testCreateTableWithLongColumnName()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9733");
    }

    @Override
    @Test
    public void testAlterTableAddLongColumnName()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9733");
    }

    @Override
    @Test
    public void testAlterTableRenameColumnToLongName()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9733");
    }

    @Test
    @Override
    public void testJoinPushdownWithLongIdentifiers()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9733");
    }

    @Test
    public void testJoinPushdownWithImplicitCast()
    {
        try (TestTable leftTable = new TestTable(
                getQueryRunner()::execute,
                "left_table",
                "(id INT, c_tinyint tinyint, c_varchar_10 VARCHAR(10))",
                ImmutableList.of("(1, 11, 'abc')", "(2, 22, 'def')"));
                TestTable rightTable = new TestTable(
                        getQueryRunner()::execute,
                        "right_table_",
                        "(c_bigint bigint, c_varchar_50 VARCHAR(50), c_varchar VARCHAR)",
                        ImmutableList.of("(11, 'abc', 'def')", "(44, 'ghi', 'mno')"))) {
            Session session = joinPushdownEnabled(getSession());

            for (String joinType : List.of("LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN")) {
                // Implicit cast between integer types - tinyint is upcasted to bigint during query optimization
                assertThat(query(session, "SELECT id FROM %s l %s %s r ON l.c_tinyint = r.c_bigint".formatted(leftTable.getName(), joinType, rightTable.getName())))
                        .isFullyPushedDown();
                // Implicit cast between varchar - varchar(10) is upcasted to varchar(50) during query optimization
                assertThat(query(session, "SELECT id FROM %s l %s %s r ON l.c_varchar_10 = r.c_varchar_50".formatted(leftTable.getName(), joinType, rightTable.getName())))
                        .isFullyPushedDown();
                // Implicit cast between varchar - varchar(10) is upcasted to varchar during query optimization
                assertThat(query(session, "SELECT id FROM %s l %s %s r ON l.c_varchar_10 = r.c_varchar".formatted(leftTable.getName(), joinType, rightTable.getName())))
                        .isFullyPushedDown();
            }
        }
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("exceeds maximum length limit of 255 characters");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("exceeds maximum length limit of 255 characters");
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(
                "SQL compilation error: Non-nullable column 'B_VARCHAR' cannot be added to non-empty table " +
                        "'TEST_ADD_NN_.*' unless it has a non-null default value\\.");
    }

    @Test
    public void testIsNullExpressionPredicatePushdown()
    {
        // Simple predicate that can be represented as TupleDomain - expected to pass
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL")).isFullyPushedDown();

        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testIsNotNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NOT NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NOT NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testNotExpressionPushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NOT(name = 'A')")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_predicate_pushdown",
                "(a_int integer, a_varchar varchar(2))",
                List.of(
                        "1, 'Aa'",
                        "2, 'Bb'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar = 'Aa') OR a_int = 2")).isFullyPushedDown();
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar = 'Aa' OR a_int = 2)")).isFullyPushedDown();
        }
    }

    @Test
    public void testWideningCastPushdown()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_widening_cast_pushdown", """
                        (
                            varchar_5 varchar(5),
                            varchar_20 varchar(20),
                            int_col integer,
                            dec_10_2 decimal(10, 2),
                            dec_15_2 decimal(15, 2),
                            a_real real,
                            a_double double,
                            ts_3 timestamp(3),
                            ts_6 timestamp(6),
                            tstz_3 timestamp(3) with time zone,
                            tstz_6 timestamp(6) with time zone
                        )""",
                List.of("'apple', 'apple', 1, 1.50, 1.50, REAL '1.5', DOUBLE '1.5'," +
                        " TIMESTAMP '2024-01-01 00:00:00.000', TIMESTAMP '2024-01-01 00:00:00.000000'," +
                        " TIMESTAMP '2024-01-01 00:00:00.000 UTC', TIMESTAMP '2024-01-01 00:00:00.000000 UTC'"))) {

            // verify that it's not enabled without the session property
            assertThat(query("SELECT varchar_5 FROM " + table.getName() + " WHERE varchar_5 = varchar_20"))
                    .isNotFullyPushedDown(FilterNode.class);

            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE varchar_5 = varchar_20"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE CAST(varchar_5 AS varchar) = 'apple'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE CAST(int_col AS DECIMAL(10, 0)) = 1"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE CAST(int_col AS BIGINT) = 1"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE a_real = a_double"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE dec_10_2 = dec_15_2"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE ts_3 = ts_6"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_5 FROM " + table.getName() + " WHERE tstz_3 = tstz_6"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
        }
    }

    @Test
    public void testNarrowingCastNotPushedDown()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_narrowing_cast_not_pushed_down", """
                        (
                            varchar_5 varchar(5),
                            varchar_20 varchar(20),
                            int_col integer,
                            dec_10_2 decimal(10, 2),
                            dec_5_2 decimal(5, 2),
                            dec_15_5 decimal(15, 5),
                            dec_10_5 decimal(10, 5),
                            ts_3 timestamp(3),
                            ts_6 timestamp(6),
                            tstz_3 timestamp(3) with time zone,
                            tstz_6 timestamp(6) with time zone
                        )""",
                List.of("'apple', 'apple', 1, 1.50, 1.50, 1.50000, 1.50000," +
                        " TIMESTAMP '2024-01-01 00:00:00.000', TIMESTAMP '2024-01-01 00:00:00.000000'," +
                        " TIMESTAMP '2024-01-01 00:00:00.000 UTC', TIMESTAMP '2024-01-01 00:00:00.000000 UTC'"))) {

            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(varchar_20 AS varchar(5)) = varchar_5"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(dec_10_2 AS decimal(5, 2)) = dec_5_2"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(dec_10_2 AS decimal(15, 5)) = dec_15_5"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(dec_10_2 AS decimal(10, 5)) = dec_10_5"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(int_col AS decimal(5, 0)) = dec_5_2"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(ts_6 AS timestamp(3)) = ts_3"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(tstz_6 AS timestamp(3) with time zone) = tstz_3"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(experimentalPushdownEnabled, "SELECT varchar_20 FROM " + table.getName() + " WHERE CAST(int_col AS varchar(20)) = varchar_5"))
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testCoalesceVarcharPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_coalesce_varchar_pushdown",
                "(a_int integer, a_varchar varchar(5), b_varchar varchar(5))",
                List.of(
                        "1, 'apple', 'red'",
                        "2, NULL, 'green'",
                        "3, NULL, NULL"))) {

            // verify that it's not enabled without the session property
            assertThat(query("SELECT a_varchar FROM " + table.getName() + " WHERE COALESCE(a_varchar, '') = ''"))
                    .isNotFullyPushedDown(FilterNode.class);

            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            assertThat(query(experimentalPushdownEnabled, "SELECT a_int FROM " + table.getName() + " WHERE COALESCE(a_varchar, '') = 'apple'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT a_int FROM " + table.getName() + " WHERE COALESCE(a_varchar, b_varchar, '') = 'green'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT a_int FROM " + table.getName() + " WHERE COALESCE(a_varchar, b_varchar) IS NULL"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(experimentalPushdownEnabled, "SELECT a_int FROM " + table.getName() + " WHERE COALESCE(a_varchar, b_varchar) IS NOT NULL"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(2);
            assertThat(query(experimentalPushdownEnabled, "SELECT a_int FROM " + table.getName() + " WHERE NOT(COALESCE(a_varchar, '') = 'apple')"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(2);
            assertThat(query(experimentalPushdownEnabled, "SELECT a_int FROM " + table.getName() + " WHERE COALESCE(COALESCE(a_varchar, b_varchar), '') = 'apple'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
        }
    }

    @Test
    public void testCoalesceMixedCollationPushdown()
    {
        try (TestTable table = new TestTable(
                snowflakeExecutor,
                getSession().getSchema().orElseThrow() + ".test_coalesce_collation_collision",
                "(en_col VARCHAR COLLATE 'en', tr_col VARCHAR COLLATE 'tr')",
                List.of("'t', 't'"))) {

            assertThat(query(getSession(), "SELECT en_col FROM " + table.getName() + " WHERE COALESCE(en_col, tr_col) = 't'"))
                    .isNotFullyPushedDown(FilterNode.class)
                    .result().rowCount().isEqualTo(1);

            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            assertThat(query(experimentalPushdownEnabled, "SELECT en_col FROM " + table.getName() + " WHERE COALESCE(en_col, tr_col) = 't'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
        }
    }

    @Test
    public void testJsonExtractScalarPushdown()
    {
        try (TestTable table = new TestTable(snowflakeExecutor, TEST_SCHEMA + ".test_json_extract_scalar_pushdown", jsonExtractPushdownTestTableDefinition())) {
            // verify that it's not enabled without the session property
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.boolean') = 'true'"))
                    .isNotFullyPushedDown(FilterNode.class);

            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            // json_extract_scalar returns NULL for JSON null or non-existent paths
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.nonexistent') IS NULL"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.null') IS NULL"))
                    .isFullyPushedDown();

            // scalar types
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.boolean') = 'true'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.number_1') = '123'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.number_2') = '3.14'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.number_3') = '12345678901234567890123456789012345678'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.string_1') = 'a string'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.string_2') = 'Bag full of 💰'"))
                    .isFullyPushedDown();

            // json_extract_scalar returns NULL for non-scalar types
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.object') IS NULL"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_1') IS NULL"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_2') IS NULL"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_3') IS NULL"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_4') IS NULL"))
                    .isFullyPushedDown();

            // array/object subscript
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.object.key') = 'value'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_1[0]') IS NULL"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_2[0]') = '1'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_3[0]') = 'one'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.all_types.array_4[0]') = '1'"))
                    .isFullyPushedDown();

            // nested array/object
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.store.book[0].contributors[0][1]') = 'Levine'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.store.bicycle.price') = '19.95'"))
                    .isFullyPushedDown();

            // paths with special characters and bracket notation paths
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract_scalar(json_data, '$.store.book[0][\"special$character\"]') = 'true'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testJsonExtractPushdown()
    {
        try (TestTable table = new TestTable(snowflakeExecutor, TEST_SCHEMA + ".test_json_extract_pushdown", jsonExtractPushdownTestTableDefinition())) {
            // verify that it's not enabled without the session property
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.boolean') = JSON 'true'"))
                    .isNotFullyPushedDown(FilterNode.class);

            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            // json_extract returns NULL for non-existent paths
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.nonexistent') IS NULL"))
                    .isFullyPushedDown();
            // json_extract returns JSON 'null' for JSON null
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.null') = JSON 'null'"))
                    .isFullyPushedDown();

            // scalar types
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.boolean') = JSON 'true'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.number_1') = JSON '123'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.number_2') = JSON '3.14'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.number_3') = JSON '12345678901234567890123456789012345678'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.string_1') = JSON '\"a string\"'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.string_2') = JSON '\"Bag full of 💰\"'"))
                    .isFullyPushedDown();

            // non-scalar types
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.object') = JSON '{\"key_2\": \"value_2\", \"key_1\": \"value_1\"}'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.array_1') = JSON '[]'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.array_2') = JSON '[1, 2]'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.array_3') = JSON '[\"one\", \"two\"]'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.array_4') = JSON '[1, \"two\"]'"))
                    .isFullyPushedDown();

            // array subscript
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.all_types.array_2[1]') = JSON '2'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.store.book[0]') = JSON '{\"author\":\"Nigel Rees\",\"contributors\":[[\"Adam\",\"Levine\"],[\"Bob\",\"Strong\"]],\"special$character\":true}'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.store.book[0].author') = JSON '\"Nigel Rees\"'"))
                    .isFullyPushedDown();

            // nested array/object
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.store.book[0].contributors[0][1]') = JSON '\"Levine\"'"))
                    .isFullyPushedDown();
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.store.bicycle') = JSON '{\"color\":\"red\", \"price\":19.95}'"))
                    .isFullyPushedDown();

            // paths with special characters and bracket notation paths
            assertThat(query(experimentalPushdownEnabled, "SELECT id FROM " + table.getName() + " WHERE json_extract(json_data, '$.store.book[0][\"special$character\"]') = JSON 'true'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(onRemoteDatabase(), schema + ".char_trailing_space", "(x char(10))", List.of("'test'"))) {
            String tableName = table.getName();
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test'", "VALUES 'test'");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test  '", "VALUES 'test'");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test        '", "VALUES 'test'");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char ' test'");
        }
    }

    @Test
    public void testCollatedColumnJoinPushdown()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable leftTable = new TestTable(
                onRemoteDatabase(),
                schema + ".left_collated_join",
                "(lowercase_a VARCHAR COLLATE 'en-ci')",
                ImmutableList.of("('a')"));
             TestTable rightTable = new TestTable(
                     onRemoteDatabase(),
                     schema + ".right_collated_join",
                     "(uppercase_a VARCHAR COLLATE 'en-ci')",
                     ImmutableList.of("('A')"))) {
            Session joinPushdownSession = joinPushdownEnabled(getSession());
            String leftTableName = leftTable.getName();
            String rightTableName = rightTable.getName();

            assertThat(query("SELECT l.lowercase_a, r.uppercase_a FROM %s l INNER JOIN %s r ON l.lowercase_a = r.uppercase_a".formatted(
                    leftTableName,
                    rightTableName)))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT l.lowercase_a, r.uppercase_a FROM %s l LEFT JOIN %s r ON l.lowercase_a = r.uppercase_a".formatted(
                    leftTableName,
                    rightTableName)))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query(joinPushdownSession, "SELECT l.lowercase_a, r.uppercase_a FROM %s l RIGHT JOIN %s r ON l.lowercase_a = r.uppercase_a".formatted(
                    leftTableName,
                    rightTableName)))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT l.lowercase_a, r.uppercase_a FROM %s l FULL JOIN %s r ON l.lowercase_a = r.uppercase_a".formatted(
                    leftTableName,
                    rightTableName)))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    @Test
    public void testCollatedDisjointInPushdown()
    {
        // Confirmed with query history that these get simplified into an IN predicate.
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                schema + ".trimmed_in",
                "(a_with_space VARCHAR COLLATE 'trim')",
                ImmutableList.of(
                        "(' t')",
                        "('m ')",
                        "(' z ')"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() +
                    " WHERE a_with_space = 't' OR a_with_space = 'm' OR a_with_space = 'z'"))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    @Test
    public void testCollatedPredicatePushdown()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                schema + ".case_insensitive_equals",
                "(a VARCHAR COLLATE 'en-ci')",
                ImmutableList.of("('a')", "('A')"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE a = 'a'"))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    @Test
    public void testCollatedTopNPushdown()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                schema + ".top_n_first_upper",
                "(a VARCHAR COLLATE 'upper')",
                ImmutableList.of("('a')", "('A')", "('b')", "('B')"))) {
            @Language("SQL")
            String top2 = "SELECT a FROM " + testTable.getName() + " ORDER BY a ASC LIMIT 2";
            assertThat(query(top2))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    @Test
    public void testCollatedUpdate()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                schema + ".update_upper",
                "(updateable VARCHAR, mykey VARCHAR COLLATE 'upper')",
                ImmutableList.of("('updateme'), ('a')", "('dont update me'), ('A')"))) {
            @Language("SQL")
            String update = "UPDATE " + testTable.getName() + " SET updateable = 'updated' WHERE mykey = 'a'";
            assertUpdate(update, 1);
        }
    }

    @Test
    public void testCollatedDelete()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                schema + ".delete_upper",
                "(my_key VARCHAR COLLATE 'upper')",
                ImmutableList.of("('a')", "('A')"))) {
            @Language("SQL")
            String delete = "DELETE FROM " + testTable.getName() + " WHERE my_key = 'a'";
            assertUpdate(delete, 1);
        }
    }

    @Test
    public void testCollatedExpression()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                schema + ".collated_expression",
                "(en VARCHAR COLLATE 'en', tr VARCHAR COLLATE 'tr')",
                ImmutableList.of("('a'), ('A')"))) {
            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en = tr"))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en <> tr"))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en IS NOT DISTINCT FROM tr"))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en > tr"))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en >= tr"))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en < tr"))
                    .hasCorrectResultsRegardlessOfPushdown();

            assertThat(query("SELECT en, tr FROM " + testTable.getName() + " WHERE en <= tr"))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    @Test
    public void testLikePushdown()
    {
        Session session = experimentalPushdownEnabled();
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                session.getSchema().orElseThrow() + ".test_like_pushdown",
                "(n int, a_varchar VARCHAR(20))",
                List.of("0, null",
                        "1, 'hello'",
                        "2, 'he1lo'",
                        "3, 'he%lo'",
                        "4, 'he\\\\lo'", // SQL "he\\lo" => Snowflake "he\lo" (len 5: 'h' 'e' '\' 'l' 'o')
                        "5, 'HELLO'",
                        "6, 'helLO'",
                        "7, 'hello   '",
                        "8, 'he\\nlo'",  // SQL "he\nlo" => Snowflake "he\nlo" (len 5, 'h' 'e' <new line> 'l' 'o')
                        "9, 'he\\\\1lo'" // SQL "he\\1lo" => Snowflake "he\1lo" (len 6, 'h' 'e' '\' '1', 'l', 'o')
                ))) {
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE NULL")).returnsEmptyResult();
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE a_varchar")).isFullyPushedDown();
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE UPPER(a_varchar)")).isNotFullyPushedDown(FilterNode.class);

            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE 'hello'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE '%'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(9);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE 'h%'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(8);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE '%o'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(6);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE '%e%'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(8);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE 'he_lo'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(5);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE 'hel_o%'"))
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(2);
            // '\' is literal in LIKE patterns; '\%' and '\_' still have '%' and '_' acting as wildcards
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE 'he\\%lo'")) // actual pattern: 'he\%lo'
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(2);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a_varchar LIKE 'he\\_lo'")) // actual pattern: 'he\_lo'
                    .isFullyPushedDown()
                    .result().rowCount().isEqualTo(1);
        }
    }

    @Test
    public void testCompoundPredicatePushdown() {
        Session session = experimentalPushdownEnabled();
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                session.getSchema().orElseThrow() + ".test_predicate_pushdown",
                "(n int, a_varchar VARCHAR(20))",
                List.of("0, null",
                        "1, 'hello'",
                        "2, 'he1lo'",
                        "3, 'he%lo'",
                        "4, 'he\\\\lo'", // SQL "he\\lo" => Snowflake "he\lo" (len 5: 'h' 'e' '\' 'l' 'o')
                        "5, 'HELLO'",
                        "6, 'helLO'",
                        "7, 'hello   '",
                        "8, 'he\\nlo'",  // SQL "he\nlo" => Snowflake "he\nlo" (len 5, 'h' 'e' <new line> 'l' 'o')
                        "9, 'he\\\\1lo'" // SQL "he\\1lo" => Snowflake "he\1lo" (len 6, 'h' 'e' '\' '1', 'l', 'o')
                ))) {
            // These should be pushed down once UPPER/LOWER are pushed down as well
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE UPPER(a_varchar) LIKE 'HE%LO'"))
                    .isNotFullyPushedDown(FilterNode.class)
                    .result().rowCount().isEqualTo(8);
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE LOWER(a_varchar) LIKE 'hello'"))
                    .isNotFullyPushedDown(FilterNode.class)
                    .result().rowCount().isEqualTo(3);
        }
    }

    @Test
    public void testCollatedLikePushdown()
    {
        Session session = experimentalPushdownEnabled();
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                session.getSchema().orElseThrow() + ".collated_like_pushdown",
                "(a VARCHAR(3) COLLATE 'utf8', b VARCHAR(3) COLLATE 'en-ci')",
                ImmutableList.of(
                        "'abc', 'abc'",
                        "'Abc', 'Abc'",
                        "'AbC', 'AbC'",
                        "'abC', 'abC'"
                )
        )) {
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE a LIKE 'a_c'"))
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE b LIKE 'a_c'"))
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT * FROM " + table.getName() + " WHERE b LIKE a"))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    private Session experimentalPushdownEnabled()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                .build();
    }

    private static String jsonExtractPushdownTestTableDefinition()
    {
        // Snowflake doesn't allow using `parse_json` in VALUES of INSERT statement so we need to wrap it inside a SELECT
        return """
                (id VARCHAR, json_data VARIANT)
                AS SELECT id, parse_json(json_data) AS json_data
                FROM VALUES
                    ('row 1', '{
                           "store": {
                             "book": [
                               {
                                 "author": "Nigel Rees",
                                 "special$character": true,
                                 "contributors": [
                                   ["Adam", "Levine"],
                                   ["Bob", "Strong"]
                                 ]
                               },
                               {
                                 "author": "Evelyn Waugh"
                               }
                             ],
                             "bicycle": {
                               "color": "red",
                               "price": 19.95
                             }
                           },
                           "all_types": {
                             "null": null,
                             "boolean": true,
                             "number_1": 123,
                             "number_2": 3.14,
                             "number_3": 12345678901234567890123456789012345678,
                             "string_1": "a string",
                             "string_2": "Bag full of 💰",
                             "object": {
                                "key_1": "value_1",
                                "key_2": "value_2"
                             },
                             "array_1": [],
                             "array_2": [1, 2],
                             "array_3": ["one", "two"],
                             "array_4": [1, "two"]
                           }
                         }'
                    ) t(id, json_data)
                """;
    }

    @Override
    protected String sumDistinctAggregationPushdownExpectedResult()
    {
        return "VALUES (BIGINT '4', DECIMAL '8')";
    }

    @Override
    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        super.createTableForWrites(createTable, tableName, primaryKey, updateCount);
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        primaryKey.ifPresent(key -> onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)", schemaTableName, tableName, key)));
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, String primaryKey)
    {
        TestTable testTable = super.createTestTableForWrites(namePrefix, tableDefinition, primaryKey);
        String tableName = testTable.getName();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)", schemaTableName, tableName, primaryKey));
        return testTable;
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, List<String> rowsToInsert, String primaryKey)
    {
        TestTable testTable = super.createTestTableForWrites(namePrefix, tableDefinition, rowsToInsert, primaryKey);
        String tableName = testTable.getName();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)", schemaTableName, tableName, primaryKey));
        return testTable;
    }
}
