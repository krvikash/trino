/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.oracle.BaseOracleConnectorTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.trino.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static com.starburstdata.trino.plugin.oracle.OracleDataTypes.prestoTimestampWithTimeZoneDataType;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.testing.datatype.DataType.timestampDataType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestStarburstOracleConnectorTest
        extends BaseOracleConnectorTest
{
    private SharedResource.Lease<TestingStarburstOracleServer> oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());

        return OracleQueryRunner.builder(oracleServer)
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.remarks-reporting.enabled", "true")
                        .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_TOPN_PUSHDOWN -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES CAST(1250 AS DECIMAL(19,0)), 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();
    }

    @Test
    public void testStringComparisonConstantPushdown()
    {
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 1 OR name = 'ROMANIA'"))
                .matches(
                        """
                        VALUES (CAST(1 AS DECIMAL(19,0)), CAST(1 AS DECIMAL(19,0)), CAST('ARGENTINA' AS varchar(25))), 
                               (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))""")
                .isFullyPushedDown();
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_string_comparison_pushdown",
                """
                (id int,
                clob_string CLOB,
                nclob_string NCLOB)
                """,
                List.of(
                        "1, 'b', 'b'",
                        "2, 'c', 'c'"))) {
            assertThat(query("SELECT id FROM %s WHERE id = 1 OR clob_string = 'c'".formatted(table.getName())))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT id FROM %s WHERE id = 1 OR nclob_string = 'c'".formatted(table.getName())))
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testNotPushdown()
    {
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name NOT LIKE 'Romania' OR nationkey = 1"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE NOT (name LIKE '%nia' OR nationkey = 1)"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name NOT LIKE 'Romania' OR nationkey = 1 AND NOT name LIKE 'Croatia'"))
                .isFullyPushedDown();
    }

    @Test
    public void testLikePushdown()
    {
        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        String withConnectorExpression = " OR some_column = 'x'";
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_like_pushdown",
                """
                (
                unsupported BFILE,
                some_column VARCHAR2(100),
                a_string VARCHAR2(100),
                a_string_2 VARCHAR2(100),
                a_char CHAR(100),
                a_nvarchar NVARCHAR2(100),
                a_nchar NCHAR(100))
                """,
                List.of(
                        // Escape test: literal backslash
                        "BFILENAME('/opt/oracle', ''), 'z', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\'",
                        // Underscore wildcard
                        "BFILENAME('/opt/oracle', ''), 'z', '_', '_', '_', '_', '_'",
                        // Percent wildcard
                        "BFILENAME('/opt/oracle', ''), 'z', '%', '%', '%', '%', '%'",
                        // Literal characters for comparison
                        "BFILENAME('/opt/oracle', ''), 'z', 'a', 'a', 'a', 'a', 'a'",
                        "BFILENAME('/opt/oracle', ''), 'z', 'b', 'b', 'b', 'b', 'b'",
                        "BFILENAME('/opt/oracle', ''), 'z', 'c', 'c', 'c', 'c', 'c'"))) {
            assertLike(true, table, withConnectorExpression, convertToVarchar);
            assertLike(false, table, withConnectorExpression, convertToVarchar);
        }
    }

    private void assertLike(boolean isPositive, TestTable table, String withConnectorExpression, Session convertToVarchar)
    {
        String like = isPositive ? "LIKE" : "NOT LIKE";

        // Tests for VARCHAR2 column
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " NULL")).returnsEmptyResult();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_string")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_string" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " unsupported")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " unsupported" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

        // Tests for CHAR column
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " NULL")).returnsEmptyResult();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " 'b'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " 'b'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " 'b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " 'b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " '%b'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " '%b'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_char " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

        // Tests for NVARCHAR column
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " NULL")).returnsEmptyResult();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " 'b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " 'b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " 'b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " 'b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " '%b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " '%b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " '%b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nvarchar " + like + " '%b%'" + withConnectorExpression)).isFullyPushedDown();

        // Tests for NCHAR column
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " NULL")).returnsEmptyResult();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " 'b'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " 'b'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " 'b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " 'b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " '%b'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " '%b'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nchar " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

        // metacharacters for VARCHAR2
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '_'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '_'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '__'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '__'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%%'" + withConnectorExpression)).isFullyPushedDown();

        // escape for VARCHAR2
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\_'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\_'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\__'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\__'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\\\'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\' ESCAPE '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\' ESCAPE '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%' ESCAPE '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%' ESCAPE '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%$_%' ESCAPE '$'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%$_%' ESCAPE '$'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
    }

    @Override
    protected String getUser()
    {
        return OracleTestUsers.USER;
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return oracleServer.get().getSqlExecutor();
    }

    @Test
    public void testCreateTableAsSelectIntoAnotherUsersSchema()
    {
        // running test in two schemas to ensure we test-cover table creation in a non-default schema
        testCreateTableAsSelectIntoAnotherUsersSchema("alice");
        testCreateTableAsSelectIntoAnotherUsersSchema("bob");
    }

    private void testCreateTableAsSelectIntoAnotherUsersSchema(String user)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, format("oracle.%s.nationkeys_copy", user), "AS SELECT nationkey FROM nation", ImmutableList.of("123456789"))) {
            assertQuery(format("SELECT * FROM %s", table.getName()), "SELECT nationkey FROM nation UNION SELECT 123456789");
        }
    }

    @Test
    public void testGetColumns()
    {
        // OracleClient.getColumns is using wildcard at the end of table name.
        // Here we test that columns do not leak between tables.
        // See OracleClient#getColumns for more details.
        try (TestTable ignored = new TestTable(onRemoteDatabase(), "ordersx", "AS SELECT 'a' some_additional_column FROM dual")) {
            assertQuery(
                    format("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' AND table_schema = '%s'", getUser()),
                    "VALUES 'orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority', 'comment'");
        }
    }

    @Test
    public void testAdditionalPredicatePushdownForChars()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                getUser() + ".test_predicate_pushdown_char",
                "(c_long_char CHAR(2000), c_long_varchar VARCHAR2(4000))",
                ImmutableList.of("'my_long_char', 'my_long_varchar'"))) {
            // Verify using a large value in WHERE, larger than the 2000 and 4000 bytes Oracle max
            // this does not work in Oracle 11
            assertThat(query(format("SELECT c_long_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", table.getName()))).isFullyPushedDown();
            assertThat(query(format("SELECT c_long_varchar FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", table.getName()))).isFullyPushedDown();
        }
    }

    /**
     * This test covers only predicate pushdown for Oracle (it doesn't test timestamp semantics).
     *
     * @see TestOracleTypeMapping
     * @see io.trino.testing.AbstractTestDistributedQueries
     */
    @Test
    public void testPredicatePushdownForTimestamps()
    {
        LocalDateTime date1950 = LocalDateTime.of(1950, 5, 30, 23, 59, 59, 0);
        ZonedDateTime yakutat1978 = ZonedDateTime.of(1978, 4, 30, 23, 55, 10, 10, ZoneId.of("America/Yakutat"));
        ZonedDateTime pacific1976 = ZonedDateTime.of(1976, 3, 15, 0, 2, 22, 10, ZoneId.of("Pacific/Wake"));

        List<String> values = ImmutableList.<String>builder()
                .add(timestampDataType().toLiteral(date1950))
                .add(oracleTimestamp3TimeZoneDataType().toLiteral(yakutat1978))
                .add(prestoTimestampWithTimeZoneDataType().toLiteral(pacific1976))
                .add("'result_value'")
                .build();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                getUser() + ".test_predicate_pushdown_timestamp",
                "(t_timestamp TIMESTAMP, t_timestamp3_with_tz TIMESTAMP(3) WITH TIME ZONE, t_timestamp_with_tz TIMESTAMP WITH TIME ZONE, dummy_col VARCHAR(12))",
                ImmutableList.of(String.join(", ", values)))) {
            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp = %s",
                    table.getName(),
                    format("timestamp '%s'", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(date1950)))))
                    .isFullyPushedDown();

            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp3_with_tz = %s",
                    table.getName(),
                    prestoTimestampWithTimeZoneDataType().toLiteral(yakutat1978))))
                    .isFullyPushedDown();

            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp_with_tz = %s",
                    table.getName(),
                    prestoTimestampWithTimeZoneDataType().toLiteral(pacific1976))))
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertQueryFails("CALL system.execute('SELECT 1')", "(?s)Failed to execute query.*");
        assertQueryFails("CALL system.execute('invalid')", "(?s)Failed to execute query.*");
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        if (columnName.equals("a\"quote") && exception.getMessage().contains("ORA-03001: unimplemented feature")) {
            return true;
        }

        return false;
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9681");
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9681");
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        abort("https://starburstdata.atlassian.net/browse/SEP-9681");
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(short_decimal number(9, 3), long_decimal number(30, 10), a_bigint number(19), t_double binary_double)", rows);
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }
}
