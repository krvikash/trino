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
package io.trino.plugin.singlestore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.plugin.jdbc.JoinOperator.RIGHT_JOIN;
import static io.trino.plugin.singlestore.SingleStoreQueryRunner.TPCH_SCHEMA;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSingleStoreConnectorTest
        extends BaseJdbcConnectorTest
{
    private static final String CONNECTOR_NAME = "singlestore";
    protected TestingSingleStoreServer singleStoreServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        singleStoreServer = new TestingSingleStoreServer();
        return SingleStoreQueryRunner.builder(singleStoreServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterAll
    public final void destroy()
    {
        singleStoreServer.close();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two bit(10), three varchar(10))");
    }

    @Override
    @Test
    public void testNativeQuerySelectUnsupportedType()
    {
        // No SingleStore type is found to be unsupported by the connector and also unsupported when type introspection is done via ResultSetMetaData.
        super.testNativeQuerySelectUnsupportedType(true);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();

        return switch (typeName) {
            // SingleStore does not have built-in support for boolean type. SingleStore provides BOOLEAN as the synonym of TINYINT(1)
            // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
            case "boolean" -> Optional.empty();
            // SingleStore supports only second precision
            // Skip 'time' that is alias of time(3) here and add test cases in TestSingleStoreTypeMapping.testTime instead
            case "time" -> Optional.empty();
            case "timestamp(3) with time zone", "timestamp(6) with time zone" -> Optional.of(dataMappingTestSetup.asUnsupported());
            // TODO this should either work or fail cleanly
            case "date" -> {
                // The connector supports date type, but value less than `1000-01-01` are unsupported in SingleStore
                // See BaseSingleStoreTypeMapping for additional test coverage
                if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '0001-01-01'")) {
                    yield Optional.empty();
                }
                yield Optional.of(dataMappingTestSetup);
            }
            case "timestamp" -> Optional.empty();
            // TODO fails due to case insensitive UTF-8 comparisons
            case "varchar" -> Optional.empty();
            default -> Optional.of(dataMappingTestSetup);
        };
    }

    @Test
    void testCreateDropMultipleCatalogs()
    {
        String firstCatalog = "catalog1_" + randomNameSuffix();
        String secondCatalog = "catalog2_" + randomNameSuffix();
        try {
            @Language("SQL")
            String createFirstCatalogSql = CREATE_CATALOG_SQL_TEMPLATE
                    .formatted(firstCatalog, CONNECTOR_NAME, singleStoreServer.getPassword(), singleStoreServer.getJdbcUrl(), singleStoreServer.getUsername());
            assertUpdate(createFirstCatalogSql);
            assertThat((String) computeActual("SHOW CREATE CATALOG " + firstCatalog).getOnlyValue())
                    .isEqualTo(createFirstCatalogSql);
            assertQuerySucceeds("SHOW TABLES FROM %s.%s".formatted(firstCatalog, TPCH_SCHEMA));

            @Language("SQL")
            String createSecondCatalogSql =
                    """
                    CREATE CATALOG %s USING %s
                    WITH (
                       "connection-password" = '%s',
                       "connection-url" = '%s',
                       "connection-user" = '%s',
                       "jdbc-types-mapped-to-varchar" = 'true'
                    )""".formatted(secondCatalog, CONNECTOR_NAME, singleStoreServer.getPassword(), singleStoreServer.getJdbcUrl(), singleStoreServer.getUsername());
            assertUpdate(createSecondCatalogSql);
            assertThat((String) computeActual("SHOW CREATE CATALOG " + secondCatalog).getOnlyValue())
                    .isEqualTo(createSecondCatalogSql);
            assertQuerySucceeds("SHOW TABLES FROM %s.%s".formatted(secondCatalog, TPCH_SCHEMA));
        }
        finally {
            assertUpdate("DROP CATALOG IF EXISTS " + firstCatalog);
            assertUpdate("DROP CATALOG IF EXISTS " + secondCatalog);
        }
    }

    @Override
    protected Map<String, String> getBehaviorAlteringCatalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-password", "INVALID")
                .buildOrThrow();
    }

    @Override
    protected void assertAlteredCatalogBehavior(String catalogName)
    {
        assertThatThrownBy(() -> computeActual(format("SHOW TABLES FROM %s.%s", catalogName, TPCH_SCHEMA)))
                .isInstanceOf(QueryFailedException.class)
                .hasMessageMatching(".*Access denied for user 'root'@'.*' \\(using password: YES\\)");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        // SingleStore's utf8 encoding is 3 bytes and truncates strings upon encountering a 4 byte sequence
        abort("SingleStore doesn't support utf8mb4");
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // SingleStore's utf8 encoding is 3 bytes and truncates strings upon encountering a 4 byte sequence
        abort("SingleStore doesn't support utf8mb4");
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    public void testReadFromView()
    {
        onRemoteDatabase().execute("CREATE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testNameEscaping()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "test_table")).isFalse();

        assertUpdate(getSession(), "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertThat(getQueryRunner().tableExists(getSession(), "test_table")).isTrue();

        assertQuery(getSession(), "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(getSession(), "DROP TABLE test_table");
        assertThat(getQueryRunner().tableExists(getSession(), "test_table")).isFalse();
    }

    @Test
    public void testSingleStoreTinyint()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        assertThat(query("SHOW COLUMNS FROM mysql_test_tinyint1"))
                .result().matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("c_tinyint", "tinyint", "", "")
                        .build());

        onRemoteDatabase().execute("INSERT INTO tpch.mysql_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.mysql_test_tinyint1 WHERE c_tinyint = 127");
        assertThat(materializedRows.getRowCount())
                .isEqualTo(1);
        MaterializedRow row = getOnlyElement(materializedRows);

        assertThat(row.getFields().size())
                .isEqualTo(1);
        assertThat(row.getField(0))
                .isEqualTo((byte) 127);

        assertUpdate("DROP TABLE mysql_test_tinyint1");
    }

    // Overridden because the method from BaseConnectorTest fails on one of the assertions, see TODO below
    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        try (TestTable table = newTrinoTable("insert_not_null", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            assertQueryFails(format("INSERT INTO %s (nullable_col) VALUES (1)", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (TRY(5/0), 4)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col) VALUES (TRY(6/0))", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            // TODO (https://github.com/trinodb/trino/issues/13551) This doesn't fail for other connectors so
            //  probably shouldn't fail for SingleStore either. Once fixed, remove test override.
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation WHERE regionkey < 0", table.getName()), ".*Field 'not_null_col' doesn't have a default value.*");
        }

        try (TestTable table = newTrinoTable("commuted_not_null", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format(".* Field '%s' doesn't have a default value", columnName);
    }

    @Test
    public void testColumnComment()
    {
        // TODO add support for setting comments on existing column and replace the test with io.trino.testing.BaseConnectorTest#testCommentColumn

        onRemoteDatabase().execute("CREATE TABLE tpch.test_column_comment (col1 bigint COMMENT 'test comment', col2 bigint COMMENT '', col3 bigint)");

        assertQuery(
                "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('col1', 'test comment'), ('col2', null), ('col3', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

    @Test
    @Override
    public void testAddNotNullColumn()
    {
        assertThatThrownBy(super::testAddNotNullColumn)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Should fail to add not null column without a default value to a non-empty table");

        try (TestTable table = newTrinoTable("test_add_nn_col", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    // SingleStore adds implicit default value of '' for b_varchar
                    .matches("VALUES ('a', '')");
        }
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();
    }

    /**
     * Overrides the test to account for join pushdown behavior, where column
     * aliases may have additional suffix characters appended.
     * <p>
     * The maximum identifier length is therefore reduced by 2 to ensure generated
     * column aliases remain within the allowed limit after join pushdown.
     */
    @Test
    @Override
    public void testJoinPushdownWithLongIdentifiers()
    {
        String baseColumnName = "col";
        int maxLength = maxColumnNameLength().orElseThrow() - 2; // 2 extra chars for column alias name when join is pushed down

        String validColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        try (TestTable left = newTrinoTable("test_long_id_l", format("(%s BIGINT)", validColumnName));
                TestTable right = newTrinoTable("test_long_id_r", format("(%s BIGINT)", validColumnName))) {
            assertThat(query(joinPushdownEnabled(getSession()),
                    """
                    SELECT l.%1$s, r.%1$s
                    FROM %2$s l JOIN %3$s r ON l.%1$s = r.%1$s\
                    """.formatted(validColumnName, left.getName(), right.getName())))
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        // In latest versions SingleStore throws error when inserting invalid dates
        assertThatThrownBy(super::testCreateTableAsSelectNegativeDate)
                .hasStackTraceContaining("Invalid DATE/TIME in type conversion for column 'dt'");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        // In latest versions SingleStore throws error when inserting invalid dates
        assertThatThrownBy(super::testInsertNegativeDate)
                .hasStackTraceContaining("Invalid DATE/TIME in type conversion for column 'dt'");
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        // SingleStore returns a ResultSet metadata with no columns for CREATE TABLE statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for CREATE TABLE at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in SingleStore because the connector wraps it in additional syntax, which causes syntax error.
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // SingleStore returns a ResultSet metadata with no columns for INSERT statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for INSERT at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in SingleStore because the connector wraps it in additional syntax, which causes syntax error.
        try (TestTable testTable = simpleTable()) {
            assertThat(query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                "(.*)Invalid DATE/TIME in type conversion");
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        onRemoteDatabase().execute("SELECT count(*) FROM tpch.orders WHERE " + getLongInClause(0, 300_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 30)
                .mapToObj(value -> getLongInClause(value * 10_000, 10_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute("SELECT count(*) FROM tpch.orders WHERE " + longInClauses);
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Test
    public void testStringPushdownWithBinary()
    {
        Session session = stringPushdownWithBinaryEnabled(getSession());

        // varchar equality
        assertThat(query(session, "SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query(session, "SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN without domain compaction
        assertThat(query(session, "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(session)
                        .setCatalogSessionProperty("singlestore", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // Verify that a FilterNode is retained and only a compacted domain is pushed down to connector as a range predicate
                .isNotFullyPushedDown(node(FilterNode.class, tableScan(
                        tableHandle -> {
                            TupleDomain<ColumnHandle> constraint = ((JdbcTableHandle) tableHandle).getConstraint();
                            ColumnHandle nameColumn = constraint.getDomains().orElseThrow()
                                    .keySet().stream()
                                    .map(JdbcColumnHandle.class::cast)
                                    .filter(column -> column.getColumnName().equals("name"))
                                    .collect(onlyElement());
                            return constraint.getDomains().get().get(nameColumn).getValues().getRanges().getOrderedRanges()
                                    .equals(ImmutableList.of(
                                            Range.range(
                                                    createVarcharType(25),
                                                    utf8Slice("POLAND"),
                                                    true,
                                                    utf8Slice("VIETNAM"),
                                                    true)));
                        },
                        TupleDomain.all(),
                        ImmutableMap.of())));

        // varchar different case
        assertThat(query(session, "SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // varchar predicate over join
        Session joinPushdownEnabled = joinPushdownEnabled(session);
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isFullyPushedDown();

        // join on varchar columns
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.address = n.name"))
                .isFullyPushedDown();

        // varchar IS (NOT) NULL predicate
        try (TestTable table = newTrinoTable("test_null", "(id INT, data VARCHAR)", ImmutableList.of("1, 'test'", "2, NULL"))) {
            assertThat(query(session, "SELECT id FROM " + table.getName() + " WHERE data IS NULL"))
                    .matches("VALUES 2")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT id FROM " + table.getName() + " WHERE data IS NOT NULL"))
                    .matches("VALUES 1")
                    .isFullyPushedDown();
        }

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_string_pushdown",
                """
                (
                some_varchar varchar(255),
                some_char char(16),
                other_column varchar(255)
                )
                """,
                List.of(
                        "null, null, null",
                        "'AA', 'AA', 'AA'",
                        "'aa', 'aa', 'aa'",
                        "'bb', 'bb', 'bb'",
                        "'cc', 'cc', 'cc'"))) {
            // char pushdown
            assertThat(query(session, "SELECT some_char FROM " + table.getName() + " WHERE some_char = 'aa'")).isFullyPushedDown();
            // char BETWEEN varchar literals is planned as a Let over CAST(char AS varchar), which blocks domain extraction
            assertThat(query(session, "SELECT some_char FROM " + table.getName() + " WHERE some_char BETWEEN 'aa' AND 'bb'"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_char FROM " + table.getName() + " WHERE some_char = 'AA'")).isFullyPushedDown();

            // equality/like/in on same column transformed to IN via queryBuilder
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar = 'aa' OR some_varchar = 'BB'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar IN ('aa', 'bb')")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE 'aa' OR some_varchar = 'BB'")).isFullyPushedDown();
            // char IN varchar literals is planned as IN over CAST(char AS varchar), which blocks domain extraction
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char = 'aa' OR some_char = 'BB'"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char IN ('aa', 'bb')"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE 'aa' OR some_char = 'BB'")).isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    void testStringComplexExpressionPushdownWithBinary()
    {
        Session session = stringPushdownWithBinaryEnabled(getSession());
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_string_complex_expression_pushdown",
                """
                (
                some_varchar varchar(255),
                some_char char(16),
                some_longtext longtext,
                other_varchar varchar(255),
                other_char char(16)
                )
                """,
                List.of(
                        "null, null, null, null, null",
                        "'AA', 'AA', 'AA', 'AA', 'AA'",
                        "'aa', 'aa', 'aa', 'aa', 'aa'",
                        "'BB', 'BB', 'BB', 'BB', 'BB'",
                        "'bb', 'bb', 'bb', 'bb', 'bb'",
                        "'cc', 'cc', 'cc', 'cc', 'cc'",
                        "'dd', 'dd', null, null, null"))) {
            for (String operator : List.of("=", "<>", "<", "<=", ">", ">=", "IS NOT DISTINCT FROM")) {
                assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar " + operator + " 'aa' OR other_varchar = 'bb'")).isFullyPushedDown();
                // assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char " + operator + " 'aa' OR other_char = 'bb'")).isFullyPushedDown();

                assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar " + operator + " other_varchar OR other_varchar = 'bb'")).isFullyPushedDown();
//                assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char " + operator + " other_char OR other_char = 'bb'")).isFullyPushedDown();

                // Comparing VARCHARs of different lengths uses CAST internally, CAST pushdown is not supported yet
                assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar " + operator + " some_longtext OR other_varchar = 'bb'")).isNotFullyPushedDown(FilterNode.class);
                assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char " + operator + " some_longtext OR other_char = 'bb'")).isNotFullyPushedDown(FilterNode.class);
            }

            // not pushed because there is no rewrite rule for IS NULL
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar IS NULL OR other_varchar = 'bb'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char IS NULL OR other_char = 'bb'")).isNotFullyPushedDown(FilterNode.class);

            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar IS NOT NULL OR other_varchar = 'bb'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char IS NOT NULL OR other_char = 'bb'")).isNotFullyPushedDown(FilterNode.class);

            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar IS NOT DISTINCT FROM NULL OR other_varchar = 'bb'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char IS NOT DISTINCT FROM NULL OR other_char = 'bb'")).isNotFullyPushedDown(FilterNode.class);

            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar IN ('aa', 'dd') OR other_varchar = 'bb'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char IN ('aa', 'dd') OR other_char = 'bb'")).isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testLikePushdownWithBinary()
    {
        Session session = stringPushdownWithBinaryEnabled(getSession());

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_like_pushdown",
                """
                (
                some_char char(4),
                some_tiny_text tinytext,
                some_text text,
                some_long_text longtext,
                some_varchar varchar(255),
                other_column varchar(255)
                )
                """,
                List.of(
                        "null, null, null, null, null, null",
                        "'AA', 'AA', 'AA', 'AA', 'AA', 'AA'",
                        "'aa', 'aa', 'aa', 'aa', 'aa', 'aa'",
                        "'bb', 'bb', 'bb', 'bb', 'bb', 'bb'",
                        "'%%', '%%', '%%', '%%', '%%', '%%'"))) {
            // tiny text
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE NULL")).isReplacedWithEmptyValues();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE '%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE 'aa'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE 'a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE '%a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE '%a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE 'a_'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE '_a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE 'a%' OR some_tiny_text = 'BB'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_tiny_text LIKE 'a%' OR other_column = 'BB'")).isFullyPushedDown();

            // text
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE NULL")).isReplacedWithEmptyValues();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE '%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE 'aa'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE 'a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE '%a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE '%a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE 'a_'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE '_a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE 'a%' OR some_text = 'BB'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_text LIKE 'a%' OR other_column = 'BB'")).isFullyPushedDown();

            // long text
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE NULL")).isReplacedWithEmptyValues();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE '%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE 'aa'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE 'a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE '%a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE '%a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE 'a_'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE '_a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE 'a%' OR some_text = 'BB'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_long_text LIKE 'a%' OR other_column = 'BB'")).isFullyPushedDown();

            // varchar
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE NULL")).isReplacedWithEmptyValues();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE '%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE 'aa'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE 'a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE '%a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE '%a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE 'a_'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE '_a'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE 'a%' OR some_varchar = 'BB'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE 'a%' OR other_column = 'BB'")).isFullyPushedDown();

            // char - not pushed down because SingleStore has different padding handing in LIKE than in trino
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE NULL")).isReplacedWithEmptyValues();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE '%'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE 'aa'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE 'a%'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE '%a'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE '%a%'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE 'a_'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE '_a'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE 'a%' OR some_char = 'BB'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_char LIKE 'a%' OR other_column = 'BB'")).isNotFullyPushedDown(FilterNode.class);

            // SingleStore doesn't natively support ESCAPE syntax so we simulate it in trino
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar LIKE '|%%' ESCAPE '|'")).isNotFullyPushedDown(FilterNode.class);

            // like with NOT
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar NOT LIKE NULL")).isReplacedWithEmptyValues();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar NOT LIKE 'a%'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE some_varchar NOT LIKE 'aa' OR other_column = 'aa'")).isFullyPushedDown();
            assertThat(query(session, "SELECT some_varchar FROM " + table.getName() + " WHERE NOT (some_varchar LIKE '%a' OR other_column = 'bb')")).isFullyPushedDown();
        }
    }

    @Test
    public void testJoinsWithBinary()
    {
        Session session = Session.builder(joinPushdownEnabled(getSession()))
                .setCatalogSessionProperty("singlestore", "enable_string_pushdown_with_binary", "true")
                .build();

        try (TestTable leftTable = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_join_with_binary_left",
                """
                (
                some_varchar_255 varchar(255),
                some_varchar_10000 varchar(10000),
                some_longtext longtext
                )
                """,
                List.of(
                        "null, null, null",
                        "'AA', 'AA', 'AA'",
                        "'aa', 'aa', 'aa'",
                        "'aa  ', 'aa  ', 'aa  '",
                        "'bb', 'bb', 'bb'",
                        "'cc', 'cc', 'cc'"));
                TestTable rightTable = new TestTable(
                        onRemoteDatabase(),
                        "tpch.single_store_join_with_binary_right",
                        """
                        (
                        some_varchar_255 varchar(255),
                        some_varchar_10000 varchar(10000),
                        some_longtext longtext
                        )
                        """,
                        List.of(
                                "null, null, null",
                                "'AA', 'AA', 'AA'",
                                "'aa', 'aa', 'aa'",
                                "'aa  ', 'aa  ', 'aa  '",
                                "'bb', 'bb', 'bb'",
                                "'cc', 'cc', 'cc'"))) {
            assertThat(query(session, "SELECT r.some_varchar_255 FROM %s l LEFT JOIN %s r ON l.some_varchar_255 = r.some_varchar_255".formatted(leftTable.getName(), rightTable.getName()))).isFullyPushedDown();
            assertThat(query(session, "SELECT r.some_varchar_255 FROM %s l LEFT JOIN %s r ON l.some_varchar_255 = r.some_varchar_10000".formatted(leftTable.getName(), rightTable.getName()))).isFullyPushedDown();
            assertThat(query(session, "SELECT r.some_varchar_255 FROM %s l LEFT JOIN %s r ON l.some_varchar_255 = r.some_longtext".formatted(leftTable.getName(), rightTable.getName()))).isFullyPushedDown();
            assertThat(query(session, "SELECT r.some_varchar_255 FROM %s l LEFT JOIN %s r ON l.some_varchar_10000 = r.some_longtext".formatted(leftTable.getName(), rightTable.getName()))).isFullyPushedDown();
        }
    }

    @Test
    public void testDateTimestampSimplePredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.date_related_simple_predicate_pushdown",
                """
                (
                c_date date,
                c_datetime datetime,
                c_datetime_6 datetime(6),
                c_timestamp timestamp,
                c_timestamp_6 timestamp(6)
                )
                """,
                List.of(
                        "null, null, null, null, null",
                        "'2022-01-01', '2023-01-01', '2022-01-01 10:00:00', '2023-01-01 10:00:00', '2022-01-01 10:00:00.000001'",
                        "'2023-01-01', '2024-01-01', '2023-01-01 10:00:00', '2024-01-01 10:00:00', '2023-01-01 10:00:00.000001'",
                        "'2024-01-01', '2025-01-01', '2024-01-01 10:00:00', '2025-01-01 10:00:00', '2024-01-01 10:00:00.000001'"))) {
            for (String operator : List.of("=", "<>", "<", "<=", ">", ">=", "IS NOT DISTINCT FROM")) {
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '2023-01-01'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 10:00:00 UTC'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 11:00:00 +01:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 10:00:00Z'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 11:00:00 Europe/Warsaw'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 10:00:00.000001 UTC'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 +01:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 10:00:00.000001Z'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 Europe/Warsaw'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 10:00:00 UTC'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 11:00:00 +01:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 10:00:00Z'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 11:00:00 Europe/Warsaw'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 10:00:00.000001 UTC'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 +01:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 10:00:00.000001Z'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 Europe/Warsaw'".formatted(table.getName(), operator))).isFullyPushedDown();
            }
        }
    }

    @Test
    public void testDateTimestampComplexPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.date_related_complex_predicate_pushdown",
                """
                (
                c_date date,
                c_other_date date,
                c_datetime datetime,
                c_other_datetime datetime,
                c_datetime_6 datetime(6),
                c_other_datetime_6 datetime(6),
                c_timestamp timestamp,
                c_other_timestamp timestamp,
                c_timestamp_6 timestamp(6),
                c_other_timestamp_6 timestamp(6)
                )
                """,
                List.of(
                        "null, null, null, null, null, null, null, null, null, null",
                        "'2022-01-01', '2023-01-01', '2022-01-01 10:00:00', '2023-01-01 10:00:00', '2022-01-01 10:00:00.000001', '2023-01-01 10:00:00.000001', '2022-01-01 10:00:00', '2023-01-01 10:00:00', '2022-01-01 10:00:00.000001', '2023-01-01 10:00:00.000001'",
                        "'2023-01-01', '2024-01-01', '2023-01-01 10:00:00', '2024-01-01 10:00:00', '2023-01-01 10:00:00.000001', '2024-01-01 10:00:00.000001', '2023-01-01 10:00:00', '2024-01-01 10:00:00', '2023-01-01 10:00:00.000001', '2024-01-01 10:00:00.000001'",
                        "'2024-01-01', '2025-01-01', '2024-01-01 10:00:00', '2025-01-01 10:00:00', '2024-01-01 10:00:00.000001', '2025-01-01 10:00:00.000001', '2024-01-01 10:00:00', '2025-01-01 10:00:00', '2024-01-01 10:00:00.000001', '2025-01-01 10:00:00.000001'"))) {
            for (String operator : List.of("=", "<>", "<", "<=", ">", ">=", "IS NOT DISTINCT FROM")) {
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '2023-01-01' OR c_other_date = DATE '2024-01-01'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s c_other_date OR c_datetime = c_other_datetime".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s c_other_datetime OR c_date = c_other_date".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 10:00:00' OR c_other_datetime = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                // below predicates are pushdown without having constant rewrite rules for timestamp with timezones type
                // because UnwrapCastInComparison rule makes literals with timezones to be cast to literals without timezones + casts for literals are done on trino level
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 10:00:00 UTC' OR c_other_datetime = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 11:00:00 +01:00' OR c_other_datetime = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 10:00:00Z' OR c_other_datetime = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '2023-01-01 11:00:00 Europe/Warsaw' OR c_other_datetime = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s c_other_datetime_6 OR c_date = c_other_date".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 10:00:00.000001' OR c_other_datetime_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 10:00:00.000001 UTC' OR c_other_datetime_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 +01:00' OR c_other_datetime_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 10:00:00.000001Z' OR c_other_datetime_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 Europe/Warsaw' OR c_other_datetime_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s c_other_timestamp OR c_date = c_other_date".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 10:00:00' OR c_other_timestamp = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 10:00:00 UTC' OR c_other_timestamp = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 11:00:00 +01:00' OR c_other_timestamp = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 10:00:00Z' OR c_other_timestamp = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '2023-01-01 11:00:00 Europe/Warsaw' OR c_other_timestamp = TIMESTAMP '2024-01-01 10:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();

                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s c_other_timestamp_6 OR c_date = c_other_date".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 10:00:00.000001' OR c_other_timestamp_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 10:00:00.000001 UTC' OR c_other_timestamp_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 +01:00' OR c_other_timestamp_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 10:00:00.000001Z' OR c_other_timestamp_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '2023-01-01 11:00:00.000001 Europe/Warsaw' OR c_other_timestamp_6 = TIMESTAMP '2024-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isFullyPushedDown();

                // literals on edges and outside of ranges supported "officially" by SingleStore - see comment in io.trino.plugin.singlestore.RewriteDateTimestampConstant
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '-0001-01-01' OR c_other_date = DATE '1000-01-01'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '999-01-01' OR c_other_date = DATE '1000-01-01'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '1000-01-01' OR c_other_date = DATE '1000-01-01'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '9999-12-31' OR c_other_date = DATE '1000-01-01'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_date FROM %s WHERE c_date %s DATE '10000-01-01' OR c_other_date = DATE '1000-01-01'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);

                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '-0001-01-01 23:59:59' OR c_other_datetime = TIMESTAMP '1000-01-01 00:00:00'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '0999-12-31 23:59:59' OR c_other_datetime = TIMESTAMP '1000-01-01 00:00:00'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '1000-01-01 00:00:00' OR c_other_datetime = TIMESTAMP '1000-01-01 00:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '9999-12-31 23:59:59' OR c_other_datetime = TIMESTAMP '1000-01-01 00:00:00'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime FROM %s WHERE c_datetime %s TIMESTAMP '10000-01-01 00:00:00' OR c_other_datetime = TIMESTAMP '1000-01-01 00:00:00'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);

                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '-0001-01-01 23:59:59.999999' OR c_other_datetime_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '0999-12-31 23:59:59.999999' OR c_other_datetime_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '1000-01-01 00:00:00.000000' OR c_other_datetime_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '9999-12-31 23:59:59.999999' OR c_other_datetime_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_datetime_6 FROM %s WHERE c_datetime_6 %s TIMESTAMP '10000-01-01 00:00:00.000000' OR c_other_datetime_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);

                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '-0001-01-01 23:59:59' OR c_other_timestamp = TIMESTAMP '2023-01-01 10:00:00'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '0999-12-31 23:59:59' OR c_other_timestamp = TIMESTAMP '2023-01-01 10:00:00'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '1000-01-01 00:00:00' OR c_other_timestamp = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '9999-12-31 23:59:59' OR c_other_timestamp = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp FROM %s WHERE c_timestamp %s TIMESTAMP '10000-01-01 00:00:00' OR c_other_timestamp = TIMESTAMP '2023-01-01 10:00:00'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);

                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '-0001-01-01 23:59:59.999999' OR c_other_timestamp_6 = TIMESTAMP '2023-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '0999-12-31 23:59:59.999999' OR c_other_timestamp_6 = TIMESTAMP '2023-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '1000-01-01 00:00:00.000000' OR c_other_timestamp_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '9999-12-31 23:59:59.999999' OR c_other_timestamp_6 = TIMESTAMP '1000-01-01 00:00:00.000000'".formatted(table.getName(), operator))).isFullyPushedDown();
                assertThat(query("SELECT c_timestamp_6 FROM %s WHERE c_timestamp_6 %s TIMESTAMP '10000-01-01 00:00:00.000000' OR c_other_timestamp_6 = TIMESTAMP '2023-01-01 10:00:00.000001'".formatted(table.getName(), operator))).isNotFullyPushedDown(FilterNode.class);
            }
        }
    }

    @Test
    @Override
    public void testCountDistinctWithStringTypes()
    {
        List<String> rows = Stream.of(null, "a", "b", "A", "B", " a ", "a", "b", " b ", "ą")
                .map(value -> value == null ? "NULL, NULL" : format("'%1$s', '%1$s'", value))
                .collect(toList());

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "distinct_strings", "(t_char CHAR(5), t_varchar VARCHAR(5))", rows)) {
            Session session = stringPushdownWithBinaryEnabled(getSession());
            assertThat(query(session, "SELECT count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '7'")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT count(DISTINCT t_char) FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '7'")
                    .isFullyPushedDown();

            Session withMarkDistinct = sessionWithDistinctAggregationsStrategy(session, "mark_distinct");
            assertThat(query(withMarkDistinct, "SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '7', BIGINT '7')")
                    .isFullyPushedDown();
            Session withSingleStep = sessionWithDistinctAggregationsStrategy(session, "single_step");
            assertThat(query(withSingleStep, "SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '7', BIGINT '7')")
                    .isFullyPushedDown();
            Session withPreAggregate = sessionWithDistinctAggregationsStrategy(session, "pre_aggregate");
            assertThat(query(withPreAggregate, "SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '7', BIGINT '7')")
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override
    public void testDistinctAggregationPushdown()
    {
        Session session = stringPushdownWithBinaryEnabled(getSession());
        // Overridden because SingleStore connector supports pushdown of all aggregation functions including multiple DISTINCTs
        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query(session, "SELECT DISTINCT name, min(comment) FROM nation GROUP BY name")).isFullyPushedDown();
        // Integral types
        try (TestTable emptyTable = createAggregationTestTable("tpch.empty_table", ImmutableList.of())) {
            assertThat(query("SELECT DISTINCT a_bigint FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT min(DISTINCT a_bigint) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT DISTINCT t_double, min(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
        }

        // String types
        try (TestTable emptyTable = new TestTable(onRemoteDatabase(), "tpch.empty_table", "(a_varchar varchar(10), a_char char(10))", ImmutableList.of())) {
            assertThat(query(session, "SELECT DISTINCT a_varchar, count(a_char) FROM " + emptyTable.getName() + " GROUP BY a_varchar")).isFullyPushedDown();
            assertThat(query(session, "SELECT DISTINCT a_char, count(a_varchar) FROM " + emptyTable.getName() + " GROUP BY a_char")).isFullyPushedDown();
        }

        Session withMarkDistinct = sessionWithDistinctAggregationsStrategy(session, "mark_distinct");
        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // distinct aggregation with varchar with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT comment) FROM nation GROUP BY comment")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation")).isFullyPushedDown();
        // two distinct aggregations with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT name), count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT name), max(comment) FROM nation")).isFullyPushedDown();

        Session withoutMarkDistinct = sessionWithDistinctAggregationsStrategy(session, "single_step");
        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // distinct aggregation with varchar with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT comment) FROM nation GROUP BY comment")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation")).isFullyPushedDown();
        // two distinct aggregations with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT name), count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT name), max(comment) FROM nation")).isFullyPushedDown();
    }

    @Test
    public void testAggregationPushdownWithStringTypes()
    {
        List<String> rows = Stream.of(null, "a", "b", "A", "B", " a ", "a", "b", " b ", "ą")
                .map(value -> value == null ? "NULL, NULL" : format("'%1$s', '%1$s'", value))
                .collect(toList());

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "strings", "(t_char CHAR(5), t_varchar VARCHAR(5))", rows)) {
            Session session = stringPushdownWithBinaryEnabled(getSession());
            assertThat(query(session, "SELECT count(t_varchar), count(t_char), count(*) FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '9', BIGINT '9', BIGINT '10')")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT t_varchar, count(t_varchar), count(*) FROM " + testTable.getName() + " GROUP BY t_varchar"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(null, BIGINT '0', BIGINT '1')," +
                            "(VARCHAR 'a', BIGINT '2', BIGINT '2')," +
                            "(VARCHAR 'b', BIGINT '2', BIGINT '2')," +
                            "(VARCHAR 'A', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR 'B', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR ' a ', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR ' b ', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR 'ą', BIGINT '1', BIGINT '1')")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT t_char, count(t_char), count(*) FROM " + testTable.getName() + " GROUP BY t_char"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(null, BIGINT '0', BIGINT '1')," +
                            "(VARCHAR 'a    ', BIGINT '2', BIGINT '2')," +
                            "(VARCHAR 'b    ', BIGINT '2', BIGINT '2')," +
                            "(VARCHAR 'A    ', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR 'B    ', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR ' a   ', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR ' b   ', BIGINT '1', BIGINT '1')," +
                            "(VARCHAR 'ą    ', BIGINT '1', BIGINT '1')")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT count(t_varchar) FROM " + testTable.getName() + " WHERE t_varchar = 'a' GROUP BY t_varchar"))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT count(t_char) FROM " + testTable.getName() + " WHERE t_char = 'a' GROUP BY t_char"))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT min(t_varchar), min(t_char), max(t_varchar), max(t_char) FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (VARCHAR ' a ', VARCHAR ' a   ', VARCHAR 'ą', VARCHAR 'ą    ')")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT t_varchar, min(t_varchar), max(t_varchar) FROM " + testTable.getName() + " GROUP BY t_varchar"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(null, null, null)," +
                            "(VARCHAR 'a', VARCHAR 'a', VARCHAR 'a')," +
                            "(VARCHAR 'b', VARCHAR 'b', VARCHAR 'b')," +
                            "(VARCHAR 'A', VARCHAR 'A', VARCHAR 'A')," +
                            "(VARCHAR 'B', VARCHAR 'B', VARCHAR 'B')," +
                            "(VARCHAR ' a ', VARCHAR ' a ', VARCHAR ' a ')," +
                            "(VARCHAR ' b ', VARCHAR ' b ', VARCHAR ' b ')," +
                            "(VARCHAR 'ą', VARCHAR 'ą', VARCHAR 'ą')")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT t_char, min(t_char), max(t_char) FROM " + testTable.getName() + " GROUP BY t_char"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(null, null, null)," +
                            "(VARCHAR 'a    ', VARCHAR 'a    ', VARCHAR 'a    ')," +
                            "(VARCHAR 'b    ', VARCHAR 'b    ', VARCHAR 'b    ')," +
                            "(VARCHAR 'A    ', VARCHAR 'A    ', VARCHAR 'A    ')," +
                            "(VARCHAR 'B    ', VARCHAR 'B    ', VARCHAR 'B    ')," +
                            "(VARCHAR ' a   ', VARCHAR ' a   ', VARCHAR ' a   ')," +
                            "(VARCHAR ' b   ', VARCHAR ' b   ', VARCHAR ' b   ')," +
                            "(VARCHAR 'ą    ', VARCHAR 'ą    ', VARCHAR 'ą    ')")
                    .isFullyPushedDown();

            assertThat(query(session, "SELECT min(t_varchar), max(t_varchar) FROM " + testTable.getName() + " WHERE t_varchar = 'a' GROUP BY t_varchar"))
                    .skippingTypesCheck()
                    .matches("VALUES (VARCHAR 'a', VARCHAR 'a')")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT min(t_char), max(t_char) FROM " + testTable.getName() + " WHERE t_char = 'a' GROUP BY t_char"))
                    .skippingTypesCheck()
                    .matches("VALUES (VARCHAR 'a    ', VARCHAR 'a    ')")
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override
    public void testCaseSensitiveAggregationPushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_cs_agg_pushdown",
                "(a_varchar VARCHAR(2) COLLATE utf8mb4_general_ci, a_char CHAR(2) COLLATE utf8mb4_general_ci, a_bigint bigint)",
                ImmutableList.of(
                        "'A', 'A', 1",
                        "'B', 'B', 1",
                        "'a', 'a', 3",
                        "'b', 'b', 4",
                        "'a ', 'a ', 5",
                        "'aA', 'aA', 6"))) {
            Session session = stringPushdownWithBinaryEnabled(getSession());
            // case-sensitive functions
            assertThat(query(session, "SELECT max(a_varchar), min(a_varchar), max(a_char), min(a_char) FROM " + table.getName()))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES ('b', 'A', 'b ', 'A ')"); // char is padded with spaces
            // distinct over case-sensitive column
            assertThat(query(session, "SELECT distinct a_varchar FROM " + table.getName()))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES 'A', 'B', 'a', 'b', 'aA', 'a '");
            assertThat(query(session, "SELECT distinct a_char FROM " + table.getName()))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES 'A ', 'B ', 'a ', 'b ', 'aA'"); // char is padded with spaces
            // case-sensitive grouping sets
            assertThat(query(session, "SELECT a_varchar, count(*) FROM " + table.getName() + " GROUP BY a_varchar"))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES ('A', BIGINT '1'), ('a', BIGINT '1'), ('b', BIGINT '1'), ('B', BIGINT '1'), ('a ', BIGINT '1'), ('aA', BIGINT '1')");
            assertThat(query(session, "SELECT a_char, count(*) FROM " + table.getName() + " GROUP BY a_char"))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES ('A ', BIGINT '1'), ('B ', BIGINT '1'), ('a ', BIGINT '2'), ('b ', BIGINT '1'), ('aA', BIGINT '1')");

            // case-insensitive functions with case-insensitive grouping sets
            assertThat(query(session, "SELECT count(a_varchar), count(a_char) FROM " + table.getName())).isFullyPushedDown();
            assertThat(query(session, "SELECT count(a_varchar), count(a_char) FROM " + table.getName() + " GROUP BY a_bigint")).isFullyPushedDown();

            // aggregation and filtering on the same column varchar
            assertThat(query(
                    session,
                    """
                    SELECT a_varchar, COUNT(*)
                    FROM  %s
                    WHERE a_varchar = 'a'
                    GROUP BY a_varchar
                    """.formatted(table.getName())))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES (VARCHAR 'a', BIGINT '1')");

            // aggregation and filtering on the same column char
            assertThat(query(
                    session,
                    """
                    SELECT a_char, COUNT(*)
                    FROM  %s
                    WHERE a_char = 'a'
                    GROUP BY a_char
                    """.formatted(table.getName())))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES (CHAR 'a ', BIGINT '2')"); // char is padded with spaces so 'a' and 'a ' are same

            // aggregation and filtering on the same column char with alias, trino does not allow alias in GROUP BY
            assertThat(query(
                    session,
                    """
                    SELECT a_char AS a_alias, COUNT(*)
                    FROM  %s
                    WHERE a_char = 'a'
                    GROUP BY a_char
                    """.formatted(table.getName())))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES (CHAR 'a ', BIGINT '2')");

            // DISTINCT over case-sensitive columns
            assertThat(query(session, "SELECT count(DISTINCT a_varchar) FROM " + table.getName()))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES BIGINT '6'");
            assertThat(query(session, "SELECT count(DISTINCT a_char) FROM " + table.getName()))
                    .isFullyPushedDown()
                    .skippingTypesCheck()
                    .matches("VALUES BIGINT '5'"); // char is padded with spaces so 'a' and 'a ' are same

            // multiple grouping sets are not pushed down by optimizer
            assertThat(query(
                    session,
                    """
                    SELECT
                        a_varchar,
                        a_char,
                        SUM(a_bigint) AS total_sum
                    FROM %s
                    GROUP BY GROUPING SETS (
                        (a_varchar, a_char), -- Group by both columns
                        (a_varchar),         -- Group by `a_varchar` only
                        (a_char),            -- Group by `a_char` only
                        ()                   -- Grand total (no grouping)
                    )
                    """.formatted(table.getName())))
                    .isNotFullyPushedDown(GroupIdNode.class);

            verifyMultipleDistinctPushdown(sessionWithDistinctAggregationsStrategy(session, "mark_distinct"), table);
            verifyMultipleDistinctPushdown(sessionWithDistinctAggregationsStrategy(session, "single_step"), table);
            verifyMultipleDistinctPushdown(sessionWithDistinctAggregationsStrategy(session, "pre_aggregate"), table);
        }
    }

    private void verifyMultipleDistinctPushdown(Session session, TestTable table)
    {
        assertThat(query(session, "SELECT count(DISTINCT a_varchar), count(DISTINCT a_bigint) FROM " + table.getName()))
                .isFullyPushedDown()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '6', BIGINT '5')");

        assertThat(query(session, "SELECT count(DISTINCT a_char), count(DISTINCT a_bigint) FROM " + table.getName()))
                .isFullyPushedDown()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '5', BIGINT '5')");

        assertThat(query(session, "SELECT count(DISTINCT a_varchar), sum(DISTINCT a_bigint) FROM " + table.getName()))
                .isFullyPushedDown()
                .skippingTypesCheck()
                .matches(sumDistinctAggregationPushdownExpectedResult());

        assertThat(query(session, "SELECT count(DISTINCT a_char), sum(DISTINCT a_bigint) FROM " + table.getName()))
                .isFullyPushedDown()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '5', BIGINT '19')"); // char is padded with spaces so 'a' and 'a ' are same
    }

    @Override
    protected boolean expectDateLiteralJoinPushdown(JoinOperator joinOperator)
    {
        // In SingleStore connector we added Date constants rewrite for predicates.
        // It makes `SELECT n.name FROM nation n %s orders o ON DATE '2025-03-19' = o.orderdate` from BaseJdbcConnectorTest.testJoinPushdown to be pushdown for RIGHT join.
        // Other join types after reordering/optimizations are treated as cross join and are not pushdown by PushJoinIntoTableScan rule.
        return joinOperator == RIGHT_JOIN;
    }

    private static Session sessionWithDistinctAggregationsStrategy(Session session, String strategyValue)
    {
        return Session.builder(session)
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, strategyValue)
                .build();
    }

    private static Session stringPushdownWithBinaryEnabled(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("singlestore", "enable_string_pushdown_with_binary", "true")
                .build();
    }

    @Override
    protected String sumDistinctAggregationPushdownExpectedResult()
    {
        return "VALUES (BIGINT '6', BIGINT '19')";
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(62);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        // The error message says 60 char, but the actual limitation is 62
        assertThat(e).hasMessageContaining("Distributed SingleStore requires the length of the database name to be at most 60 characters");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(256);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Object name cannot have more than 256 characters");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(256);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*Identifier name '.*' is too long");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return singleStoreServer::execute;
    }
}
