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
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestSingleStoreCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;
    private TestingSingleStoreServer singleStoreServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        singleStoreServer = closeAfterClass(new TestingSingleStoreServer());
        return SingleStoreQueryRunner.builder(singleStoreServer)
                .addConnectorProperty("join-pushdown.enabled", "true")
                .addConnectorProperty("singlestore.experimental.enable-string-pushdown-with-binary", "true")
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return singleStoreServer::execute;
    }

    @BeforeAll
    void setup()
    {
        // in both tables values used in supported/unsupported test cases should follow schema:
        // <matching value in both tables> <not matching value in both tables> <null>
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "integer", asList(11, 12, 13))
                .addColumn("c_varchar_5", "varchar(5)", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_10", "varchar(10)", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_8192", "varchar(8192)", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_8193", "varchar(8193)", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_10000", "varchar(10000)", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_tinytext", "tinytext", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_text", "text", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_mediumtext", "mediumtext", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_longtext", "longtext", asList("'abcde'", "'12345'", null))
                .addColumn("c_varchar_unicode", "varchar(50)", asList("'こんにちは世界'", "'😂'", null))

                .addColumn("c_date", "date", asList("'2024-09-08'", "'2019-08-15'", null))
                .addColumn("c_datetime", "datetime", asList("'2024-09-08 12:15:23'", "'2019-08-15 09:45:12'", null))
                .addColumn("c_datetime_0", "datetime(0)", asList("'2024-09-08 12:15:23'", "'2019-08-15 09:45:12'", null))
                .addColumn("c_datetime_6", "datetime(6)", asList("'2024-09-08 12:15:23.123123'", "'2019-08-15 09:45:12.987654'", null))
                .addColumn("c_timestamp", "timestamp", asList("'2024-09-08 12:15:23'", "'2019-08-15 09:45:12'", null)) // timestamp is alias for timestamp(0) in SingleStore
                .addColumn("c_timestamp_0", "timestamp(0)", asList("'2024-09-08 12:15:23'", "'2019-08-15 09:45:12'", null))
                .addColumn("c_timestamp_6", "timestamp(6)", asList("'2024-09-08 12:15:23.123123'", "'2019-08-15 09:45:12.987654'", null))
                .execute(onRemoteDatabase(), "tpch.left_table_"));

        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "integer", asList(21, 22, 23))
                .addColumn("c_varchar_5", "varchar(5)", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_10", "varchar(10)", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_8192", "varchar(8192)", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_8193", "varchar(8193)", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_10000", "varchar(10000)", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_tinytext", "longtext", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_text", "longtext", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_mediumtext", "longtext", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_longtext", "longtext", asList("'abcde'", "'54321'", null))
                .addColumn("c_varchar_unicode", "varchar(50)", asList("'こんにちは世界'", "'😇'", null))

                .addColumn("c_date", "date", asList("'2024-09-08'", "'2020-01-01'", null))
                .addColumn("c_datetime", "datetime", asList("'2024-09-08 12:15:23'", "'2020-01-01 12:00:00'", null))
                .addColumn("c_datetime_0", "datetime(0)", asList("'2024-09-08 12:15:23'", "'2020-01-01 12:00:00'", null))
                .addColumn("c_datetime_zero_time_0", "datetime(0)", asList("'2024-09-08 00:00:00'", "'2020-01-01 12:00:00'", null))
                .addColumn("c_datetime_6", "datetime(6)", asList("'2024-09-08 12:15:23.123123'", "'2020-01-01 12:00:00.000000'", null))
                .addColumn("c_datetime_zero_millis_6", "datetime(6)", asList("'2024-09-08 12:15:23.000000'", "'2020-01-01 12:00:00.000000'", null))
                .addColumn("c_datetime_zero_time_6", "datetime(6)", asList("'2024-09-08 00:00:00.000000'", "'2020-01-01 12:00:00.000000'", null))
                .addColumn("c_timestamp", "timestamp(0)", asList("'2024-09-08 12:15:23'", "'2020-01-01 12:00:00'", null))
                .addColumn("c_timestamp_0", "timestamp(0)", asList("'2024-09-08 12:15:23'", "'2020-01-01 12:00:00'", null))
                .addColumn("c_timestamp_zero_time_0", "timestamp(0)", asList("'2024-09-08 00:00:00'", "'2020-01-01 12:00:00'", null))
                .addColumn("c_timestamp_6", "timestamp(6)", asList("'2024-09-08 12:15:23.123123'", "'2020-01-01 12:00:00.000000'", null))
                .addColumn("c_timestamp_zero_millis_6", "timestamp(6)", asList("'2024-09-08 12:15:23.000000'", "'2020-01-01 12:00:00.000000'", null))
                .addColumn("c_timestamp_zero_time_6", "timestamp(6)", asList("'2024-09-08 00:00:00.000000'", "'2020-01-01 12:00:00.000000'", null))
                .execute(onRemoteDatabase(), "tpch.right_table_"));
    }

    @Override
    protected String leftTable()
    {
        return left.getName();
    }

    @Override
    protected String rightTable()
    {
        return right.getName();
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_varchar_5", "varchar(5)", "c_varchar_5"))
                .add(new CastTestCase("c_varchar_5", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_5", "varchar(10000)", "c_varchar_10000"))
                .add(new CastTestCase("c_varchar_10000", "varchar(10000)", "c_varchar_10000"))
                .add(new CastTestCase("c_varchar_10000", "varchar(8192)", "c_varchar_8192"))
                .add(new CastTestCase("c_varchar_10000", "varchar(8193)", "c_varchar_8193"))
                .add(new CastTestCase("c_varchar_10000", "varchar(5)", "c_varchar_5"))
                .add(new CastTestCase("c_varchar_10", "varchar(5)", "c_varchar_5"))
                .add(new CastTestCase("c_varchar_tinytext", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_tinytext", "varchar(10000)", "c_varchar_10000"))
                .add(new CastTestCase("c_varchar_tinytext", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_varchar_text", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_text", "varchar(10000)", "c_varchar_10000"))
                .add(new CastTestCase("c_varchar_text", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_varchar_mediumtext", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_mediumtext", "varchar(10000)", "c_varchar_10000"))
                .add(new CastTestCase("c_varchar_mediumtext", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_varchar_longtext", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_longtext", "varchar(10000)", "c_varchar_10000"))
                .add(new CastTestCase("c_varchar_longtext", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_varchar_10", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_varchar_8192", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_varchar_unicode", "varchar(10)", "c_varchar_unicode"))
                .add(new CastTestCase("c_varchar_unicode", "varchar(10000)", "c_varchar_unicode"))
                .add(new CastTestCase("c_varchar_unicode", "varchar", "c_varchar_unicode"))
                .add(new CastTestCase("c_date", "date", "c_date"))
                .add(new CastTestCase("c_datetime", "date", "c_date"))
                .add(new CastTestCase("c_datetime_0", "date", "c_date"))
                .add(new CastTestCase("c_datetime_6", "date", "c_date"))
                .add(new CastTestCase("c_timestamp", "date", "c_date"))
                .add(new CastTestCase("c_timestamp_0", "date", "c_date"))
                .add(new CastTestCase("c_timestamp_6", "date", "c_date"))
                .add(new CastTestCase("c_date", "timestamp(0)", "c_datetime_zero_time_0"))
                .add(new CastTestCase("c_datetime", "timestamp(0)", "c_datetime_0"))
                .add(new CastTestCase("c_datetime_0", "timestamp(0)", "c_datetime_0"))
                .add(new CastTestCase("c_timestamp", "timestamp(0)", "c_timestamp_0"))
                .add(new CastTestCase("c_timestamp_0", "timestamp(0)", "c_timestamp_0"))
                .add(new CastTestCase("c_date", "timestamp", "c_datetime_zero_time_6")) // timestamp is an alias for timestamp(3) in Trino
                .add(new CastTestCase("c_datetime", "timestamp", "c_datetime_zero_millis_6")) // c_datetime is datetime(0) in SingleStore
                .add(new CastTestCase("c_datetime_0", "timestamp", "c_datetime_zero_millis_6"))
                .add(new CastTestCase("c_timestamp", "timestamp", "c_timestamp_zero_millis_6")) // c_timestamp is timestamp(0) in SingleStore
                .add(new CastTestCase("c_timestamp_0", "timestamp", "c_timestamp_zero_millis_6"))
                .add(new CastTestCase("c_date", "timestamp(3)", "c_datetime_zero_time_6"))
                .add(new CastTestCase("c_datetime", "timestamp(3)", "c_datetime_zero_millis_6"))
                .add(new CastTestCase("c_datetime_0", "timestamp(3)", "c_datetime_zero_millis_6"))
                .add(new CastTestCase("c_timestamp", "timestamp(3)", "c_timestamp_zero_millis_6"))
                .add(new CastTestCase("c_timestamp_0", "timestamp(3)", "c_timestamp_zero_millis_6"))
                .add(new CastTestCase("c_date", "timestamp(6)", "c_datetime_zero_time_6"))
                .add(new CastTestCase("c_datetime", "timestamp(6)", "c_datetime_zero_millis_6"))
                .add(new CastTestCase("c_datetime_0", "timestamp(6)", "c_datetime_zero_millis_6"))
                .add(new CastTestCase("c_timestamp", "timestamp(6)", "c_timestamp_zero_millis_6"))
                .add(new CastTestCase("c_timestamp_0", "timestamp(6)", "c_timestamp_zero_millis_6"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_datetime_6", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_datetime_6", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_timestamp_6", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_timestamp_6", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("c_date", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_date", "varchar", "c_varchar_longtext"))
                .add(new CastTestCase("id", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("id", "varchar", "c_varchar_longtext"))
                // Trino rounds seconds based on microsecond part while SingleStore truncates microseconds
                .add(new CastTestCase("c_datetime_6", "timestamp(0)", "c_datetime_0"))
                .add(new CastTestCase("c_timestamp_6", "timestamp(0)", "c_timestamp_0"))
                // Casts to timestamp with precision higher than supported in SingleStore
                .add(new CastTestCase("c_date", "timestamp(7)", "c_timestamp_0"))
                .add(new CastTestCase("c_datetime_6", "timestamp(7)", "c_timestamp_0"))
                .add(new CastTestCase("c_timestamp_6", "timestamp(7)", "c_timestamp_0"))
                .build();
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.<InvalidCastTestCase>builder()
                .add(new InvalidCastTestCase("id", "date"))
                .add(new InvalidCastTestCase("id", "timestamp(0)"))
                .add(new InvalidCastTestCase("id", "timestamp(6)"))
                .add(new InvalidCastTestCase("c_varchar_5", "int"))
                .add(new InvalidCastTestCase("c_varchar_longtext", "int"))
                .add(new InvalidCastTestCase("c_date", "int"))
                .add(new InvalidCastTestCase("c_datetime", "int"))
                .add(new InvalidCastTestCase("c_datetime_0", "int"))
                .add(new InvalidCastTestCase("c_datetime_6", "int"))
                .add(new InvalidCastTestCase("c_timestamp", "int"))
                .add(new InvalidCastTestCase("c_timestamp_0", "int"))
                .add(new InvalidCastTestCase("c_timestamp_6", "int"))
                .build();
    }

    @Test
    void testCastPushdownDisabled()
    {
        Session sessionWithoutComplexExpressionPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutComplexExpressionPushdown, "SELECT CAST(c_varchar_10 AS VARCHAR(100)) FROM %s".formatted(leftTable()))).isNotFullyPushedDown(ProjectNode.class);
        assertThat(query(sessionWithoutComplexExpressionPushdown, "SELECT CAST(c_varchar_10 AS VARCHAR) FROM %s".formatted(leftTable()))).isNotFullyPushedDown(ProjectNode.class);
        assertThat(query(sessionWithoutComplexExpressionPushdown, "SELECT CAST(c_varchar_longtext AS VARCHAR(100)) FROM %s".formatted(leftTable()))).isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testAllJoinPushdownWithCast()
    {
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .matches("VALUES 11, 12, 13")
                    .isFullyPushedDown();
            assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .matches("VALUES null, null, 11")
                    .isFullyPushedDown();
            assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .matches("VALUES 11")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testJoinPushdownWithNestedCast()
    {
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_varchar_10 AS VARCHAR(10)) AS VARCHAR(5)) = r.c_varchar_5".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_varchar_10 AS VARCHAR(10)) AS VARCHAR) = r.c_varchar_longtext".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_varchar_10 AS VARCHAR) AS VARCHAR(5)) = r.c_varchar_5".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_varchar_10 AS VARCHAR) AS VARCHAR) = r.c_varchar_longtext".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();

        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp AS DATE) AS DATE) = r.c_date".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp_0 AS DATE) AS DATE) = r.c_date".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp_6 AS DATE) AS DATE) = r.c_date".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_date AS TIMESTAMP(0)) AS DATE) = r.c_date".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_date AS TIMESTAMP(6)) AS DATE) = r.c_date".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();

        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp AS DATE) AS TIMESTAMP(0)) = r.c_timestamp_zero_time_0".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp AS DATE) AS TIMESTAMP(6)) = r.c_timestamp_zero_time_6".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp_0 AS DATE) AS TIMESTAMP(0)) = r.c_timestamp_zero_time_0".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp_0 AS DATE) AS TIMESTAMP(6)) = r.c_timestamp_zero_time_6".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp_6 AS DATE) AS TIMESTAMP(0)) = r.c_timestamp_zero_time_0".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_timestamp_6 AS DATE) AS TIMESTAMP(6)) = r.c_timestamp_zero_time_6".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();

        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_date AS TIMESTAMP(0)) AS TIMESTAMP(0)) = r.c_timestamp_zero_time_0".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_date AS TIMESTAMP(0)) AS TIMESTAMP(6)) = r.c_timestamp_zero_time_6".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.c_date AS TIMESTAMP(6)) AS TIMESTAMP(6)) = r.c_timestamp_zero_time_6".formatted(leftTable(), rightTable())))
                .isFullyPushedDown();
    }

    @Test
    void testTruncationBehavior()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_truncation_behavior",
                """
                (
                c_varchar_5 varchar(5),
                c_varchar_10000 varchar(10000),
                c_tinytext tinytext,
                c_text text,
                c_mediumtext mediumtext,
                c_longtext longtext
                )
                """,
                List.of(
                        "'%s', '%s', '%s', '%s', '%s', '%s'".formatted(
                                chars(5),
                                chars(10000),
                                chars(255),
                                chars(10000),
                                chars(10000),
                                chars(10000)),
                        "'%s', '%s', '%s', '%s', '%s', '%s'".formatted(
                                spaces(5),
                                spaces(10000),
                                spaces(255),
                                spaces(10000),
                                spaces(10000),
                                spaces(10000))))) {
            for (String columnName : List.of(
                    "c_varchar_5",
                    "c_varchar_10000",
                    "c_tinytext",
                    "c_text",
                    "c_mediumtext",
                    "c_longtext")) {
                assertThat(query("SELECT CAST(%s AS VARCHAR(5)) FROM %s".formatted(columnName, table.getName()))).isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR(8192)) FROM %s".formatted(columnName, table.getName()))).isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR(8193)) FROM %s".formatted(columnName, table.getName()))).isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR(9000)) FROM %s".formatted(columnName, table.getName()))).isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR) FROM %s".formatted(columnName, table.getName()))).isFullyPushedDown();
            }
        }
    }

    private static String spaces(int count)
    {
        return " ".repeat(count);
    }

    private static String chars(int count)
    {
        return "a".repeat(count);
    }

    @Test
    void testUnicodeCharacterHandling()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_unicode",
                """
                (
                c_varchar_12000 VARCHAR(12000),
                c_tinytext TINYTEXT,
                c_text TEXT,
                c_mediumtext MEDIUMTEXT,
                c_longtext LONGTEXT
                )
                """,
                List.of(
                        "'%s', '%s', '%s', '%s', '%s'".formatted("😂".repeat(12000), "😂".repeat(50), "😂".repeat(12000), "😂".repeat(12000), "😂".repeat(12000)), // emoji 😂 is 4 bytes long
                        "'こんにちは世界', 'こんにちは世界', 'こんにちは世界', 'こんにちは世界', 'こんにちは世界'"))) {
            assertThat(query("SELECT CAST(%s AS VARCHAR(5)) FROM %s".formatted("c_tinytext", table.getName())))
                    .matches("VALUES '😂😂😂😂😂', 'こんにちは'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(%s AS VARCHAR(250)) FROM %s".formatted("c_tinytext", table.getName())))
                    .matches("VALUES CAST('%s' AS VARCHAR(250)), CAST('こんにちは世界' AS VARCHAR(250))".formatted("😂".repeat(50)))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(%s AS VARCHAR(10000)) FROM %s".formatted("c_tinytext", table.getName())))
                    .matches("VALUES CAST('%s' AS VARCHAR(10000)), CAST('こんにちは世界' AS VARCHAR(10000))".formatted("😂".repeat(50)))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(%s AS VARCHAR) FROM %s".formatted("c_tinytext", table.getName())))
                    .matches("VALUES CAST('%s' AS VARCHAR), CAST('こんにちは世界' AS VARCHAR)".formatted("😂".repeat(50)))
                    .isFullyPushedDown();

            for (String columnName : List.of("c_varchar_12000", "c_text", "c_mediumtext", "c_longtext")) {
                assertThat(query("SELECT CAST(%s AS VARCHAR(5)) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES '😂😂😂😂😂', 'こんにちは'")
                        .isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR(250)) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES '%s', CAST('こんにちは世界' AS VARCHAR(250))".formatted("😂".repeat(250)))
                        .isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR(10000)) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES '%s', CAST('こんにちは世界' AS VARCHAR(10000))".formatted("😂".repeat(10000)))
                        .isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES CAST('%s' AS VARCHAR), CAST('こんにちは世界' AS VARCHAR)".formatted("😂".repeat(12000)))
                        .isFullyPushedDown();
            }
        }
    }

    @Test
    void testEmptyStringHandling()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_empty_string_handling",
                """
                (
                c_varchar varchar(5),
                c_tinytext tinytext,
                c_text text,
                c_mediumtext mediumtext,
                c_longtext longtext
                )
                """,
                List.of(
                        "'', '', '', '', ''"))) {
            for (String columnName : List.of("c_varchar", "c_tinytext", "c_text", "c_mediumtext", "c_longtext")) {
                assertThat(query("SELECT CAST(%s AS VARCHAR(50)) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES CAST('' AS VARCHAR(50))")
                        .isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR(10000)) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES CAST('' AS VARCHAR(10000))")
                        .isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS VARCHAR) FROM %s".formatted(columnName, table.getName())))
                        .matches("VALUES CAST('' AS VARCHAR)")
                        .isFullyPushedDown();
            }
        }
    }

    @Test
    void testVarcharCastPushdownUsingTableCreatedInTrino()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_varchar_cast_with_trino_created_table",
                """
                (
                c_varchar_5 varchar(5),
                c_varchar_10 varchar(10),
                c_varchar_max varchar(21844),
                c_varchar varchar
                )
                """,
                List.of(
                        "'abc', 'abc', 'abc', 'abc'",
                        "'😂', '😂', '😂', '😂'",
                        "'', '', '', ''",
                        "null, null, null, null"))) {
            assertThat(query("SELECT CAST(c_varchar_5 AS varchar(10)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR(10)), CAST('😂' AS VARCHAR(10)), CAST('' AS VARCHAR(10)), CAST(null AS VARCHAR(10))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar_10 AS varchar(5)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR(5)), CAST('😂' AS VARCHAR(5)), CAST('' AS VARCHAR(5)), CAST(null AS VARCHAR(5))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar_10 AS varchar(21844)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR(21844)), CAST('😂' AS VARCHAR(21844)), CAST('' AS VARCHAR(21844)), CAST(null AS VARCHAR(21844))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar_10 AS varchar) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR), CAST('😂' AS VARCHAR), CAST('' AS VARCHAR), CAST(null AS VARCHAR)")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar_max AS varchar(10)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR(10)), CAST('😂' AS VARCHAR(10)), CAST('' AS VARCHAR(10)), CAST(null AS VARCHAR(10))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar_max AS varchar) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR), CAST('😂' AS VARCHAR), CAST('' AS VARCHAR), CAST(null AS VARCHAR)")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar AS varchar(10)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR(10)), CAST('😂' AS VARCHAR(10)), CAST('' AS VARCHAR(10)), CAST(null AS VARCHAR(10))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_varchar AS varchar(21844)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES CAST('abc' AS VARCHAR(21844)), CAST('😂' AS VARCHAR(21844)), CAST('' AS VARCHAR(21844)), CAST(null AS VARCHAR(21844))")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testDateRelatedCastPushdownUsingTableCreatedInTrino()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_date_related_cast_with_trino_created_table",
                """
                (
                c_date date,
                c_timestamp_0 timestamp(0),
                c_timestamp timestamp, -- becomes datetime(6) in SingleStore table
                c_timestamp_3 timestamp(3), -- becomes datetime(6) in SingleStore table
                c_timestamp_6 timestamp(6)
                )
                """,
                List.of(
                        "DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP '9999-12-31 23:59:59.999', TIMESTAMP '9999-12-31 23:59:59.999', TIMESTAMP '9999-12-31 23:59:59.999999'",
                        "DATE '2019-08-15', TIMESTAMP '2019-08-15 09:45:12', TIMESTAMP '2019-08-15 09:45:12.987', TIMESTAMP '2019-08-15 09:45:12.987', TIMESTAMP '2019-08-15 09:45:12.987654'",
                        "DATE '1000-01-01', TIMESTAMP '1000-01-01 00:00:00', TIMESTAMP '1000-01-01 00:00:00.000', TIMESTAMP '1000-01-01 00:00:00.000', TIMESTAMP '1000-01-01 00:00:00.000000'",
                        "null, null, null, null, null"))) {
            assertThat(query("SELECT CAST(c_date AS date) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '2019-08-15', DATE '1000-01-01', CAST(null AS DATE)")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS date) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '2019-08-15', DATE '1000-01-01', CAST(null AS DATE)")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp AS date) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '2019-08-15', DATE '1000-01-01', CAST(null AS DATE)")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_3 AS date) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '2019-08-15', DATE '1000-01-01', CAST(null AS DATE)")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_6 AS date) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '2019-08-15', DATE '1000-01-01', CAST(null AS DATE)")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date AS timestamp(0)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 00:00:00', TIMESTAMP '2019-08-15 00:00:00', TIMESTAMP '1000-01-01 00:00:00', CAST(null AS TIMESTAMP(0))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS timestamp(0)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP '2019-08-15 09:45:12', TIMESTAMP '1000-01-01 00:00:00', CAST(null AS TIMESTAMP(0))")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date AS timestamp) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 00:00:00.000', TIMESTAMP '2019-08-15 00:00:00.000', TIMESTAMP '1000-01-01 00:00:00.000', CAST(null AS TIMESTAMP(3))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS timestamp) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.000', TIMESTAMP '2019-08-15 09:45:12.000', TIMESTAMP '1000-01-01 00:00:00.000', CAST(null AS TIMESTAMP(3))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp AS timestamp) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.999', TIMESTAMP '2019-08-15 09:45:12.987', TIMESTAMP '1000-01-01 00:00:00.000', CAST(null AS TIMESTAMP(3))")
                    .isNotFullyPushedDown(ProjectNode.class); // timestamp(6) -> timestamp(3) downcast
            assertThat(query("SELECT CAST(c_timestamp_3 AS timestamp) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.999', TIMESTAMP '2019-08-15 09:45:12.987', TIMESTAMP '1000-01-01 00:00:00.000', CAST(null AS TIMESTAMP(3))")
                    .isNotFullyPushedDown(ProjectNode.class); // timestamp(6) -> timestamp(3) downcast

            assertThat(query("SELECT CAST(c_date AS timestamp(6)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 00:00:00.000000', TIMESTAMP '2019-08-15 00:00:00.000000', TIMESTAMP '1000-01-01 00:00:00.000000', CAST(null AS TIMESTAMP(6))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS timestamp(6)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.000000', TIMESTAMP '2019-08-15 09:45:12.000000', TIMESTAMP '1000-01-01 00:00:00.000000', CAST(null AS TIMESTAMP(6))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp AS timestamp(6)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.999000', TIMESTAMP '2019-08-15 09:45:12.987000', TIMESTAMP '1000-01-01 00:00:00.000000', CAST(null AS TIMESTAMP(6))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_3 AS timestamp(6)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.999000', TIMESTAMP '2019-08-15 09:45:12.987000', TIMESTAMP '1000-01-01 00:00:00.000000', CAST(null AS TIMESTAMP(6))")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_6 AS timestamp(6)) FROM %s".formatted(testTable.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.999999', TIMESTAMP '2019-08-15 09:45:12.987654', TIMESTAMP '1000-01-01 00:00:00.000000', CAST(null AS TIMESTAMP(6))")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testCastsOnMinMaxDatetimeRanges()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_datetime_min_max_casts",
                """
                (
                c_date date,
                c_datetime datetime,
                c_datetime_0 datetime(0),
                c_datetime_6 datetime(6),
                c_timestamp timestamp,
                c_timestamp_0 timestamp(0),
                c_timestamp_6 timestamp(6)
                )
                """,
                List.of(
                        "'9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999999', '2038-01-19 03:14:07', '2038-01-19 03:14:07', '2038-01-19 03:14:07.999999'",
                        "'1000-01-01', '1000-01-01 00:00:00', '1000-01-01 00:00:00', '1000-01-01 00:00:00.000000', '1970-01-01 00:00:01', '1970-01-01 00:00:01', '1970-01-01 00:00:01.000000'"))) {
            assertThat(query("SELECT CAST(c_timestamp AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '2038-01-19', DATE '1970-01-01'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '2038-01-19', DATE '1970-01-01'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_6 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '2038-01-19', DATE '1970-01-01'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '1000-01-01'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime_0 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '1000-01-01'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime_6 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '9999-12-31', DATE '1000-01-01'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 00:00:00', TIMESTAMP '1000-01-01 00:00:00'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP '1000-01-01 00:00:00'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime_0 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP '1000-01-01 00:00:00'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2038-01-19 03:14:07', TIMESTAMP '1970-01-01 00:00:01'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2038-01-19 03:14:07', TIMESTAMP '1970-01-01 00:00:01'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 00:00:00.000000', TIMESTAMP '1000-01-01 00:00:00.000000'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.000000', TIMESTAMP '1000-01-01 00:00:00.000000'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime_0 AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.000000', TIMESTAMP '1000-01-01 00:00:00.000000'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_datetime_6 AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '9999-12-31 23:59:59.999999', TIMESTAMP '1000-01-01 00:00:00.000000'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2038-01-19 03:14:07.000000', TIMESTAMP '1970-01-01 00:00:01.000000'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_0 AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2038-01-19 03:14:07.000000', TIMESTAMP '1970-01-01 00:00:01.000000'")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_timestamp_6 AS timestamp(6)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2038-01-19 03:14:07.999999', TIMESTAMP '1970-01-01 00:00:01.000000'")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testDateCastInGroupByAndAggregation()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.single_store_aggregation_on_casted_date",
                """
                (
                c_date date,
                c_datetime datetime,
                c_datetime_0 datetime(0),
                c_datetime_6 datetime(6),
                c_timestamp timestamp,
                c_timestamp_0 timestamp(0),
                c_timestamp_6 timestamp(6)
                )
                """,
                List.of(
                        "null, null, null, null, null, null, null",
                        "'2024-09-08', '2024-09-08 00:00:00', '2024-09-08 00:00:00', '2024-09-08 00:00:00.123456', '2024-09-08 00:00:00', '2024-09-08 00:00:00', '2024-09-08 00:00:00.123456'",
                        "'2024-09-08', '2024-09-08 14:18:03', '2024-09-08 14:18:03', '2024-09-08 14:18:03.123456', '2024-09-08 14:18:03', '2024-09-08 14:18:03', '2024-09-08 14:18:03.123456'",
                        "'2024-09-09', '2024-09-09 00:00:00', '2024-09-09 00:00:00', '2024-09-09 00:00:00.123456', '2024-09-09 00:00:00', '2024-09-09 00:00:00', '2024-09-09 00:00:00.123456'",
                        "'2024-09-09', '2024-09-09 14:18:03', '2024-09-09 14:18:03', '2024-09-09 14:18:03.123456', '2024-09-09 14:18:03', '2024-09-09 14:18:03', '2024-09-09 14:18:03.123456'",
                        "'9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999999', '2038-01-19 03:14:07', '2038-01-19 03:14:07', '2038-01-19 03:14:07.999999'",
                        "'1000-01-01', '1000-01-01 00:00:00', '1000-01-01 00:00:00', '1000-01-01 00:00:00.000000', '1970-01-01 00:00:01', '1970-01-01 00:00:01', '1970-01-01 00:00:01.000000'"))) {
            assertThat(query("SELECT DATE(c_date), count(*) FROM %s GROUP BY DATE(c_date)".formatted(table.getName())))
                    .matches("VALUES (DATE '1000-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '9999-12-31', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
            assertThat(query("SELECT DATE(c_datetime), count(*) FROM %s GROUP BY DATE(c_datetime)".formatted(table.getName())))
                    .matches("VALUES (DATE '1000-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '9999-12-31', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
            assertThat(query("SELECT DATE(c_datetime_0), count(*) FROM %s GROUP BY DATE(c_datetime_0)".formatted(table.getName())))
                    .matches("VALUES (DATE '1000-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '9999-12-31', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
            assertThat(query("SELECT DATE(c_datetime_6), count(*) FROM %s GROUP BY DATE(c_datetime_6)".formatted(table.getName())))
                    .matches("VALUES (DATE '1000-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '9999-12-31', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
            assertThat(query("SELECT DATE(c_timestamp), count(*) FROM %s GROUP BY DATE(c_timestamp)".formatted(table.getName())))
                    .matches("VALUES (DATE '1970-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '2038-01-19', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
            assertThat(query("SELECT DATE(c_timestamp_0), count(*) FROM %s GROUP BY DATE(c_timestamp_0)".formatted(table.getName())))
                    .matches("VALUES (DATE '1970-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '2038-01-19', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
            assertThat(query("SELECT DATE(c_timestamp_6), count(*) FROM %s GROUP BY DATE(c_timestamp_6)".formatted(table.getName())))
                    .matches("VALUES (DATE '1970-01-01', bigint '1'), (DATE '2024-09-08', bigint '2'), (DATE '2024-09-09', bigint '2'), (DATE '2038-01-19', bigint '1'), (DATE(null), bigint '1')")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testTimestampRoundingWhileDowncast()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.timestamp_downcast_projection",
                """
                (
                c_datetime_6 datetime(6),
                c_timestamp_6 timestamp(6)
                )
                """,
                List.of(
                        "'2024-09-08 12:15:23.500000', '2024-09-08 12:15:23.500000'",
                        "'2024-09-08 12:15:23.499999', '2024-09-08 12:15:23.499999'",
                        "'9999-12-31 23:59:59.999999', '2038-01-19 03:14:07.999999'"))) {
            // This test show why we don't pushdown timestamp/datetime downcasts (at least for now).
            // Trino rounds when downcasting: .500000 rounds up to 12:15:24, .499999 rounds down to 12:15:23
            // SingleStore during casting would truncate both to 12:15:23

            // We could workaround downcasting timestamp(6) to timestamp(0) by using `FROM_UNIXTIME(ROUND(UNIX_TIMESTAMP(c_timestamp_6)))` but:
            // * it works only for timestamp range, not for datetime range (results in null)
            // * returning millis part in UNIX_TIMESTAMP is undocumented feature
            // * using such a approach would result in null in case of downcasting max supported timestamp in SingleStore
            //   2038-01-19 03:14:07.999999 is rounded to 2038-01-19 03:14:08 which is above supported range of FROM_UNIXTIME and timestamp(0)

            assertThat(query("SELECT CAST(c_datetime_6 AS TIMESTAMP(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2024-09-08 12:15:24', TIMESTAMP '2024-09-08 12:15:23', TIMESTAMP '10000-01-01 00:00:00'")
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT CAST(c_timestamp_6 AS TIMESTAMP(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '2024-09-08 12:15:24', TIMESTAMP '2024-09-08 12:15:23', TIMESTAMP '2038-01-19 03:14:08'")
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }
}
