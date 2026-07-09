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
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestParallelSnowflakeCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private String testDbName;

    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestDatabase testDb = closeAfterClass(SnowflakeServer.createTestDatabase());
        testDbName = testDb.getName();
        return SnowflakeQueryRunner.parallelBuilder()
                .withDatabase(Optional.of(testDbName))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(ImmutableMap.of(
                        "jdbc-types-mapped-to-varchar", "c_boolean",
                        "join-pushdown.enabled", "true"))
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sql -> SnowflakeServer.safeExecuteOnDatabase(testDbName, sql);
    }

    @BeforeAll
    public void setup()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "number(10)", asList(11, 12, 13))

                .addColumn("c_boolean", "boolean", asList("'TRUE'", "'FALSE'", null))
                // All these types map to NUMBER(38,0) by default in Snowflake:
                // C_TINYINT -> NUMBER(38,0)
                // C_SMALLINT-> NUMBER(38,0)
                // C_INTEGER -> NUMBER(38,0)
                // C_BIGINT  -> NUMBER(38,0)
                .addColumn("c_tinyint", "tinyint", asList(11, 12, 13))
                .addColumn("c_smallint", "smallint", asList(11, 12, 13))
                .addColumn("c_integer", "integer", asList(11, 12, 13))
                .addColumn("c_bigint", "bigint", asList(11, 12, 13))

                .addColumn("c_decimal_3_0", "decimal(3, 0)", asList(1, 2, null))
                .addColumn("c_decimal_38_5", "decimal(38, 5)", asList(1.11, 2.22, null))
                .addColumn("c_decimal_38_0", "decimal(38, 0)", asList(1, 2, null))
                .addColumn("c_decimal_38_37", "decimal(38, 37)", asList(1.1232, 2.12313, null))
                .addColumn("c_numeric_3_0", "numeric(3, 0)", asList(1, 2, null))
                .addColumn("c_numeric_38_5", "numeric(38, 5)", asList(1.11, 2.22, null))
                .addColumn("c_numeric_38_37", "numeric(38, 37)", asList(1.1232, 2.12313, null))
                .addColumn("c_number_3_0", "number(3, 0)", asList(1, 2, null))
                .addColumn("c_number_38_5", "number(38, 5)", asList(1.11, 2.22, null))
                .addColumn("c_number_38_37", "number(38, 37)", asList(1.1232, 2.12313, null))

                .addColumn("c_double", "double", asList(1.23, 2.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 2.67, null))
                .addColumn("c_real", "real", asList(1.23, 2.67, null))
                .addColumn("c_float", "float", asList(1.23, 2.67, null))
                .addColumn("c_float4", "float4", asList(1.23, 2.67, null))
                .addColumn("c_float8", "float8", asList(1.23, 2.67, null))
                // All these types map to VARCHAR in Snowflake:
                // C_VARCHAR_10       -> VARCHAR(10)
                // C_VARCHAR_16777216 -> VARCHAR(16777216)
                // C_TEXT             -> VARCHAR(16777216)
                // C_STRING           -> VARCHAR(16777216)
                // C_CHAR_10          -> VARCHAR(10)
                // C_CHAR_16777216    -> VARCHAR(16777216)
                // C_CHARACTER_10     -> VARCHAR(10)
                // C_NCHAR_10         -> VARCHAR(10)
                // etc.
                .addColumn("c_varchar_10", "varchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_20", "varchar(20)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_50", "varchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_16777216", "varchar(16777216)", asList("'India'", "'Poland'", null)) // Snowflake MAX_VARCHAR
                .addColumn("c_varchar_5_unicode", "varchar(5)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_varchar_50_unicode", "varchar(50)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_char_5_unicode", "char(5)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_char_50_unicode", "char(50)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_text", "text", asList("'India'", "'Poland'", null))
                .addColumn("c_string", "string", asList("'India'", "'Poland'", null))

                .addColumn("c_char_10", "char(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_16777216", "char(16777216)", asList("'India'", "'Poland'", null)) // Snowflake MAX_VARCHAR
                .addColumn("c_character_10", "character(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_nchar_10", "nchar(10)", asList("'India'", "'Poland'", null))

                .addColumn("c_varbinary", "varbinary", asList("X''", "X'000000000000'", null))
                .addColumn("c_binary", "binary", asList("X''", "X'000000000000'", null))
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2019-08-15'", null))
                .addColumn("c_time", "time", asList("TIME '00:13:42.000'", "TIME '11:01:17.100'", null))
                .addColumn("c_timestamp_3", "timestamp(3)", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_timestamp_6", "timestamp(6)", asList("TIMESTAMP '2024-09-08 01:02:03.666333'", "TIMESTAMP '2019-08-15 09:08:07.333666'", null))
                .addColumn("c_timestamp_9", "timestamp(9)", asList("TIMESTAMP '2024-09-08 01:02:03.666333111'", "TIMESTAMP '2019-08-15 09:08:07.333666111'", null))
                .addColumn("c_timestamptz", "timestamp_tz", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2019-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_timestampltz", "timestamp_ltz", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2019-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_timestampntz", "timestamp_ntz", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_datetime", "datetime", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .execute(onRemoteDatabase(), TEST_SCHEMA + "." + "left_table_"));

        // 2nd row right table value is different from left table
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "number(10)", asList(11, 22, 13))

                .addColumn("c_boolean", "boolean", asList("'TRUE'", "'FALSE'", null))
                // All these types map to NUMBER(38,0) by default in Snowflake:
                // C_TINYINT -> NUMBER(38,0)
                // C_SMALLINT-> NUMBER(38,0)
                // C_INTEGER -> NUMBER(38,0)
                // C_BIGINT  -> NUMBER(38,0)
                .addColumn("c_tinyint", "tinyint", asList(11, 22, 13))
                .addColumn("c_smallint", "smallint", asList(11, 22, 13))
                .addColumn("c_integer", "integer", asList(11, 22, 13))
                .addColumn("c_bigint", "bigint", asList(11, 22, 13))

                .addColumn("c_decimal_3_0", "decimal(3, 0)", asList(1, 2, null))
                .addColumn("c_decimal_38_5", "decimal(38, 5)", asList(1.11, 22.22, null))
                .addColumn("c_decimal_38_0", "decimal(38, 0)", asList(1, 22, null))
                .addColumn("c_decimal_38_37", "decimal(38, 37)", asList(1.1232, 2.212313, null))
                .addColumn("c_numeric_3_0", "numeric(3, 0)", asList(1, 22, null))
                .addColumn("c_numeric_38_5", "numeric(38, 5)", asList(1.11, 22.22, null))
                .addColumn("c_numeric_38_37", "numeric(38, 37)", asList(1.1232, 2.212313, null))
                .addColumn("c_number_3_0", "number(3, 0)", asList(1, 22, null))
                .addColumn("c_number_38_5", "number(38, 5)", asList(1.11, 22.22, null))
                .addColumn("c_number_38_37", "number(38, 37)", asList(1.1232, 2.212313, null))

                .addColumn("c_double", "double", asList(1.23, 22.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 22.67, null))
                .addColumn("c_real", "real", asList(1.23, 22.67, null))
                .addColumn("c_float", "float", asList(1.23, 22.67, null))
                .addColumn("c_float4", "float4", asList(1.23, 22.67, null))
                .addColumn("c_float8", "float8", asList(1.23, 22.67, null))
                // All these types map to VARCHAR in Snowflake:
                // C_VARCHAR_10       -> VARCHAR(10)
                // C_VARCHAR_16777216 -> VARCHAR(16777216)
                // C_TEXT             -> VARCHAR(16777216)
                // C_STRING           -> VARCHAR(16777216)
                // C_CHAR_10          -> VARCHAR(10)
                // C_CHAR_16777216    -> VARCHAR(16777216)
                // C_CHARACTER_10     -> VARCHAR(10)
                // C_NCHAR_10         -> VARCHAR(10)
                // etc.
                .addColumn("c_varchar_10", "varchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_20", "varchar(20)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_50", "varchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_16777216", "varchar(16777216)", asList("'India'", "'France'", null)) // Snowflake MAX_VARCHAR
                .addColumn("c_varchar_5_unicode", "varchar(5)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_varchar_50_unicode", "varchar(50)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_char_5_unicode", "char(5)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_char_50_unicode", "char(50)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_text", "text", asList("'India'", "'France'", null))
                .addColumn("c_string", "string", asList("'India'", "'France'", null))

                .addColumn("c_char_10", "char(10)", asList("'India'", "'France'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'France'", null))
                .addColumn("c_char_16777216", "char(16777216)", asList("'India'", "'France'", null)) // Snowflake MAX_VARCHAR
                .addColumn("c_character_10", "character(10)", asList("'India'", "'France'", null))
                .addColumn("c_nchar_10", "nchar(10)", asList("'India'", "'France'", null))

                .addColumn("c_varbinary", "varbinary", asList("X''", "X'000000000001'", null))
                .addColumn("c_binary", "binary", asList("X''", "X'000000000001'", null))
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2020-08-15'", null))
                .addColumn("c_time", "time", asList("TIME '00:13:42.000'", "TIME '11:22:17.100'", null))
                .addColumn("c_timestamp_3", "timestamp(3)", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2022-08-15 09:08:07.333'", null))
                .addColumn("c_timestamp_6", "timestamp(6)", asList("TIMESTAMP '2024-09-08 01:02:03.666333'", "TIMESTAMP '2022-08-15 09:08:07.333666'", null))
                .addColumn("c_timestamp_9", "timestamp(9)", asList("TIMESTAMP '2024-09-08 01:02:03.666333111'", "TIMESTAMP '2022-08-15 09:08:07.333666111'", null))
                .addColumn("c_timestamptz", "timestamp_tz", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2022-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_timestampltz", "timestamp_ltz", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2022-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_timestampntz", "timestamp_ntz", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2022-08-15 09:08:07.333'", null))
                .addColumn("c_datetime", "datetime", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2022-08-15 09:08:07.333'", null))
                .execute(onRemoteDatabase(), TEST_SCHEMA + "." + "right_table_"));
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

    @Test
    public void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(100)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    public void testAllJoinPushdownWithCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(10)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.targetColumn(),
                testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.targetColumn(),
                testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.targetColumn(),
                testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.targetColumn(),
                testCase.castType())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(200)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(
                leftTable(),
                rightTable(),
                testCase.sourceColumn(),
                testCase.castType(),
                testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    public void testCastPushdownDisabled()
    {
        Session sessionWithoutComplexExpressionPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutComplexExpressionPushdown, "SELECT CAST (c_varchar_10 AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    public void testCastPushdownWithForcedTypedToVarchar()
    {
        // This type is forced mapped to varchar
        assertThat(query("SELECT CAST(c_boolean AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    public void testSemiStructuredTypesCastPushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                getSession().getSchema().orElseThrow() + ".semi_structured_types",
                "(id INT, c_variant VARIANT, c_object OBJECT, c_array ARRAY)")) {
            // these types must be inserted as CTAS
            onRemoteDatabase().execute(
                    """
                    INSERT INTO %s (id, c_variant, c_object, c_array)
                    SELECT 1,
                    to_variant(OBJECT_CONSTRUCT('key1', 42, 'key2', 54)),
                    OBJECT_CONSTRUCT('key1',42,'key2',54),
                    ARRAY_CONSTRUCT(12, 'twelve', NULL)""".formatted(table.getName()));
            for (CastTestCase testCase : ImmutableList.of(
                    new CastTestCase("c_variant", "varchar(50)", "c_varchar_50"),
                    new CastTestCase("c_object", "varchar(50)", "c_varchar_50"),
                    new CastTestCase("c_array", "varchar(50)", "c_varchar_50"))) {
                assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table.getName())))
                        .isNotFullyPushedDown(ProjectNode.class);
            }
        }
    }

    @Test
    public void testCastPushdownWithCharPadding()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                getSession().getSchema().orElseThrow() + ".padding",
                "(id INT, c_char_3 CHAR(3), c_char_5 CHAR(5), c_varchar_3 VARCHAR(3), c_varchar_5 VARCHAR(5))",
                List.of(
                        "1, 'A', 'A ', 'A', 'A    '",
                        "2, 'A', 'A', 'A ', 'A'",
                        "3, 'A', 'A', 'A  ', 'A  x'",
                        "4, ' A', 'A ', 'A  ', ' A '",
                        "5, ' a', 'a', 'a  ', ' a '",
                        "6, 'a  ', 'a', 'A', 'a '"))) {
            assertThat(query(
                    "SELECT id, c_char_3 = c_char_5, c_char_3 = c_varchar_5, c_varchar_3 = c_char_5,  c_varchar_3 = c_varchar_5 FROM %s ORDER BY id".formatted(table.getName())))
                    .hasCorrectResultsRegardlessOfPushdown();
        }
    }

    @Test
    public void testCastPushdownOfIntegralsWrittenWithTrino()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                getSession().getSchema().orElseThrow() + ".integrals",
                // Trino write mappings are different from Snowflake defaults
                "(id INT, c_tinyint TINYINT, c_smallint SMALLINT, c_integer INTEGER, c_bigint BIGINT)",
                List.of(
                        "1, 0, 0, 0, 0",
                        "2, 127, 32767, 2147483647, 9223372036854775807",
                        "3, -127, -32767, -2147483647, -9223372036854775807",
                        "4, NULL, NULL, NULL, NULL"))) {
            for (CastTestCase testCase : ImmutableList.of(
                    new CastTestCase("c_tinyint", "smallint", "c_smallint"),
                    new CastTestCase("c_smallint", "integer", "c_integer"),
                    new CastTestCase("c_integer", "bigint", "c_bigint"))) {
                assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table.getName())))
                        .isFullyPushedDown();
            }
            for (CastTestCase testCase : ImmutableList.of(
                    new CastTestCase("c_smallint", "tinyint", "c_tinyint"),
                    new CastTestCase("c_integer", "smallint", "c_smallint"),
                    new CastTestCase("c_bigint", "integer", "c_integer"))) {
                assertThat(query("SELECT CAST(%s AS %s) FROM %s  where ID = 1".formatted(testCase.sourceColumn(), testCase.castType(), table.getName())))
                        .isFullyPushedDown();
                assertThat(query("SELECT CAST(%s AS %s) FROM %s where ID != 1".formatted(testCase.sourceColumn(), testCase.castType(), table.getName())))
                        .failure()
                        .hasMessageContaining("Number out of representable range");
                assertThat(query(
                        disablePushdown(getSession()),
                        "SELECT CAST(%s AS %s) FROM %s where ID != 1".formatted(testCase.sourceColumn(), testCase.castType(), table.getName())))
                        .failure()
                        .hasMessageContaining("Cannot cast");
            }
        }
    }

    @Test
    public void testCastPushdownOfDecimalsWithScaleAndPrecisionReduction()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                getSession().getSchema().orElseThrow() + ".decimals",
                // Trino write mappings are different from Snowflake defaults
                "(id INT, c_decimal_4_2 DECIMAL(4, 2), c_decimal_2_1 DECIMAL(2, 1))",
                List.of(
                        "1, 0, 0",
                        "2, 10.25, 1.5",
                        "3, -10.25, -1.5",
                        "4, 99.99, 9.9",
                        "5, NULL, NULL"))) {
            assertThat(query("SELECT CAST(c_decimal_4_2 AS DECIMAL(4,1)), CAST(c_decimal_2_1 AS DECIMAL(2,0)) FROM %s".formatted(table.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_decimal_4_2 AS DECIMAL(3,1)), CAST(c_decimal_2_1 AS DECIMAL(1,0)) FROM %s WHERE id = 4".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Number out of representable range");
            assertThat(query(
                    disablePushdown(getSession()),
                    "SELECT CAST(c_decimal_4_2 AS DECIMAL(3,1)), CAST(c_decimal_2_1 AS DECIMAL(1,0)) FROM %s WHERE id = 4".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Cannot cast");
        }
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_char_10", "char(50)", "c_char_50"),
                new CastTestCase("c_char_5_unicode", "char(50)", "c_char_50_unicode"),
                new CastTestCase("c_char_5_unicode", "varchar(5)", "c_varchar_5_unicode"),
                new CastTestCase("c_char_10", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_char_5_unicode", "varchar(50)", "c_varchar_50_unicode"),
                new CastTestCase("c_char_50", "char(10)", "c_char_10"),
                new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_varchar_5_unicode", "varchar(50)", "c_varchar_50_unicode"),
                new CastTestCase("c_varchar_10", "char(50)", "c_char_50"),
                new CastTestCase("c_varchar_5_unicode", "char(50)", "c_char_50_unicode"),
                new CastTestCase("c_varchar_5_unicode", "char(5)", "c_char_5_unicode"),
                new CastTestCase("c_varchar_50", "varchar(10)", "c_varchar_10"),

                new CastTestCase("c_decimal_3_0", "decimal(38, 5)", "c_decimal_38_5"),
                new CastTestCase("c_numeric_3_0", "decimal(38, 37)", "c_decimal_38_37"),

                new CastTestCase("c_decimal_38_5", "decimal(3, 0)", "c_decimal_3_0"),
                new CastTestCase("c_numeric_3_0", "decimal(38, 37)", "c_decimal_38_37"),

                new CastTestCase("c_tinyint", "smallint", "c_smallint"),
                new CastTestCase("c_smallint", "tinyint", "c_tinyint"),
                new CastTestCase("c_smallint", "integer", "c_integer"),
                new CastTestCase("c_integer", "bigint", "c_bigint"),
                new CastTestCase("c_integer", "smallint", "c_smallint"),
                new CastTestCase("c_bigint", "integer", "c_integer"),
                new CastTestCase("c_decimal_38_0", "bigint", "c_bigint"),
                new CastTestCase("c_decimal_38_0", "integer", "c_integer"),
                new CastTestCase("c_decimal_38_0", "smallint", "c_smallint"),
                new CastTestCase("c_decimal_38_0", "tinyint", "c_tinyint"),
                new CastTestCase("c_date", "date", "c_date"),
                new CastTestCase("c_timestamp_3", "timestamp(6)", "c_timestamp_6"),
                new CastTestCase("c_timestamp_6", "timestamp(9)", "c_timestamp_9"));
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_tinyint", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_smallint", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_integer", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_bigint", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_double", "real", "c_real"),
                new CastTestCase("c_double_precision", "real", "c_real"),
                new CastTestCase("c_float", "real", "c_real"),
                new CastTestCase("c_float", "real", "c_real"),
                new CastTestCase("c_float4", "real", "c_real"),
                new CastTestCase("c_float8", "real", "c_real"),
                new CastTestCase("c_decimal_3_0", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_decimal_38_5", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_char_10", "varchar", "c_varchar_10"),
                new CastTestCase("c_varchar_50", "varchar", "c_varchar_10"),
                new CastTestCase("c_date", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_date", "timestamp(9)", "c_timestamp_9"),
                new CastTestCase("c_time", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_timestamp_9", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_timestamp_6", "timestamp(3)", "c_timestamp_3"),
                new CastTestCase("c_timestamp_9", "timestamp(3)", "c_timestamp_3"),
                new CastTestCase("c_timestamp_9", "timestamp(6)", "c_timestamp_6"));
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.of(
                new InvalidCastTestCase("c_decimal_3_0", "char(50)"),
                new InvalidCastTestCase("c_numeric_38_5", "char(50)"),
                new InvalidCastTestCase("c_float", "char(50)"),
                new InvalidCastTestCase("c_float4", "char(50)"),
                new InvalidCastTestCase("c_float8", "char(50)"),
                new InvalidCastTestCase("c_double_precision", "char(50)"),
                new InvalidCastTestCase("c_double", "char(50)"),
                new InvalidCastTestCase("c_real", "char(50)"),
                new InvalidCastTestCase("c_date", "char(50)"),
                new InvalidCastTestCase("c_time", "char(50)"),
                new InvalidCastTestCase("c_timestamp_3", "char(50)"),
                new InvalidCastTestCase("c_timestamptz", "char(50)"),
                new InvalidCastTestCase("c_timestampltz", "char(50)"),
                new InvalidCastTestCase("c_timestampntz", "char(50)"),
                new InvalidCastTestCase("c_datetime", "char(50)"),
                new InvalidCastTestCase("c_binary", "char(50)"),
                new InvalidCastTestCase("c_varbinary", "char(50)"),
                new InvalidCastTestCase("c_binary", "varchar(50)"),
                new InvalidCastTestCase("c_varbinary", "varchar(50)"));
    }

    private static Session disablePushdown(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();
    }
}
