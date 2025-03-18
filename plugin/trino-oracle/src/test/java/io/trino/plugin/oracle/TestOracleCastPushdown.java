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
package io.trino.plugin.oracle;

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

import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOracleCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(new TestingOracleServer());
        QueryRunner queryRunner = OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("jdbc-types-mapped-to-varchar", "interval year(2) to month, timestamp(6) with local time zone")
                        .put("join-pushdown.enabled", "true")
                        // Set oracle.number.default-scale=s to map Oracle NUMBER (without precision/scale) to DECIMAL(38, s)
                        .put("oracle.number.default-scale", "2")
                        .buildOrThrow())
                .build();

        queryRunner.createCatalog(
                "oracle_number_mapped_to_varchar",
                "oracle",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", oracleServer.getJdbcUrl())
                        .put("connection-user", TEST_USER)
                        .put("connection-password", TEST_PASS)
                        .put("unsupported-type-handling", "CONVERT_TO_VARCHAR")
                        .put("join-pushdown.enabled", "true")
                        .buildOrThrow());
        return queryRunner;
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return new SqlExecutor()
        {
            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }

            @Override
            public void execute(String sql)
            {
                oracleServer.execute(sql);
            }
        };
    }

    @BeforeAll
    public void setup()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "number(10)", asList(11, 12, 13))
                .addColumn("c_number_3", "number(3)", asList(1, 2, null)) // tinyint in trino
                .addColumn("c_number_5", "number(5)", asList(1, 2, null)) // smallint in trino
                .addColumn("c_number_10", "number(10)", asList(1, 2, null)) // integer in trino
                .addColumn("c_number_19", "number(19)", asList(1, 2, null)) // bigint in trino
                .addColumn("c_float", "float", asList(1.23, 2.67, null)) // double in trino
                .addColumn("c_float_5", "float(5)", asList(1.23, 2.67, null)) // double in trino
                .addColumn("c_binary_float", "binary_float", asList(1.23, 2.67, null))
                .addColumn("c_binary_double", "binary_double", asList(1.23, 2.67, null))
                .addColumn("c_nan", "binary_double", asList("BINARY_DOUBLE_NAN", "BINARY_FLOAT_NAN", null))
                .addColumn("c_infinity", "binary_double", asList("BINARY_DOUBLE_INFINITY", "-BINARY_DOUBLE_INFINITY", null))
                .addColumn("c_number_15", "decimal(15)", asList(1, 2, null))
                .addColumn("c_number_10_2", "decimal(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_number_30_2", "decimal(30, 2)", asList(1.23, 2.67, null))
                .addColumn("c_number_38_0", "decimal(38, 0)", asList(1.23, 2.67, null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_501", "char(501)", asList("'India'", "'Poland'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_char_520", "char(520)", asList("'India'", "'Poland'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_nchar_10", "nchar(10)", asList("N'India'", "N'Poland'", null))
                .addColumn("c_varchar_10", "varchar2(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_10_byte", "varchar2(10 byte)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_50", "varchar2(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_1001", "varchar2(1001)", asList("'India'", "'Poland'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_1020", "varchar2(1020)", asList("'India'", "'Poland'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_numeric", "varchar2(50)", asList("'123'", "'456'", null))
                .addColumn("c_varchar_decimal", "varchar2(50)", asList("'1.23'", "'2.67'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar2(50)", asList("'H311o'", "'123Hey'", null))
                .addColumn("c_varchar_date", "varchar2(50)", asList("'2024-09-08'", "'2019-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar2(50)", asList("'2024-09-08 01:02:03.666'", "'2019-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar2(50)", asList("'2024-09-08 01:02:03.666 +05:30'", "'2019-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_nvarchar_100", "nvarchar2(100)", asList("N'India'", "N'Poland'", null)) // varchar(p) in trino
                .addColumn("c_clob", "clob", asList("'India'", "'Poland'", null)) // varchar in trino
                .addColumn("c_nclob", "nclob", asList("N'India'", "N'Poland'", null)) // varchar in trino
                .addColumn("c_blob", "blob", asList("HEXTORAW('496E646961')", "HEXTORAW('506F6C616E64')", null)) // varbinary in trino
                .addColumn("c_raw_200", "raw(200)", asList("HEXTORAW('496E646961')", "HEXTORAW('506F6C616E64')", null)) // varbinary in trino
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2019-08-15'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamp with time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2019-08-15 09:08:07.333 +05:30'", null))
                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
                .addColumn("c_char_unicode", "char(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nchar_unicode", "nchar(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_varchar_unicode", "varchar2(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nvarchar_unicode", "nvarchar2(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_clob_unicode", "clob", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nclob_unicode", "nclob", asList("N'攻殻機動隊'", "N'😂'", null))

                // unsupported in trino
                .addColumn("c_number", "number", asList(1, 2, null))
                .addColumn("c_timestamp_ltz", "timestamp with local time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_interval_ym", "interval year to month", asList("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '3-4' YEAR TO MONTH", null))
                .addColumn("c_interval_ds", "interval day to second", asList("INTERVAL '10 11:12:13.456' DAY TO SECOND", "INTERVAL '10 12:13:14.456' DAY TO SECOND", null))
                .addColumn("c_long", "long", asList("'India'", "'Poland'", null))
                .addColumn("c_xmltype", "xmltype", asList("XMLTYPE('<root><element>Sample XML-1</element></root>')", "XMLTYPE('<root><element>Sample XML-2</element></root>')", null))
                .execute(onRemoteDatabase(), "left_table_"));

        // 2nd row value is different in right table than left table
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "number(10)", asList(21, 22, 23))
                .addColumn("c_number_3", "number(3)", asList(1, 22, null)) // tinyint in trino
                .addColumn("c_number_5", "number(5)", asList(1, 22, null)) // smallint in trino
                .addColumn("c_number_10", "number(10)", asList(1, 22, null)) // integer in trino
                .addColumn("c_number_19", "number(19)", asList(1, 22, null)) // bigint in trino
                .addColumn("c_float", "float", asList(1.23, 22.67, null)) // double in trino
                .addColumn("c_float_5", "float(5)", asList(1.23, 22.67, null)) // double in trino
                .addColumn("c_binary_float", "binary_float", asList(1.23, 22.67, null))
                .addColumn("c_binary_double", "binary_double", asList(1.23, 22.67, null))
                .addColumn("c_nan", "binary_double", asList("BINARY_DOUBLE_NAN", "BINARY_DOUBLE_NAN", null))
                .addColumn("c_infinity", "binary_double", asList("BINARY_DOUBLE_INFINITY", "BINARY_DOUBLE_INFINITY", null))
                .addColumn("c_number_15", "decimal(15)", asList(1, 22, null))
                .addColumn("c_number_10_2", "decimal(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_number_30_2", "decimal(30, 2)", asList(1.23, 22.67, null))
                .addColumn("c_number_38_0", "decimal(38, 0)", asList(1.23, 22.67, null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'France'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'France'", null))
                .addColumn("c_char_501", "char(501)", asList("'India'", "'France'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_char_520", "char(520)", asList("'India'", "'France'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_nchar_10", "nchar(10)", asList("N'India'", "N'France'", null))
                .addColumn("c_varchar_10", "varchar2(10)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_10_byte", "varchar2(10 byte)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_50", "varchar2(50)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_1001", "varchar2(1001)", asList("'India'", "'France'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_1020", "varchar2(1020)", asList("'India'", "'France'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_numeric", "varchar2(50)", asList("'123'", "'234'", null))
                .addColumn("c_varchar_decimal", "varchar2(50)", asList("'1.23'", "'22.67'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar2(50)", asList("'H311o'", "'123Bye'", null))
                .addColumn("c_varchar_date", "varchar2(50)", asList("'2024-09-08'", "'2020-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar2(50)", asList("'2024-09-08 01:02:03.666'", "'2020-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar2(50)", asList("'2024-09-08 01:02:03.666 +05:30'", "'2020-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_nvarchar_100", "nvarchar2(100)", asList("N'India'", "N'France'", null)) // varchar(p) in trino
                .addColumn("c_clob", "clob", asList("'India'", "'France'", null)) // varchar in trino
                .addColumn("c_nclob", "nclob", asList("N'India'", "N'France'", null)) // varchar in trino
                .addColumn("c_blob", "blob", asList("HEXTORAW('496E646961')", "HEXTORAW('4672616E6365')", null)) // varbinary in trino
                .addColumn("c_raw_200", "raw(200)", asList("HEXTORAW('496E646961')", "HEXTORAW('4672616E6365')", null)) // varbinary in trino
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2020-08-15'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamp with time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2020-08-15 09:08:07.333 +05:30'", null))
                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😇 is 1.
                .addColumn("c_char_unicode", "char(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_nchar_unicode", "nchar(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_varchar_unicode", "varchar2(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_nvarchar_unicode", "nvarchar2(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_clob_unicode", "clob", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_nclob_unicode", "nclob", asList("N'攻殻機動隊'", "N'😇'", null))

                // unsupported in trino
                .addColumn("c_number", "number", asList(1, 22, null))
                .addColumn("c_timestamp_ltz", "timestamp with local time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_interval_ym", "interval year to month", asList("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '4-5' YEAR TO MONTH", null))
                .addColumn("c_interval_ds", "interval day to second", asList("INTERVAL '10 11:12:13.456' DAY TO SECOND", "INTERVAL '11 12:13:14.456' DAY TO SECOND", null))
                .addColumn("c_long", "long", asList("'India'", "'France'", null))
                .addColumn("c_xmltype", "xmltype", asList("XMLTYPE('<root><element>Sample XML-1</element></root>')", "XMLTYPE('<root><element>Sample XML-3</element></root>')", null))
                .execute(onRemoteDatabase(), "right_table_"));
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
    public void testCastPushdownSpecialCase()
    {
        for (CastTestCase testCase : specialCaseNClob()) {
            // Projection pushdown is supported, because trino converts the clob type to nclob thus cast is not required
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), leftTable())))
                    .isFullyPushedDown();
            // join pushdown is not supported, because comparison between nclob is not pushdown
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .joinIsNotFullyPushedDown();
        }
    }

    @Test
    public void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(100)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    public void testInsertWithImplicitCharCast()
    {
        try (TestTable sourceTable = new TestTable(
                onRemoteDatabase(),
                "source_",
                "(id int, c_char_2 char(2))",
                asList("1, 'x'", "2, 'y'", "3, null"));
                TestTable targetTable = new TestTable(
                        onRemoteDatabase(),
                        "target_",
                        "(id int, c_char_4 char(4))")) {
            assertUpdate("INSERT INTO %s SELECT * FROM %s".formatted(targetTable.getName(), sourceTable.getName()), 3);
            assertThat(query("SELECT * FROM " + targetTable.getName()))
                    .matches("VALUES " +
                            "(CAST(1 AS DECIMAL(38, 0)), CAST('x   ' AS char(4))), " +
                            "(CAST(2 AS DECIMAL(38, 0)), CAST('y   ' AS char(4))), " +
                            "(CAST(3 AS DECIMAL(38, 0)), CAST(null AS char(4)))");
        }
    }

    @Test
    public void testAllJoinPushdownWithCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(10)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(200)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    public void testCastPushdownClobSensitivity()
    {
        // Verify that clob/nclob join condition is not applied in a case-insensitive way
        try (TestTable leftTable = new TestTable(
                onRemoteDatabase(),
                "l_clob_sensitivity_",
                "(id int, c_varchar_50 varchar(50), c_clob clob, c_nclob nclob)",
                asList("11, 'India', 'India', 'India'", "12, 'Poland', 'Poland', 'Poland'"));
                TestTable rightTable = new TestTable(
                        onRemoteDatabase(),
                        "r_clob_sensitivity_",
                        "(id int, c_varchar_50 varchar(50), c_clob clob, c_nclob nclob)",
                        asList("21, 'INDIA', 'INDIA', 'INDIA'", "22, 'POLAND', 'POLAND', 'POLAND'", "23, 'India', 'India', 'India'"))) {
            assertThat(query("SELECT r.id, r.c_nclob FROM %s l JOIN %s r ON CAST(l.c_nclob AS VARCHAR(50)) = r.c_varchar_50".formatted(leftTable.getName(), rightTable.getName())))
                    .matches("VALUES (CAST(23 AS DECIMAL(38, 0)), VARCHAR 'India')")
                    .isFullyPushedDown();

            assertThat(query("SELECT r.id, r.c_clob FROM %s l JOIN %s r ON CAST(l.c_clob AS VARCHAR(50)) = r.c_varchar_50".formatted(leftTable.getName(), rightTable.getName())))
                    .matches("VALUES (CAST(23 AS DECIMAL(38, 0)), VARCHAR 'India')")
                    .isFullyPushedDown();
        }
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
        // These column types are not supported by default by trino. These types are forced mapped to varchar.
        assertThat(query("SELECT CAST(c_interval_ym AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_timestamp_ltz AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testCastPushdownUsingTableCreatedInTrino()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_table_",
                "(id int, c_tinyint tinyint, c_smallint smallint, c_integer integer, c_bigint bigint, c_decimal_10 decimal(10), c_decimal_10_2 decimal(10, 2))",
                ImmutableList.<String>builder()
                        .add("1, 1, 1, 1, 1, 1, 1.1")
                        .add("2, 2, 2, 2, 2, 2, 2.2")
                        .add("3, null, null, null, null, null, null")
                        .build())) {
            assertThat(query("SELECT CAST(c_tinyint AS decimal(15, 3)) FROM %s".formatted(testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_smallint AS decimal(15, 3)) FROM %s".formatted(testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_integer AS decimal(15, 3)) FROM %s".formatted(testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_bigint AS decimal(15, 3)) FROM %s".formatted(testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_decimal_10 AS decimal(15, 3)) FROM %s".formatted(testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_decimal_10_2 AS decimal(15, 3)) FROM %s".formatted(testTable.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.c_tinyint AS decimal(15, 3)) = r.c_decimal_10_2".formatted(testTable.getName(), testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.c_smallint AS decimal(15, 3)) = r.c_decimal_10_2".formatted(testTable.getName(), testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.c_integer AS decimal(15, 3)) = r.c_decimal_10_2".formatted(testTable.getName(), testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.c_bigint AS decimal(15, 3)) = r.c_decimal_10_2".formatted(testTable.getName(), testTable.getName())))
                    .isFullyPushedDown();
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.c_decimal_10 AS decimal(15, 3)) = r.c_decimal_10_2".formatted(testTable.getName(), testTable.getName())))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testCastPushdownWithNumberColumn()
    {
        Session withoutPushdown = Session.builder(getSession())
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(3)
                // No precision, no scale
                .addColumn("c_number_1", "number", asList(123456789, 987654321, null))
                .addColumn("c_number_2", "number", asList("99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", null))
                .addColumn("c_number_3", "number(*)", asList("99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", null))
                // Defined precision and scale
                .addColumn("c_number_4", "number(*, 1)", asList(1.25, 2.55, null))
                // Precision only
                .addColumn("c_number_5", "number(9)", asList(999999999, -999999999, null))
                // Precision and scale
                .addColumn("c_number_6", "number(9, 1)", asList(123.45, 987.65, null))
                .addColumn("c_number_7", "number(9, 2)", asList(0.01, 99.99, null))
                // Small precision
                .addColumn("c_number_8", "number(6)", asList(999999, -999999, null))
                // Negative scale
                .addColumn("c_number_9", "number(7, -2)", asList(150, 250, null))
                // Large value
                .addColumn("c_number_10", "number(38, 0)", asList("99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", null))
                // Negative value
                .addColumn("c_number_11", "number(9, 2)", asList(-123.45, -987.65, null))
                // Very small decimal value
                .addColumn("c_number_12", "number(9, 5)", asList(0.00001, 0.99999, null))
                // Zero value
                .addColumn("c_number_13", "number(9, 2)", asList(0, 0, null))
                // with 38 precision
                .addColumn("c_number_14", "number(38, 38)", asList("0.00000000000000000000000000000000000001", "0.00000000000000000000000000000000000009", null))
                .addColumn("c_number_15", "number(38, 38)", asList("0.99999999999999999999999999999999999999", "-0.99999999999999999999999999999999999999", null))
                .execute(onRemoteDatabase(), "test_number_")) {
            assertThat(query("SELECT CAST(c_number_1 AS decimal(12, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '123456789.00', DECIMAL '987654321.00', null")
                    .isFullyPushedDown();

            assertThat(query(withoutPushdown, "SELECT CAST(c_number_2 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Decimal overflow");
            assertThat(query("SELECT CAST(c_number_2 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("value larger than specified precision allowed for this column");

            assertThat(query(withoutPushdown, "SELECT CAST(c_number_2 AS decimal(38, 1)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Decimal overflow");
            assertThat(query("SELECT CAST(c_number_2 AS decimal(38, 1)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("value larger than specified precision allowed for this column");

            assertThat(query(withoutPushdown, "SELECT CAST(c_number_2 AS decimal(38, 2)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Decimal overflow");
            // Pushdown will not happen because c_number_2 is mapped to decimal(38,2) type
            assertThat(query("SELECT CAST(c_number_2 AS decimal(38, 2)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Decimal overflow");

            assertThat(query(withoutPushdown, "SELECT CAST(c_number_3 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Decimal overflow");
            assertThat(query("SELECT CAST(c_number_3 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("value larger than specified precision allowed for this column");

            assertThat(query(withoutPushdown, "SELECT CAST(c_number_3 AS decimal(38, 1)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Decimal overflow");
            assertThat(query("SELECT CAST(c_number_3 AS decimal(38, 1)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("value larger than specified precision allowed for this column");

            assertThat(query("SELECT CAST(c_number_4 AS decimal(10, 1)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.3', DECIMAL '2.6', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_5 AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '999999999', DECIMAL '-999999999', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_6 AS decimal(10, 1)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '123.5', DECIMAL '987.7', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_7 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.01', DECIMAL '99.99', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_8 AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '999999', DECIMAL '-999999', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_9 AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '200', DECIMAL '300', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_10 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '99999999999999999999999999999999999999', DECIMAL '-99999999999999999999999999999999999999', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_11 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '-123.45', DECIMAL'-987.65', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_12 AS decimal(10, 5)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.00001', DECIMAL '0.99999', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_13 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.00', DECIMAL '0.00', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_14 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0', DECIMAL '0', null")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_number_14 AS decimal(38, 36)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000', DECIMAL '0.000000000000000000000000000000000000', null")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_number_14 AS decimal(38, 37)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.000000000000000000000000000000000000', DECIMAL '0.0000000000000000000000000000000000001', null")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_number_14 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000001', DECIMAL '0.00000000000000000000000000000000000009', null")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_15 AS decimal(38, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1', DECIMAL '-1', null")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_number_15 AS decimal(38, 36)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.00000000000000000000000000000000000', DECIMAL '-1.000000000000000000000000000000000000', null")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_number_15 AS decimal(38, 37)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.000000000000000000000000000000000000', DECIMAL '-1.0000000000000000000000000000000000000', null")
                    .isFullyPushedDown();
            assertThat(query("SELECT CAST(c_number_15 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '0.99999999999999999999999999999999999999', DECIMAL '-0.99999999999999999999999999999999999999', null")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testCastPushdownWithTruncationAndRounding()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_number_1", "number(9, 4)", asList(1.234, 1.235))
                .addColumn("c_number_2", "number(9, 4)", asList(-1.234, -1.235))
                .addColumn("c_number_3", "number(9, 4)", asList(1.9899, 2.9899))
                .addColumn("c_number_4", "number(9, 4)", asList(1.9999, 2.9999))
                .addColumn("c_number_5", "number(9, 4)", asList(-1.9999, -2.9999))
                .addColumn("c_number_6", "number(9, 4)", asList(-1.9899, -2.9899))
                .addColumn("c_number_7", "number(9, 4)", asList(1.5, -1.5))
                .addColumn("c_number_8", "number(9, 4)", asList(0.9999, -0.9999))
                .execute(onRemoteDatabase(), "test_number_")) {
            assertThat(query("SELECT CAST(c_number_1 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.23', DECIMAL '1.24'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_2 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '-1.23', DECIMAL '-1.24'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_3 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.99', DECIMAL '2.99'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_4 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '2.00', DECIMAL '3.00'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_5 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '-2.00', DECIMAL '-3.00'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_6 AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '-1.99', DECIMAL '-2.99'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_7 AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '2', DECIMAL '-2'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_8 AS decimal(10, 1)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.0', DECIMAL '-1.0'")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testCastPushdownWithLowerScale()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(3)
                .addColumn("c_number_high_scale", "number(9, 4)", asList(1.2345, 2.5789, 3.9999))
                .execute(onRemoteDatabase(), "test_number_");) {
            // Lowering scale from 4 to 2 – Expect rounding or truncation
            assertThat(query("SELECT CAST(c_number_high_scale AS decimal(10, 2)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.23', DECIMAL '2.58', DECIMAL '4.00'")
                    .isFullyPushedDown();

            // Lowering scale from 4 to 1 – More aggressive truncation or rounding
            assertThat(query("SELECT CAST(c_number_high_scale AS decimal(10, 1)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1.2', DECIMAL '2.6', DECIMAL '4.0'")
                    .isFullyPushedDown();

            // Lowering scale from 4 to 0 – Expect integer truncation
            assertThat(query("SELECT CAST(c_number_high_scale AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '1', DECIMAL '3', DECIMAL '4'")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testCastPushdownWithLowerPrecisionAndScale()
    {
        Session withoutPushdown = Session.builder(getSession())
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(1)
                .addColumn("c_number_1", "number(9, 4)", List.of(12345.6789))
                .addColumn("c_number_2", "number(9, 4)", List.of(99999.9999))
                .addColumn("c_number_3", "number(9, 4)", List.of(1.2345))
                .execute(onRemoteDatabase(), "test_number_")) {
            assertThat(query("SELECT CAST(c_number_1 AS decimal(5, 4)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("value larger than specified precision allowed for this column");
            assertThat(query(withoutPushdown, "SELECT CAST(c_number_1 AS decimal(5, 4)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Cannot cast DECIMAL(9, 4) '12345.6789' to DECIMAL(5, 4)");

            assertThat(query("SELECT CAST(c_number_2 AS decimal(6, 1)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("value larger than specified precision allowed for this column");
            assertThat(query(withoutPushdown, "SELECT CAST(c_number_2 AS decimal(6, 1)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Cannot cast DECIMAL(9, 4) '99999.9999' to DECIMAL(6, 1)");

            assertThat(query("SELECT CAST(c_number_2 AS decimal(7, 1)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '100000.0'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_3 AS decimal(5, 4)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '1.2345'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_3 AS decimal(4, 3)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '1.235'")
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_number_3 AS decimal(1, 0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '1'")
                    .isFullyPushedDown();
        }
    }

    @Test
    void testCastNumberMappedToVarchar()
    {
        Session session = Session.builder(getSession())
                .setCatalog("oracle_number_mapped_to_varchar")
                .setCatalogSessionProperty("oracle_number_mapped_to_varchar", "number_rounding_mode", "HALF_UP")
                .build();

        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(1)
                .addColumn("c_number_1", "number", List.of(12345.6789))
                .addColumn("c_number_2", "number(*)", List.of(12345.6789))
                .execute(onRemoteDatabase(), "test_number_")) {
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(4, 0)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Cannot cast VARCHAR '12345.6789' to DECIMAL(4, 0). Value too large");
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(10, 4)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '12345.6789'")
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(10, 1)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '12345.7'")
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '12346'")
                    .isNotFullyPushedDown(ProjectNode.class);

            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(4, 0)) FROM %s".formatted(table.getName())))
                    .failure()
                    .hasMessageContaining("Cannot cast VARCHAR '12345.6789' to DECIMAL(4, 0). Value too large");
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(10, 4)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '12345.6789'")
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(10, 1)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '12345.7'")
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(10, 0)) FROM %s".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES DECIMAL '12346'")
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    void testCastHighNumberScaleWithRoundingMode()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(1)
                .addColumn("c_number_1", "number(38, 40)", List.of(0.0012345678901234567890123456789012345678))
                .addColumn("c_number_2", "number(18, 40)", List.of(0.0000000000000000000000123456789012345678))
                .addColumn("c_number_3", "number(38, 80)", List.of(0.00000000000000000000000000000000000000000000012345678901234567890123456789012345678))
                .execute(onRemoteDatabase(), "test_number_")) {
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "CEILING")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123457'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000001'")
                    .isFullyPushedDown();

            session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "FLOOR")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123456'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000000'")
                    .isFullyPushedDown();

            session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "HALF_DOWN")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123457'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000000'")
                    .isFullyPushedDown();

            session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "HALF_EVEN")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123457'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000000'")
                    .isFullyPushedDown();

            session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "HALF_UP")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123457'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000000'")
                    .isFullyPushedDown();

            session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "UP")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123457'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000001'")
                    .isFullyPushedDown();

            session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", "number_rounding_mode", "DOWN")
                    .build();
            assertThat(query(session, "SELECT CAST(c_number_1 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00123456789012345670000000000000000000'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_2 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000001234567890123456'")
                    .isFullyPushedDown();
            assertThat(query(session, "SELECT CAST(c_number_3 AS decimal(38, 38)) FROM %s".formatted(table.getName())))
                    .matches("VALUES DECIMAL '0.00000000000000000000000000000000000000'")
                    .isFullyPushedDown();
        }
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_char_10", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_char_50", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_char_501", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_char_520", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nchar_10", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_varchar_10", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_varchar_10_byte", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_varchar_1001", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_varchar_1020", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nvarchar_100", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_clob", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nclob", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_char_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nchar_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_varchar_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nvarchar_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_clob_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nclob_unicode", "char(50)", "c_char_50"))

                .add(new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_varchar_10_byte", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_varchar_1001", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_varchar_1020", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nvarchar_100", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_clob", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nclob", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_3", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_5", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_10", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_19", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_15", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_10_2", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_number_30_2", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_varchar_unicode", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nvarchar_unicode", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_clob_unicode", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nclob_unicode", "varchar(50)", "c_varchar_50"))

                .add(new CastTestCase("c_number_3", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_3", "decimal(10, 2)", "c_number_10_2"))
                .add(new CastTestCase("c_number_3", "decimal(30, 2)", "c_number_30_2"))
                .add(new CastTestCase("c_number_3", "decimal(38, 0)", "c_number_38_0"))

                .add(new CastTestCase("c_number_5", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_5", "decimal(10, 2)", "c_number_10_2"))
                .add(new CastTestCase("c_number_5", "decimal(30, 2)", "c_number_30_2"))
                .add(new CastTestCase("c_number_5", "decimal(38, 0)", "c_number_38_0"))

                .add(new CastTestCase("c_number_10", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_10", "decimal(10, 2)", "c_number_10_2"))
                .add(new CastTestCase("c_number_10", "decimal(30, 2)", "c_number_30_2"))
                .add(new CastTestCase("c_number_10", "decimal(38, 0)", "c_number_38_0"))

                .add(new CastTestCase("c_number_19", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_19", "decimal(10, 2)", "c_number_10_2"))
                .add(new CastTestCase("c_number_19", "decimal(30, 2)", "c_number_30_2"))
                .add(new CastTestCase("c_number_19", "decimal(38, 0)", "c_number_38_0"))

                .add(new CastTestCase("c_number_10_2", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_10_2", "decimal(30, 2)", "c_number_30_2"))
                .add(new CastTestCase("c_number_10_2", "decimal(38, 0)", "c_number_38_0"))

                .add(new CastTestCase("c_number_30_2", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_30_2", "decimal(10, 2)", "c_number_10_2"))
                .add(new CastTestCase("c_number_30_2", "decimal(38, 0)", "c_number_38_0"))

                .add(new CastTestCase("c_number_38_0", "decimal(15)", "c_number_15"))
                .add(new CastTestCase("c_number_38_0", "decimal(10, 2)", "c_number_10_2"))
                .add(new CastTestCase("c_number_38_0", "decimal(30, 2)", "c_number_30_2"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_char_10", "char(501)", "c_char_501"))
                .add(new CastTestCase("c_char_501", "char(520)", "c_char_520"))
                .add(new CastTestCase("c_varchar_10", "char(501)", "c_char_501"))
                .add(new CastTestCase("c_varchar_1001", "char(501)", "c_char_501"))
                .add(new CastTestCase("c_clob", "char(501)", "c_char_501"))
                .add(new CastTestCase("c_nclob", "char(501)", "c_char_501"))

                // Issue with padding in the result data
                .add(new CastTestCase("c_char_10", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_char_50", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_char_501", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_char_520", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nchar_10", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_char_unicode", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nchar_unicode", "varchar(50)", "c_varchar_50"))

                .add(new CastTestCase("c_char_10", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_char_501", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_varchar_10", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_varchar_1001", "varchar(1020)", "c_varchar_1020"))
                .add(new CastTestCase("c_clob", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_nclob", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_3", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_5", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_10", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_19", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_float", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_float_5", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_binary_float", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_binary_double", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_nan", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_infinity", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_15", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_10_2", "varchar(1001)", "c_varchar_1001"))
                .add(new CastTestCase("c_number_30_2", "varchar(1001)", "c_varchar_1001"))

                .add(new CastTestCase("c_char_10", "varchar", "c_clob"))
                .add(new CastTestCase("c_char_501", "varchar", "c_clob"))
                .add(new CastTestCase("c_varchar_10", "varchar", "c_clob"))
                .add(new CastTestCase("c_varchar_1001", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_3", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_5", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_10", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_19", "varchar", "c_clob"))
                .add(new CastTestCase("c_float", "varchar", "c_clob"))
                .add(new CastTestCase("c_float_5", "varchar", "c_clob"))
                .add(new CastTestCase("c_binary_float", "varchar", "c_clob"))
                .add(new CastTestCase("c_binary_double", "varchar", "c_clob"))
                .add(new CastTestCase("c_nan", "varchar", "c_clob"))
                .add(new CastTestCase("c_infinity", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_15", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_10_2", "varchar", "c_clob"))
                .add(new CastTestCase("c_number_30_2", "varchar", "c_clob"))

                .add(new CastTestCase("c_char_10", "varchar", "c_nclob"))
                .add(new CastTestCase("c_char_501", "varchar", "c_nclob"))
                .add(new CastTestCase("c_varchar_10", "varchar", "c_nclob"))
                .add(new CastTestCase("c_varchar_1001", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_3", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_5", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_10", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_19", "varchar", "c_nclob"))
                .add(new CastTestCase("c_float", "varchar", "c_nclob"))
                .add(new CastTestCase("c_float_5", "varchar", "c_nclob"))
                .add(new CastTestCase("c_binary_float", "varchar", "c_nclob"))
                .add(new CastTestCase("c_binary_double", "varchar", "c_nclob"))
                .add(new CastTestCase("c_nan", "varchar", "c_nclob"))
                .add(new CastTestCase("c_infinity", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_15", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_10_2", "varchar", "c_nclob"))
                .add(new CastTestCase("c_number_30_2", "varchar", "c_nclob"))

                .add(new CastTestCase("c_number_3", "tinyint", "c_number_5"))
                .add(new CastTestCase("c_number_3", "smallint", "c_number_10"))
                .add(new CastTestCase("c_number_3", "integer", "c_number_19"))
                .add(new CastTestCase("c_number_3", "bigint", "c_float"))
                .add(new CastTestCase("c_number_3", "real", "c_float_5"))
                .add(new CastTestCase("c_number_3", "double", "c_binary_float"))
                .add(new CastTestCase("c_number_3", "double", "c_binary_double"))
                .add(new CastTestCase("c_varchar_10", "varbinary", "c_blob"))
                .add(new CastTestCase("c_varchar_10", "varbinary", "c_raw_200"))
                .add(new CastTestCase("c_timestamp", "date", "c_date"))
                .add(new CastTestCase("c_timestamptz", "timestamp", "c_timestamp"))
                .add(new CastTestCase("c_timestamp", "timestamp with time zone", "c_timestamptz"))

                // When data inserted from Trino, below cases give mismatched value between pushdown
                // and without pushdown, So not supporting cast pushdown for these cases
                .add(new CastTestCase("c_float", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_float_5", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_binary_float", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_binary_double", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nan", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_infinity", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_date", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_timestamp", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_timestamptz", "varchar(50)", "c_varchar_50"))
                .build();
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.<InvalidCastTestCase>builder()
                .add(new InvalidCastTestCase("c_number_3", "char(50)"))
                .add(new InvalidCastTestCase("c_number_5", "char(50)"))
                .add(new InvalidCastTestCase("c_number_10", "char(50)"))
                .add(new InvalidCastTestCase("c_number_19", "char(50)"))
                .add(new InvalidCastTestCase("c_float", "char(50)"))
                .add(new InvalidCastTestCase("c_float_5", "char(50)"))
                .add(new InvalidCastTestCase("c_binary_float", "char(50)"))
                .add(new InvalidCastTestCase("c_binary_double", "char(50)"))
                .add(new InvalidCastTestCase("c_number_15", "char(50)"))
                .add(new InvalidCastTestCase("c_number_10_2", "char(50)"))
                .add(new InvalidCastTestCase("c_number_30_2", "char(50)"))
                .add(new InvalidCastTestCase("c_date", "char(50)"))
                .add(new InvalidCastTestCase("c_timestamp", "char(50)"))
                .add(new InvalidCastTestCase("c_timestamptz", "char(50)"))
                .add(new InvalidCastTestCase("c_blob", "char(50)"))
                .add(new InvalidCastTestCase("c_raw_200", "char(50)"))

                .add(new InvalidCastTestCase("c_number_3", "char(501)"))
                .add(new InvalidCastTestCase("c_number_5", "char(501)"))
                .add(new InvalidCastTestCase("c_number_10", "char(501)"))
                .add(new InvalidCastTestCase("c_number_19", "char(501)"))
                .add(new InvalidCastTestCase("c_float", "char(501)"))
                .add(new InvalidCastTestCase("c_float_5", "char(501)"))
                .add(new InvalidCastTestCase("c_binary_float", "char(501)"))
                .add(new InvalidCastTestCase("c_binary_double", "char(501)"))
                .add(new InvalidCastTestCase("c_number_15", "char(501)"))
                .add(new InvalidCastTestCase("c_number_10_2", "char(501)"))
                .add(new InvalidCastTestCase("c_number_30_2", "char(501)"))
                .add(new InvalidCastTestCase("c_date", "char(501)"))
                .add(new InvalidCastTestCase("c_timestamp", "char(501)"))
                .add(new InvalidCastTestCase("c_timestamptz", "char(501)"))
                .add(new InvalidCastTestCase("c_blob", "char(501)"))
                .add(new InvalidCastTestCase("c_raw_200", "char(501)"))

                .add(new InvalidCastTestCase("c_blob", "varchar(50)"))
                .add(new InvalidCastTestCase("c_raw_200", "varchar(50)"))
                .build();
    }

    private static List<CastTestCase> specialCaseNClob()
    {
        // Trino converts clob type to nclob
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_clob", "varchar", "c_clob"))
                .add(new CastTestCase("c_nclob", "varchar", "c_clob"))
                .add(new CastTestCase("c_clob", "varchar", "c_nclob"))
                .add(new CastTestCase("c_nclob", "varchar", "c_nclob"))
                .build();
    }
}
