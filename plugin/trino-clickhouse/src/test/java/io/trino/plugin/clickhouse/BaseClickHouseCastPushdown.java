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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class BaseClickHouseCastPushdown
        extends BaseJdbcCastPushdownTest
{
    // https://clickhouse.com/docs/sql-reference/data-types/datetime
    protected static final String MIN_SUPPORTED_DATE_VALUE = "1970-01-01";
    protected static final String MAX_SUPPORTED_DATE_VALUE = "2149-06-06";
    // https://clickhouse.com/docs/sql-reference/data-types/date32 (Supports the date range same with DateTime64)
    protected static final String MIN_SUPPORTED_DATE32_VALUE = "1900-01-01";
    protected static final String MAX_SUPPORTED_DATE32_VALUE = "2299-12-31";
    // https://clickhouse.com/docs/sql-reference/data-types/datetime
    private static final String MIN_SUPPORTED_DATETIME_VALUE = "1970-01-01 00:00:00";
    private static final String MAX_SUPPORTED_DATETIME_VALUE = "2106-02-07 06:28:15";
    // https://clickhouse.com/docs/sql-reference/data-types/datetime64
    private static final String MIN_SUPPORTED_DATETIME64_VALUE = "1900-01-01 00:00:00";
    private static final String MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8 = "2299-12-31 23:59:59.99999999";
    private static final String MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9 = "2262-04-11 23:47:16.854775807";

    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;
    protected TestingClickHouseServer clickhouseServer;

    protected abstract DockerImageName clickHouseServerImage();

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return new ClickHouseSqlExecutor(clickhouseServer::execute);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(clickHouseServerImage()));
        return ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("unsupported-type-handling", "CONVERT_TO_VARCHAR")
                .addConnectorProperty("join-pushdown.enabled", "true")
                .addConnectorProperty("clickhouse.allow-timestamp-unsafe-cast-pushdown", "true")
                .build();
    }

    @BeforeAll
    public void setup()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "Int32", asList(11, 12, 13))
                .addColumn("c_int", "Int32", asList(11, 12, null))
                .addColumn("c_float32", "Float32", asList(11.11, 12.12, null))
                .addColumn("c_fixed_string", "FixedString(5)", asList("'abc'", "'def'", null))
                .addColumn("c_date", "Nullable(date)", asList("'2024-09-08'", "'2019-08-15'", null))
                .addColumn("c_date32", "Nullable(date32)", asList("'2024-09-08'", "'2019-08-15'", null)) // Supports the date range same with DateTime64
                .addColumn("c_datetime", "Nullable(datetime)", asList("'2024-09-08 01:02:03'", "'2019-08-15 09:08:07'", null))
                .addColumn("c_datetime64", "Nullable(datetime64)", asList("'2024-09-08 01:02:03'", "'2019-08-15 09:08:07'", null)) // map to datetime64(3)
                .addColumn("c_datetime64_0", "Nullable(datetime64(0))", asList("'2024-09-08 01:02:03'", "'2019-08-15 09:08:07'", null))
                .addColumn("c_datetime64_1", "Nullable(datetime64(1))", asList("'2024-09-08 01:02:03.9'", "'2019-08-15 09:08:07.1'", null))
                .addColumn("c_datetime64_2", "Nullable(datetime64(2))", asList("'2024-09-08 01:02:03.98'", "'2019-08-15 09:08:07.12'", null))
                .addColumn("c_datetime64_3", "Nullable(datetime64(3))", asList("'2024-09-08 01:02:03.987'", "'2019-08-15 09:08:07.123'", null))
                .addColumn("c_datetime64_4", "Nullable(datetime64(4))", asList("'2024-09-08 01:02:03.9876'", "'2019-08-15 09:08:07.1234'", null))
                .addColumn("c_datetime64_5", "Nullable(datetime64(5))", asList("'2024-09-08 01:02:03.98765'", "'2019-08-15 09:08:07.12345'", null))
                .addColumn("c_datetime64_6", "Nullable(datetime64(6))", asList("'2024-09-08 01:02:03.987654'", "'2019-08-15 09:08:07.123456'", null))
                .addColumn("c_datetime64_7", "Nullable(datetime64(7))", asList("'2024-09-08 01:02:03.9876543'", "'2019-08-15 09:08:07.1234567'", null))
                .addColumn("c_datetime64_8", "Nullable(datetime64(8))", asList("'2024-09-08 01:02:03.98765432'", "'2019-08-15 09:08:07.12345678'", null))
                .addColumn("c_datetime64_9", "Nullable(datetime64(9))", asList("'2024-09-08 01:02:03.987654321'", "'2019-08-15 09:08:07.123456789'", null))

                .addColumn("c_datetime_tz", "datetime('Asia/Kolkata')", asList("TIMESTAMP '2024-09-08 01:02:03'", "TIMESTAMP '2019-08-15 09:08:07'", null))
                .addColumn("c_datetime64_tz", "datetime64(0, 'Asia/Kolkata')", asList("TIMESTAMP '2024-09-08 01:02:03'", "TIMESTAMP '2019-08-15 09:08:07'", null))

                .execute(onRemoteDatabase(), "tpch.left_table_"));

        // 2nd row value is different in right table than left table
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "Int32", asList(11, 22, 13))
                .addColumn("c_int", "Int32", asList(11, 22, null))
                .addColumn("c_float32", "Float32", asList(11.11, 22.22, null))
                .addColumn("c_fixed_string", "FixedString(5)", asList("'abc'", "'ghi'", null))
                .addColumn("c_date", "Nullable(date)", asList("'2024-09-08'", "'2010-08-15'", null))
                .addColumn("c_date32", "Nullable(date32)", asList("'2024-09-08'", "'2010-08-15'", null))
                .addColumn("c_datetime", "Nullable(datetime)", asList("'2024-09-08 01:02:03'", "'2010-08-15 09:08:07'", null))
                .addColumn("c_datetime64", "Nullable(datetime64)", asList("'2024-09-08 01:02:03'", "'2010-08-15 09:08:07'", null)) // map to datetime64(3)
                .addColumn("c_datetime64_0", "Nullable(datetime64(0))", asList("'2024-09-08 01:02:03'", "'2010-08-15 09:08:07'", null))
                .addColumn("c_datetime64_1", "Nullable(datetime64(1))", asList("'2024-09-08 01:02:03.9'", "'2010-08-15 09:08:07.1'", null))
                .addColumn("c_datetime64_2", "Nullable(datetime64(2))", asList("'2024-09-08 01:02:03.98'", "'2010-08-15 09:08:07.12'", null))
                .addColumn("c_datetime64_3", "Nullable(datetime64(3))", asList("'2024-09-08 01:02:03.987'", "'2010-08-15 09:08:07.123'", null))
                .addColumn("c_datetime64_4", "Nullable(datetime64(4))", asList("'2024-09-08 01:02:03.9876'", "'2010-08-15 09:08:07.1234'", null))
                .addColumn("c_datetime64_5", "Nullable(datetime64(5))", asList("'2024-09-08 01:02:03.98765'", "'2010-08-15 09:08:07.12345'", null))
                .addColumn("c_datetime64_6", "Nullable(datetime64(6))", asList("'2024-09-08 01:02:03.987654'", "'2010-08-15 09:08:07.123456'", null))
                .addColumn("c_datetime64_7", "Nullable(datetime64(7))", asList("'2024-09-08 01:02:03.9876543'", "'2010-08-15 09:08:07.1234567'", null))
                .addColumn("c_datetime64_8", "Nullable(datetime64(8))", asList("'2024-09-08 01:02:03.98765432'", "'2010-08-15 09:08:07.12345678'", null))
                .addColumn("c_datetime64_9", "Nullable(datetime64(9))", asList("'2024-09-08 01:02:03.987654321'", "'2010-08-15 09:08:07.123456789'", null))

                .addColumn("c_datetime_tz", "Nullable(datetime('Asia/Kolkata'))", asList("TIMESTAMP '2024-09-08 01:02:03'", "TIMESTAMP '2010-08-15 09:08:07'", null))
                .addColumn("c_datetime64_tz", "Nullable(datetime64(0, 'Asia/Kolkata'))", asList("TIMESTAMP '2024-09-08 01:02:03'", "TIMESTAMP '2010-08-15 09:08:07'", null))

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
    @Test
    public void testJoinPushdownWithCast()
    {
        // Join pushdown is not supported?
    }

    @Test
    void testMinAndMaxDateCastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_date", "date", asList("'%s'".formatted(MIN_SUPPORTED_DATE_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATE_VALUE)))
                .execute(onRemoteDatabase(), "tpch.min_max_date_")) {
            assertThat(query("SELECT CAST(c_date AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES (DATE '%s'), (DATE '%s')".formatted(MIN_SUPPORTED_DATE_VALUE, MAX_SUPPORTED_DATE_VALUE))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE_VALUE, "2149-06-06 00:00:00.000"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE_VALUE, MAX_SUPPORTED_DATE_VALUE))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testMinAndMaxDate32CastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_date32", "date32", asList("'%s'".formatted(MIN_SUPPORTED_DATE32_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATE32_VALUE)))
                .execute(onRemoteDatabase(), "tpch.min_max_date32_")) {
            assertThat(query("SELECT CAST(c_date32 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '%s', DATE '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, MAX_SUPPORTED_DATE32_VALUE))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date32 AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, "2299-12-31 00:00:00.000"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_date32 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, MAX_SUPPORTED_DATE32_VALUE))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testMinAndMaxDateTimeCastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_datetime", "datetime", asList("'%s'".formatted(MIN_SUPPORTED_DATETIME_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATETIME_VALUE)))
                .execute(onRemoteDatabase(), "tpch.min_max_datetime_")) {
            assertThat(query("SELECT CAST(c_datetime AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '%s', DATE '%s'".formatted("1970-01-01", "2106-02-07"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_datetime AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME_VALUE, "2106-02-07 06:28:15.000"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_datetime AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME_VALUE, MAX_SUPPORTED_DATETIME_VALUE))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testMinAndMaxDateTime64With8PrecisionCastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_datetime64_8", "datetime64(8)", asList("'%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8)))
                .execute(onRemoteDatabase(), "tpch.min_max_date32_")) {
            assertThat(query("SELECT CAST(c_datetime64_8 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '%s', DATE '%s'".formatted("1900-01-01", "2299-12-31"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_datetime64_8 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '%s', DATE '%s'".formatted("1900-01-01", "2299-12-31"))
                    .isFullyPushedDown();

            Session sessionWithoutPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                    .build();

            // The maximum representable value for DateTime64 is '2299-12-31 23:59:59.99999999'.
            // To avoid overflow, any timestamp value greater than or equal to '2299-12-31 23:59:59.5'
            // is not rounded up during cast pushdown. This prevents the cast from producing
            // an invalid rounded value of '2300-01-01 00:00:00'.
            assertThat(query("SELECT CAST(c_datetime64_8 AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE, "2299-12-31 23:59:59.999"));
            assertThat(query(sessionWithoutPushdown, "SELECT CAST(c_datetime64_8 AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE, "2300-01-01 00:00:00.000"));

            assertThat(query("SELECT CAST(c_datetime64_8 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE, "2299-12-31 23:59:59"));
            assertThat(query(sessionWithoutPushdown, "SELECT CAST(c_datetime64_8 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE, "2300-01-01 00:00:00"));
        }
    }

    @Test
    void testMinAndMaxDateTime64With9PrecisionCastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_datetime64_9", "datetime64(9)", asList("'%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9)))
                .execute(onRemoteDatabase(), "tpch.min_max_date32_")) {
            assertThat(query("SELECT CAST(c_datetime64_9 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '%s', DATE '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, "2262-04-11"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_datetime64_9 AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE, "2262-04-11 23:47:16.855"))
                    .isFullyPushedDown();

            assertThat(query("SELECT CAST(c_datetime64_9 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATETIME64_VALUE, "2262-04-11 23:47:17"))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testDateTime64CastPushdownMaxEdge()
    {
        // The maximum DateTime64 value that remains within ClickHouse's representable range after rounding
        assertDateTime64CastPushdownMaxEdge(0, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59");
        assertDateTime64CastPushdownMaxEdge(1, "2299-12-31 23:59:59.94999999", "2299-12-31 23:59:59.9");
        assertDateTime64CastPushdownMaxEdge(2, "2299-12-31 23:59:59.99499999", "2299-12-31 23:59:59.99");
        assertDateTime64CastPushdownMaxEdge(3, "2299-12-31 23:59:59.99949999", "2299-12-31 23:59:59.999");
        assertDateTime64CastPushdownMaxEdge(4, "2299-12-31 23:59:59.99994999", "2299-12-31 23:59:59.9999");
        assertDateTime64CastPushdownMaxEdge(5, "2299-12-31 23:59:59.99999499", "2299-12-31 23:59:59.99999");
        assertDateTime64CastPushdownMaxEdge(6, "2299-12-31 23:59:59.99999949", "2299-12-31 23:59:59.999999");
        assertDateTime64CastPushdownMaxEdge(7, "2299-12-31 23:59:59.99999994", "2299-12-31 23:59:59.9999999");
        assertDateTime64CastPushdownMaxEdge(8, "2299-12-31 23:59:59.99999994", "2299-12-31 23:59:59.99999994");

        assertDateTime64CastPushdownMaxEdge(0, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59");
        assertDateTime64CastPushdownMaxEdge(1, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.5");
        assertDateTime64CastPushdownMaxEdge(2, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.50");
        assertDateTime64CastPushdownMaxEdge(3, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.500");
        assertDateTime64CastPushdownMaxEdge(4, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.5000");
        assertDateTime64CastPushdownMaxEdge(5, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.50000");
        assertDateTime64CastPushdownMaxEdge(6, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.500000");
        assertDateTime64CastPushdownMaxEdge(7, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.5000000");
        assertDateTime64CastPushdownMaxEdge(8, "2299-12-31 23:59:59.49999999", "2299-12-31 23:59:59.49999999");

        assertDateTime64CastPushdownMaxEdge(1, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.5");
        assertDateTime64CastPushdownMaxEdge(2, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.55");
        assertDateTime64CastPushdownMaxEdge(3, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.550");
        assertDateTime64CastPushdownMaxEdge(4, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.5500");
        assertDateTime64CastPushdownMaxEdge(5, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.55000");
        assertDateTime64CastPushdownMaxEdge(6, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.550000");
        assertDateTime64CastPushdownMaxEdge(7, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.5500000");
        assertDateTime64CastPushdownMaxEdge(8, "2299-12-31 23:59:59.54999999", "2299-12-31 23:59:59.54999999");

        assertDateTime64CastPushdownMaxEdge(1, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.5");
        assertDateTime64CastPushdownMaxEdge(2, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.54");
        assertDateTime64CastPushdownMaxEdge(3, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.544");
        assertDateTime64CastPushdownMaxEdge(4, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.5444");
        assertDateTime64CastPushdownMaxEdge(5, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.54444");
        assertDateTime64CastPushdownMaxEdge(6, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.544444");
        assertDateTime64CastPushdownMaxEdge(7, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.5444445");
        assertDateTime64CastPushdownMaxEdge(8, "2299-12-31 23:59:59.54444445", "2299-12-31 23:59:59.54444445");
    }

    private void assertDateTime64CastPushdownMaxEdge(int targetPrecision, String actualValue, String expectedValue)
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(1)
                .addColumn("c_datetime64", "DateTime64(8)", asList("'%s'".formatted(actualValue)))
                .execute(onRemoteDatabase(), "tpch.datetime64_rounding_")) {
            assertThat(query("SELECT CAST(c_datetime64 AS timestamp(%d)) FROM %s".formatted(targetPrecision, table.getName())))
                    .matches("VALUES TIMESTAMP '%s'".formatted(expectedValue))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testDateTime64CastPushdownNoRoundUpWhenOverflow()
    {
        // The minimum DateTime64 value which after rounding to the target precision exceeds ClickHouse's maximum representable value of '2299-12-31 23:59:59.99999999'.
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.50000000", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(1, "2299-12-31 23:59:59.95000000", "2299-12-31 23:59:59.9", "2300-01-01 00:00:00.0");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(2, "2299-12-31 23:59:59.99500000", "2299-12-31 23:59:59.99", "2300-01-01 00:00:00.00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(3, "2299-12-31 23:59:59.99950000", "2299-12-31 23:59:59.999", "2300-01-01 00:00:00.000");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(4, "2299-12-31 23:59:59.99995000", "2299-12-31 23:59:59.9999", "2300-01-01 00:00:00.0000");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(5, "2299-12-31 23:59:59.99999500", "2299-12-31 23:59:59.99999", "2300-01-01 00:00:00.00000");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(6, "2299-12-31 23:59:59.99999950", "2299-12-31 23:59:59.999999", "2300-01-01 00:00:00.000000");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(7, "2299-12-31 23:59:59.99999995", "2299-12-31 23:59:59.9999999", "2300-01-01 00:00:00.0000000");

        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.5", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.55", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.555", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.5555", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.55555", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.555555", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.5555555", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
        assertDateTime64CastPushdownNoRoundUpWhenOverflow(0, "2299-12-31 23:59:59.55555555", "2299-12-31 23:59:59", "2300-01-01 00:00:00");
    }

    private void assertDateTime64CastPushdownNoRoundUpWhenOverflow(int targetPrecision, String actualValue, String expectedClickHouseValue, String expectedTrinoValue)
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(1)
                .addColumn("c_datetime64", "DateTime64(8)", asList("'%s'".formatted(actualValue)))
                .execute(onRemoteDatabase(), "tpch.datetime64_rounding_")) {
            Session sessionWithoutPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                    .build();

            assertThat(query("SELECT CAST(c_datetime64 AS timestamp(%d)) FROM %s".formatted(targetPrecision, table.getName())))
                    .matches("VALUES TIMESTAMP '%s'".formatted(expectedClickHouseValue));
            assertThat(query(sessionWithoutPushdown, "SELECT CAST(c_datetime64 AS timestamp(%d)) FROM %s".formatted(targetPrecision, table.getName())))
                    .matches("VALUES TIMESTAMP '%s'".formatted(expectedTrinoValue));
        }
    }

    @Test
    void testTimestampToDateCastPushdownWithGroupBy()
    {
        assertThat(query("SELECT CAST(c_datetime AS date) as c_date, count(*) FROM %s GROUP BY CAST(c_datetime AS date)".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08', BIGINT '1'), (DATE '2019-08-15', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT CAST(c_datetime64 AS date) as c_date, count(*) FROM %s GROUP BY CAST(c_datetime64 AS date)".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08', BIGINT '1'), (DATE '2019-08-15', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT CAST(c_datetime64_5 AS date) as c_date, count(*) FROM %s GROUP BY CAST(c_datetime64_5 AS date)".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08', BIGINT '1'), (DATE '2019-08-15', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
    }

    @Test
    void testTimestampToDateCastPushdownUsingDateFunctionWithGroupBy()
    {
        assertThat(query("SELECT date(c_datetime) as c_date, count(*) FROM %s GROUP BY date(c_datetime)".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08', BIGINT '1'), (DATE '2019-08-15', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT date(c_datetime64) as c_date, count(*) FROM %s GROUP BY date(c_datetime64)".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08', BIGINT '1'), (DATE '2019-08-15', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT date(c_datetime64_5) as c_date, count(*) FROM %s GROUP BY date(c_datetime64_5)".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08', BIGINT '1'), (DATE '2019-08-15', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
    }

    @Test
    void testTimestampToTimestampCastPushdownWithGroupBy()
    {
        assertThat(query("SELECT CAST(c_datetime AS timestamp(1)) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_datetime AS timestamp(1))".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.0', BIGINT '1'), (TIMESTAMP '2019-08-15 09:08:07.0', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT CAST(c_datetime64 AS timestamp(2)) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_datetime64 AS timestamp(2))".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.00', BIGINT '1'), (TIMESTAMP '2019-08-15 09:08:07.00', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT CAST(c_datetime64_3 AS timestamp(9)) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_datetime64_3 AS timestamp(9))".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987000000', BIGINT '1'), (TIMESTAMP '2019-08-15 09:08:07.123000000', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT CAST(c_datetime64_9 AS timestamp(6)) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_datetime64_9 AS timestamp(6))".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987654', BIGINT '1'), (TIMESTAMP '2019-08-15 09:08:07.123457', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
    }

    @Test
    void testDateToTimestampCastPushdownWithGroupBy()
    {
        assertThat(query("SELECT CAST(c_date AS timestamp) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_date AS timestamp)".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 00:00:00.000', BIGINT '1'), (TIMESTAMP '2019-08-15 00:00:00.000', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query("SELECT CAST(c_date32 AS timestamp(0)) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_date32 AS timestamp(0))".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 00:00:00', BIGINT '1'), (TIMESTAMP '2019-08-15 00:00:00', BIGINT '1'), (null, BIGINT '1')")
                .isFullyPushedDown();
    }

    @Test
    void testCastPushdownWithPredicate()
    {
        assertThat(query("SELECT c_datetime FROM %s WHERE CAST(c_datetime AS DATE) = DATE '2024-09-08'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime64 FROM %s WHERE CAST(c_datetime64 AS DATE) = DATE '2024-09-08'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.000')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime64_3 FROM %s WHERE CAST(c_datetime64_3 AS DATE) = DATE '2024-09-08'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987')")
                .isFullyPushedDown();

        assertThat(query("SELECT c_datetime64 FROM %s WHERE CAST(c_datetime64 AS DATE) IS NULL".formatted(leftTable())))
                .matches("VALUES CAST(null AS TIMESTAMP)")
                .isNotFullyPushedDown(FilterNode.class); // filterPredicate = (CAST(c_datetime64 AS date) IS NULL)

        assertThat(query("SELECT c_date FROM %s WHERE CAST(c_date AS TIMESTAMP) = TIMESTAMP '2024-09-08 00:00:00'".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_date32 FROM %s WHERE CAST(c_date32 AS TIMESTAMP) = TIMESTAMP '2024-09-08 00:00:00'".formatted(leftTable())))
                .matches("VALUES (DATE '2024-09-08')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime FROM %s WHERE CAST(c_datetime AS TIMESTAMP) = TIMESTAMP '2024-09-08 01:02:03'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime64 FROM %s WHERE CAST(c_datetime64 AS TIMESTAMP) = TIMESTAMP '2024-09-08 01:02:03'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.000')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime64 FROM %s WHERE CAST(c_datetime64 AS TIMESTAMP(0)) = TIMESTAMP '2024-09-08 01:02:03'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.000')")
                .isNotFullyPushedDown(FilterNode.class); // filterPredicate = (CAST(c_datetime64 AS timestamp(0)) = timestamp(0) '2024-09-08 01:02:03')

        // higher to lower precision
        assertThat(query("SELECT c_datetime64_9 FROM %s WHERE CAST(c_datetime64_9 AS TIMESTAMP(0)) = TIMESTAMP '2024-09-08 01:02:04'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987654321')")
                .isNotFullyPushedDown(FilterNode.class); // filterPredicate = (CAST(c_datetime64_9 AS timestamp(0)) = timestamp(0) '2024-09-08 01:02:04')
        assertThat(query("SELECT c_datetime64_9 FROM %s WHERE CAST(c_datetime64_9 AS TIMESTAMP(3)) = TIMESTAMP '2024-09-08 01:02:03.988'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987654321')")
                .isNotFullyPushedDown(FilterNode.class); // filterPredicate = (CAST(c_datetime64_9 AS timestamp(3)) = timestamp(3) '2024-09-08 01:02:03.988')

        // lower to higher precision
        assertThat(query("SELECT c_datetime64_3 FROM %s WHERE CAST(c_datetime64_3 AS TIMESTAMP(6)) = TIMESTAMP '2024-09-08 01:02:03.987'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987')")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime64_3 FROM %s WHERE CAST(c_datetime64_3 AS TIMESTAMP(9)) = TIMESTAMP '2024-09-08 01:02:03.987'".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 01:02:03.987')")
                .isFullyPushedDown();

        assertThat(query("SELECT c_datetime64 FROM %s WHERE CAST(c_datetime64 AS TIMESTAMP) IS NULL".formatted(leftTable())))
                .matches("VALUES CAST(null AS TIMESTAMP)")
                .isFullyPushedDown();
        assertThat(query("SELECT c_datetime64_3 FROM %s WHERE CAST(c_datetime64_3 AS TIMESTAMP(0)) IS NULL".formatted(leftTable())))
                .matches("VALUES CAST(null AS TIMESTAMP)")
                .isNotFullyPushedDown(FilterNode.class); // filterPredicate = (CAST(c_datetime64_3 AS timestamp(0)) IS NULL)
        assertThat(query("SELECT c_datetime64_3 FROM %s WHERE CAST(c_datetime64_3 AS TIMESTAMP(9)) IS NULL".formatted(leftTable())))
                .matches("VALUES CAST(null AS TIMESTAMP)")
                .isNotFullyPushedDown(FilterNode.class); // filterPredicate = (CAST(c_datetime64_3 AS timestamp(9)) IS NULL)
    }

    @Test
    void testCastPushdownDisabled()
    {
        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutPushdown, "SELECT CAST (c_datetime64 AS date) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query(sessionWithoutPushdown, "SELECT CAST (c_datetime64_0 AS timestamp) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testUnsafeCastPushdownDisabled()
    {
        // The minimum DateTime64 value which after rounding to the target precision exceeds ClickHouse's maximum representable value of '2299-12-31 23:59:59.99999999'.
        assertUnsafeCastNotPushdown(0, "2299-12-31 23:59:59.50000000", "2300-01-01 00:00:00");
        assertUnsafeCastNotPushdown(1, "2299-12-31 23:59:59.95000000", "2300-01-01 00:00:00.0");
        assertUnsafeCastNotPushdown(2, "2299-12-31 23:59:59.99500000", "2300-01-01 00:00:00.00");
        assertUnsafeCastNotPushdown(3, "2299-12-31 23:59:59.99950000", "2300-01-01 00:00:00.000");
        assertUnsafeCastNotPushdown(4, "2299-12-31 23:59:59.99995000", "2300-01-01 00:00:00.0000");
        assertUnsafeCastNotPushdown(5, "2299-12-31 23:59:59.99999500", "2300-01-01 00:00:00.00000");
        assertUnsafeCastNotPushdown(6, "2299-12-31 23:59:59.99999950", "2300-01-01 00:00:00.000000");
        assertUnsafeCastNotPushdown(7, "2299-12-31 23:59:59.99999995", "2300-01-01 00:00:00.0000000");

        // Cast pushdown for timestamp downcasts is disabled unless allow_unsafe_cast_pushdown is enabled, even if values are within ClickHouse’s range.
        assertUnsafeCastNotPushdown(0, "2025-01-01 01:01:01", "2025-01-01 01:01:01");
        assertUnsafeCastNotPushdown(1, "2025-01-01 01:01:01.0", "2025-01-01 01:01:01.0");
        assertUnsafeCastNotPushdown(2, "2025-01-01 01:01:01.00", "2025-01-01 01:01:01.00");
        assertUnsafeCastNotPushdown(3, "2025-01-01 01:01:01.000", "2025-01-01 01:01:01.000");
        assertUnsafeCastNotPushdown(4, "2025-01-01 01:01:01.0000", "2025-01-01 01:01:01.0000");
        assertUnsafeCastNotPushdown(5, "2025-01-01 01:01:01.00000", "2025-01-01 01:01:01.00000");
        assertUnsafeCastNotPushdown(6, "2025-01-01 01:01:01.000000", "2025-01-01 01:01:01.000000");
        assertUnsafeCastNotPushdown(7, "2025-01-01 01:01:01.0000000", "2025-01-01 01:01:01.0000000");
    }

    private void assertUnsafeCastNotPushdown(int targetPrecision, String actualValue, String expectedValue)
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(1)
                .addColumn("c_datetime64_8", "DateTime64(8)", asList("'%s'".formatted(actualValue)))
                .execute(onRemoteDatabase(), "tpch.datetime64_unsafe_pushdown_")) {
            Session unsafePushdownDisabled = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "allow_timestamp_unsafe_cast_pushdown", "false")
                    .build();
            assertThat(query(unsafePushdownDisabled, "SELECT CAST(c_datetime64_8 AS timestamp(%d)) FROM %s".formatted(targetPrecision, table.getName())))
                    .matches("VALUES TIMESTAMP '%s'".formatted(expectedValue))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_date", "date", "c_date"))
                .add(new CastTestCase("c_date", "date", "c_date32"))

                .add(new CastTestCase("c_date32", "date", "c_date"))
                .add(new CastTestCase("c_date32", "date", "c_date32"))

                .addAll(addCastFromDateToTimestampTestCases())

                .add(new CastTestCase("c_datetime", "date", "c_date"))
                .add(new CastTestCase("c_datetime", "date", "c_date32"))
                .add(new CastTestCase("c_datetime64", "date", "c_date32"))
                .add(new CastTestCase("c_datetime64", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_0", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_1", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_2", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_3", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_4", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_5", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_6", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_7", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_8", "date", "c_date"))
                .add(new CastTestCase("c_datetime64_9", "date", "c_date"))

                .add(new CastTestCase("c_datetime", "timestamp", "c_datetime"))
                .add(new CastTestCase("c_datetime", "timestamp(0)", "c_datetime64_0"))
                .add(new CastTestCase("c_datetime", "timestamp(1)", "c_datetime64_1"))
                .add(new CastTestCase("c_datetime", "timestamp(2)", "c_datetime64_2"))
                .add(new CastTestCase("c_datetime", "timestamp(3)", "c_datetime64_3"))
                .add(new CastTestCase("c_datetime", "timestamp(4)", "c_datetime64_4"))
                .add(new CastTestCase("c_datetime", "timestamp(5)", "c_datetime64_5"))
                .add(new CastTestCase("c_datetime", "timestamp(6)", "c_datetime64_6"))
                .add(new CastTestCase("c_datetime", "timestamp(7)", "c_datetime64_7"))
                .add(new CastTestCase("c_datetime", "timestamp(8)", "c_datetime64_8"))
                .add(new CastTestCase("c_datetime", "timestamp(9)", "c_datetime64_9"))

                // casts to higher precision
                .add(new CastTestCase("c_datetime64_0", "timestamp(1)", "c_datetime64_1"))
                .add(new CastTestCase("c_datetime64_1", "timestamp(2)", "c_datetime64_2"))
                .add(new CastTestCase("c_datetime64_2", "timestamp(3)", "c_datetime64_3"))
                .add(new CastTestCase("c_datetime64_3", "timestamp(4)", "c_datetime64_4"))
                .add(new CastTestCase("c_datetime64_4", "timestamp(5)", "c_datetime64_5"))
                .add(new CastTestCase("c_datetime64_5", "timestamp(6)", "c_datetime64_6"))
                .add(new CastTestCase("c_datetime64_6", "timestamp(7)", "c_datetime64_7"))
                .add(new CastTestCase("c_datetime64_7", "timestamp(8)", "c_datetime64_8"))
                .add(new CastTestCase("c_datetime64_8", "timestamp(9)", "c_datetime64_9"))

                // cast to lower precision
                // trino rounds, but clickhouse truncates the value while casting to lower precision
                .add(new CastTestCase("c_datetime64_1", "timestamp(0)", "c_datetime64_0"))
                .add(new CastTestCase("c_datetime64_2", "timestamp(1)", "c_datetime64_1"))
                .add(new CastTestCase("c_datetime64_3", "timestamp(2)", "c_datetime64_2"))
                .add(new CastTestCase("c_datetime64_4", "timestamp(3)", "c_datetime64_3"))
                .add(new CastTestCase("c_datetime64_5", "timestamp(4)", "c_datetime64_4"))
                .add(new CastTestCase("c_datetime64_6", "timestamp(5)", "c_datetime64_5"))
                .add(new CastTestCase("c_datetime64_7", "timestamp(6)", "c_datetime64_6"))
                .add(new CastTestCase("c_datetime64_8", "timestamp(7)", "c_datetime64_7"))
                .add(new CastTestCase("c_datetime64_9", "timestamp(8)", "c_datetime64_8"))

                .build();
    }

    protected List<CastTestCase> addCastFromDateToTimestampTestCases()
    {
        // Separating test case which cast from Date to DateTime64 because there is issue with clickhouse version < 25.8
        // https://github.com/ClickHouse/ClickHouse/pull/83982
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_date", "timestamp", "c_datetime"))
                .add(new CastTestCase("c_date", "timestamp(0)", "c_datetime64_0"))
                .add(new CastTestCase("c_date", "timestamp(1)", "c_datetime64_1"))
                .add(new CastTestCase("c_date", "timestamp(2)", "c_datetime64_2"))
                .add(new CastTestCase("c_date", "timestamp(3)", "c_datetime64_3"))
                .add(new CastTestCase("c_date", "timestamp(4)", "c_datetime64_4"))
                .add(new CastTestCase("c_date", "timestamp(5)", "c_datetime64_5"))
                .add(new CastTestCase("c_date", "timestamp(6)", "c_datetime64_6"))
                .add(new CastTestCase("c_date", "timestamp(7)", "c_datetime64_7"))
                .add(new CastTestCase("c_date", "timestamp(8)", "c_datetime64_8"))
                .add(new CastTestCase("c_date", "timestamp(9)", "c_datetime64_9"))

                .add(new CastTestCase("c_date32", "timestamp", "c_datetime"))
                .add(new CastTestCase("c_date32", "timestamp(0)", "c_datetime64_0"))
                .add(new CastTestCase("c_date32", "timestamp(1)", "c_datetime64_1"))
                .add(new CastTestCase("c_date32", "timestamp(2)", "c_datetime64_2"))
                .add(new CastTestCase("c_date32", "timestamp(3)", "c_datetime64_3"))
                .add(new CastTestCase("c_date32", "timestamp(4)", "c_datetime64_4"))
                .add(new CastTestCase("c_date32", "timestamp(5)", "c_datetime64_5"))
                .add(new CastTestCase("c_date32", "timestamp(6)", "c_datetime64_6"))
                .add(new CastTestCase("c_date32", "timestamp(7)", "c_datetime64_7"))
                .add(new CastTestCase("c_date32", "timestamp(8)", "c_datetime64_8"))
                .add(new CastTestCase("c_date32", "timestamp(9)", "c_datetime64_9"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_int", "double", "c_float32"))
                .add(new CastTestCase("c_date", "timestamp with time zone", "c_datetime64_tz"))
                .add(new CastTestCase("c_datetime", "timestamp with time zone", "c_datetime64_tz"))
                .add(new CastTestCase("c_datetime64", "timestamp with time zone", "c_datetime64_tz"))
                .add(new CastTestCase("c_datetime_tz", "timestamp with time zone", "c_datetime64_tz"))
                .add(new CastTestCase("c_datetime_tz", "timestamp", "c_datetime64"))
                .add(new CastTestCase("c_datetime64_tz", "timestamp with time zone", "c_datetime64_tz"))
                .add(new CastTestCase("c_datetime64_tz", "timestamp", "c_datetime64"))
                .build();
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.<InvalidCastTestCase>builder()
                .add(new InvalidCastTestCase("c_int", "date"))
                .build();
    }
}
