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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseGpuQueriesTest
        extends AbstractTestQueryFramework
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(NATION, REGION, ORDERS);

    @Test
    public void testAllTypes()
    {
        assertUpdate(
                """
                CREATE TABLE test_gpu_types AS SELECT
                    CAST(true AS boolean) AS col_boolean,
                    CAST(127 AS tinyint) AS col_tinyint,
                    CAST(32767 AS smallint) AS col_smallint,
                    CAST(2147483647 AS integer) AS col_integer,
                    CAST(9223372036854775807 AS bigint) AS col_bigint,
                    CAST(3.14 AS real) AS col_real,
                    CAST(3.141592653589793 AS double) AS col_double,
                    CAST(12.345 AS decimal(5,3)) AS col_decimal,
                    CAST(12345678901234567890123.5678 AS decimal(27,4)) AS col_long_decimal,
                    CAST('abc' AS char(3)) AS col_char,
                    CAST('hello' AS varchar) AS col_varchar,
                    CAST('hi' AS varchar(20)) AS col_varchar_20,
                    DATE '2024-01-01' AS col_date,
                    CAST(TIMESTAMP '2020-02-12 15:03:00' AS timestamp(3)) AS col_timestamp,
                    X'12ab3f' AS col_varbinary,
                    ARRAY[1, 2, 3] AS col_array,
                    MAP(ARRAY['k1', 'k2'], ARRAY[1, 2]) AS col_map,
                    CAST(ROW(1, 'x') AS row(a integer, b varchar)) AS col_row,
                    '' AS dummy
                """,
                1);

        // Verify all supported types execute with GPU
        assertThat(query("SELECT col_boolean FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_tinyint FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_smallint FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_integer FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_bigint FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_real FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_double FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_varchar FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_varchar_20 FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_varbinary FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_decimal FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_long_decimal FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_date FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_timestamp FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_char FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);

        // Verify all unsupported types execute without GPU
        assertThat(query("SELECT col_array FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_map FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_row FROM test_gpu_types"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_types");
    }

    @Test
    public void testDecimalVariants()
    {
        // Trino's Hive connector writes decimals as FIXED_LEN_BYTE_ARRAY; cuDF narrows the on-read
        // type by precision: DECIMAL32 for precision ≤9, DECIMAL64 for 10-18, DECIMAL128 beyond.
        // Trino short-decimal columns (precision ≤18) expect DECIMAL64; long-decimal columns
        // (precision >18) expect DECIMAL128. So d_small (decimal(5,3)) goes through the
        // DECIMAL32→DECIMAL64 widening cast, d_large (decimal(18,4)) is an exact match for
        // DECIMAL64, and d_long (decimal(27,6)) is an exact match for DECIMAL128. Include
        // negative, zero and null values.
        assertUpdate("CREATE TABLE test_gpu_decimals AS SELECT * FROM (VALUES " +
                "(CAST(1.23 AS decimal(5,3)), CAST(123456789.1234 AS decimal(18,4)), CAST(123456789012345678901.234567 AS decimal(27,6))), " +
                "(CAST(-9.999 AS decimal(5,3)), CAST(-999999999999.9999 AS decimal(18,4)), CAST(-999999999999999999999.999999 AS decimal(27,6))), " +
                "(CAST(0 AS decimal(5,3)), CAST(0 AS decimal(18,4)), CAST(0 AS decimal(27,6))), " +
                "(CAST(NULL AS decimal(5,3)), CAST(NULL AS decimal(18,4)), CAST(NULL AS decimal(27,6)))) " +
                "t(d_small, d_large, d_long)", 4);

        // executesWithGpu cross-checks GPU output against CPU execution, so a wrong DECIMAL32→
        // DECIMAL64 cast or a misaligned DECIMAL128 byte order would fail the comparison.
        assertThat(query("SELECT d_small, d_large, d_long FROM test_gpu_decimals"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_decimals");
    }

    @Test
    public void testDateEdgeValues()
    {
        assertUpdate("CREATE TABLE test_gpu_temporal AS SELECT * FROM (VALUES " +
                "(DATE '1970-01-01'), " +
                "(DATE '2024-02-29'), " +
                "(DATE '9999-12-31'), " +
                "(DATE '1900-06-15'), " +
                "(CAST(NULL AS date))) " +
                "t(d)", 5);

        assertThat(query("SELECT d FROM test_gpu_temporal"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_temporal");
    }

    @Test
    public void testMultipleRowsWithInterleavedNulls()
    {
        assertUpdate("CREATE TABLE test_gpu_interleaved_nulls AS SELECT * FROM (VALUES " +
                "(true, CAST(1 AS tinyint), CAST(10 AS smallint), 100, CAST(1000 AS bigint), " +
                "REAL '1.5', DOUBLE '2.5', 'a', CAST(1.23 AS decimal(5,3)), DATE '2024-01-01', CAST(TIMESTAMP '2024-01-01 00:00:00' AS timestamp(3))), " +
                "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                "(false, CAST(-1 AS tinyint), CAST(-10 AS smallint), -100, CAST(-1000 AS bigint), " +
                "REAL '-1.5', DOUBLE '-2.5', '', CAST(-9.999 AS decimal(5,3)), DATE '1970-01-01', CAST(TIMESTAMP '1970-01-01 00:00:00' AS timestamp(3))), " +
                "(NULL, CAST(2 AS tinyint), NULL, 200, NULL, REAL '3.5', NULL, 'c', NULL, " +
                "DATE '2025-06-15', NULL)) " +
                "t(c_bool, c_tiny, c_small, c_int, c_big, c_real, c_double, c_varchar, c_decimal, c_date, c_timestamp)", 4);

        assertThat(query("SELECT * FROM test_gpu_interleaved_nulls"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_interleaved_nulls");
    }

    @Test
    public void testSelectWithNulls()
    {
        assertUpdate("CREATE TABLE test_gpu_nulls AS SELECT " +
                "CAST(NULL AS boolean) AS col_boolean, " +
                "CAST(NULL AS bigint) AS col_bigint, " +
                "CAST(NULL AS double) AS col_double, " +
                "CAST(NULL AS varchar) AS col_varchar", 1);

        assertThat(query("SELECT col_boolean FROM test_gpu_nulls WHERE col_boolean IS NULL"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_bigint FROM test_gpu_nulls WHERE col_bigint IS NULL"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_double FROM test_gpu_nulls WHERE col_double IS NULL"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_varchar FROM test_gpu_nulls WHERE col_varchar IS NULL"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_nulls");
    }

    @Test
    public void testSelectWithMultipleRows()
    {
        assertUpdate("CREATE TABLE test_gpu_multiple_rows AS " +
                "SELECT CAST(orderkey % 3 = 0 AS boolean) AS is_divisible, orderkey, totalprice " +
                "FROM tpch.tiny.orders", 15000);

        assertThat(query("SELECT count(*) FROM test_gpu_multiple_rows WHERE is_divisible"))
                .executesWithGpu(AggregationNode.class);
        assertThat(query("SELECT count(*) FROM test_gpu_multiple_rows WHERE orderkey < 100"))
                .executesWithGpu(AggregationNode.class);
        assertThat(query("SELECT sum(totalprice) FROM test_gpu_multiple_rows"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_multiple_rows");
    }

    @Test
    public void testSelectWithColumnPruning()
    {
        assertUpdate(
                "CREATE TABLE test_gpu_pruning AS " +
                        "SELECT orderkey, orderstatus, totalprice, orderdate FROM tpch.tiny.orders",
                15000);

        // Read only some columns (tests column pruning)
        assertThat(query("SELECT orderkey FROM test_gpu_pruning WHERE orderkey < 10"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT totalprice FROM test_gpu_pruning WHERE orderkey < 10"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_pruning");
    }

    @Test
    public void testEmptyTable()
    {
        // Create table without inserting any data
        assertUpdate("CREATE TABLE test_gpu_empty (" +
                "col_boolean boolean, " +
                "col_bigint bigint, " +
                "col_double double, " +
                "col_varchar varchar)");

        // Verify empty table queries execute with GPU
        assertThat(query("SELECT * FROM test_gpu_empty"))
                .executesWithGpu(TableScanNode.class)
                .returnsEmptyResult();
        assertThat(query("SELECT col_bigint FROM test_gpu_empty"))
                .executesWithGpu(TableScanNode.class)
                .returnsEmptyResult();
        assertThat(query("SELECT count(*) FROM test_gpu_empty"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES BIGINT '0'");

        assertUpdate("DROP TABLE test_gpu_empty");
    }

    @Test
    public void testNestedColumnProjection()
    {
        assertUpdate("CREATE TABLE test_gpu_nested AS SELECT CAST(ROW(1, 'x') AS ROW(a integer, b varchar)) AS col_row", 1);

        assertThat(query("SELECT col_row.a FROM test_gpu_nested"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_row.a, col_row.b FROM test_gpu_nested"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_nested");
    }
}
