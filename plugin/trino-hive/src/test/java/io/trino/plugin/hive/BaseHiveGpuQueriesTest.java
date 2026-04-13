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
package io.trino.plugin.hive;

import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHiveGpuQueriesTest
        extends AbstractTestQueryFramework
{
    @Test
    public void testAllTypes()
    {
        // Create table with all types (both supported and unsupported)
        assertUpdate("CREATE TABLE test_gpu_types AS SELECT " +
                "CAST(true AS boolean) AS col_boolean, " +
                "CAST(127 AS tinyint) AS col_tinyint, " +
                "CAST(32767 AS smallint) AS col_smallint, " +
                "CAST(2147483647 AS integer) AS col_integer, " +
                "CAST(9223372036854775807 AS bigint) AS col_bigint, " +
                "CAST(3.14 AS real) AS col_real, " +
                "CAST(3.141592653589793 AS double) AS col_double, " +
                "CAST('hello' AS varchar) AS col_varchar, " +
                "CAST(12.345 AS decimal(5,3)) AS col_decimal, " +
                "CAST(12345678901234567890123.5678 AS decimal(27,4)) AS col_long_decimal, " +
                "DATE '2024-01-01' AS col_date, " +
                "CAST(TIMESTAMP '2020-02-12 15:03:00' AS timestamp(3)) AS col_timestamp, " +
                "X'12ab3f' AS col_varbinary, " +
                "CAST('abc' AS char(3)) AS col_char", 1);

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

        // Verify all unsupported types execute without GPU
        assertThat(query("SELECT col_decimal FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_long_decimal FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_date FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_timestamp FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_varbinary FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_char FROM test_gpu_types"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_types");
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
                .executesWithoutGpu();
        assertThat(query("SELECT col_bigint FROM test_gpu_nulls WHERE col_bigint IS NULL"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_double FROM test_gpu_nulls WHERE col_double IS NULL"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_varchar FROM test_gpu_nulls WHERE col_varchar IS NULL"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_nulls");
    }

    @Test
    public void testSelectWithMultipleRows()
    {
        assertUpdate("CREATE TABLE test_gpu_multiple_rows AS " +
                "SELECT CAST(orderkey % 3 = 0 AS boolean) AS is_divisible, orderkey, totalprice " +
                "FROM tpch.tiny.orders", 15000);

        assertThat(query("SELECT count(*) FROM test_gpu_multiple_rows WHERE is_divisible"))
                .executesWithoutGpu();
        assertThat(query("SELECT count(*) FROM test_gpu_multiple_rows WHERE orderkey < 100"))
                .executesWithoutGpu();
        assertThat(query("SELECT sum(totalprice) FROM test_gpu_multiple_rows"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_multiple_rows");
    }

    @Test
    public void testSelectWithColumnPruning()
    {
        assertUpdate("CREATE TABLE test_gpu_pruning AS " +
                        "SELECT orderkey, orderstatus, totalprice, orderdate FROM tpch.tiny.orders",
                15000);

        // Read only some columns (tests column pruning)
        assertThat(query("SELECT orderkey FROM test_gpu_pruning WHERE orderkey < 10"))
                .executesWithoutGpu();
        assertThat(query("SELECT totalprice FROM test_gpu_pruning WHERE orderkey < 10"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_pruning");
    }

    @Test
    public void testSelectWithPartitioning()
    {
        assertUpdate("CREATE TABLE test_gpu_partitioned " +
                        "WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                        "SELECT orderkey, totalprice, orderstatus FROM tpch.tiny.orders",
                15000);

        // Test partition pruning (partition filters are pushed down to splits, so GPU can execute)
        assertThat(query("SELECT count(*) FROM test_gpu_partitioned WHERE orderstatus = 'F'"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT sum(totalprice) FROM test_gpu_partitioned WHERE orderstatus = 'O'"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_partitioned");
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
}
