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
package io.trino.tests;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.AbstractTestWindowQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowQueries
        extends AbstractTestWindowQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .configureGpuDistributedExecution()
                .build();
    }

    @Test
    void testRowNumberOnGpu()
    {
        assertThat(query(
                """
                SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testRankOnGpu()
    {
        assertThat(query(
                """
                SELECT rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testDenseRankOnGpu()
    {
        assertThat(query(
                """
                SELECT dense_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testMultipleRankingFunctionsOnGpu()
    {
        assertThat(query(
                """
                SELECT
                    row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    rank() OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    dense_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testNoPartitionByOnGpu()
    {
        assertThat(query(
                """
                SELECT row_number() OVER (ORDER BY orderkey)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testRankNoOrderByOnGpu()
    {
        assertThat(query(
                """
                SELECT rank() OVER (PARTITION BY orderstatus)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testDenseRankNoOrderByOnGpu()
    {
        assertThat(query(
                """
                SELECT dense_rank() OVER (PARTITION BY orderstatus)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testRowNumberNoOrderByRewrittenToRowNumberNode()
    {
        // row_number() without ORDER BY is rewritten by ReplaceWindowWithRowNumber
        // to RowNumberNode, so it never reaches the GPU window compiler
        assertThat(query(
                """
                SELECT row_number() OVER ()
                FROM orders
                """))
                .executesWithoutGpu();
    }

    @Test
    void testRankWithTiesOnGpu()
    {
        assertThat(query(
                """
                SELECT
                    orderstatus,
                    rank() OVER (ORDER BY orderstatus),
                    dense_rank() OVER (ORDER BY orderstatus)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testRankOrderByFloatingPointOnGpu()
    {
        assertThat(query(
                """
                SELECT rank() OVER (ORDER BY totalprice)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAggregateWindowFunctionGpuIneligible()
    {
        assertThat(query(
                """
                SELECT sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM orders
                """))
                .executesWithoutGpu();
    }

    @Test
    void testAvgShortDecimalOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgLongDecimalOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(28, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgWideDecimalOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(29, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(38, 6)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgWideDecimalRowsFrameGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (
                    PARTITION BY orderstatus
                    ORDER BY orderkey
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(38, 6)) AS price FROM orders)
                """))
                .executesWithoutGpu();
    }

    @Test
    void testAvgDecimalExplicitDefaultFrameOnGpu()
    {
        // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is the default frame, which the GPU supports
        assertThat(query(
                """
                SELECT avg(price) OVER (
                    PARTITION BY orderstatus
                    ORDER BY orderkey
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgDecimalRowsFrameGpuIneligible()
    {
        // ROWS frames differ from the default frame in how peer rows are handled, and are not supported on the GPU
        assertThat(query(
                """
                SELECT avg(price) OVER (
                    PARTITION BY orderstatus
                    ORDER BY orderkey
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithoutGpu();
    }

    @Test
    void testAvgDecimalWithRankingFunctionsOnGpu()
    {
        assertThat(query(
                """
                SELECT
                    avg(price) OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    rank() OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgDecimalOrderByFloatingPointGpuIneligible()
    {
        // cuDF orders NaN after every real value when deriving RANGE frame boundaries, so floating-point order
        // keys are not supported on the GPU
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderstatus ORDER BY totalprice)
                FROM (SELECT orderstatus, totalprice, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithoutGpu();
    }

    @Test
    void testAvgDecimalOrderByDecimalOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderstatus ORDER BY price)
                FROM (SELECT orderstatus, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgDecimalPartitionByVarcharOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY orderpriority ORDER BY orderkey)
                FROM (SELECT orderpriority, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgDecimalPartitionByBigintOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (PARTITION BY custkey ORDER BY orderkey)
                FROM (SELECT custkey, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgDecimalNoPartitionByOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER (ORDER BY orderkey)
                FROM (SELECT orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAvgDecimalEmptyWindowOnGpu()
    {
        assertThat(query(
                """
                SELECT avg(price) OVER ()
                FROM (SELECT CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }
}
