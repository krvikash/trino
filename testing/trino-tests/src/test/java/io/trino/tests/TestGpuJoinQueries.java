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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.testing.BaseGpuJoinQueriesTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.SUPPLIER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuJoinQueries
        extends BaseGpuJoinQueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(Sets.union(
                        ImmutableSet.copyOf(REQUIRED_TPCH_TABLES),
                        ImmutableSet.of(SUPPLIER)))
                .configureGpuDistributedExecution()
                .build();
    }

    @Test
    public void testTinyInnerJoin()
    {
        assertThat(query(
                """
                SELECT t.x
                FROM (VALUES 1, 2, 3, 4, 5) t(x)
                JOIN (VALUES 2, 4) u(x) ON t.x = u.x
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoin()
    {
        assertThat(query("SELECT c.custkey, o.orderstatus FROM customer c JOIN orders o ON c.custkey = o.custkey"))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testLeftJoin()
    {
        assertThat(query("SELECT c.custkey, o.orderkey FROM customer c LEFT JOIN orders o ON c.custkey = o.custkey"))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoinWithNotEqualFilter()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                JOIN orders o ON c.custkey = o.custkey AND c.nationkey <> o.orderkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testLeftJoinWithNotEqualFilter()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                LEFT JOIN orders o ON c.custkey = o.custkey AND c.nationkey <> o.orderkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoinWithRangeFilter()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                JOIN orders o ON c.custkey = o.custkey AND c.nationkey < o.orderkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoinWithAndOrFilter()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                JOIN orders o ON c.custkey = o.custkey
                    AND (c.nationkey < o.orderkey OR c.nationkey > o.orderkey)
                    AND c.nationkey <> o.orderkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoinWithFilterAgainstConstant()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                JOIN orders o ON c.custkey = o.custkey AND c.nationkey < o.orderkey AND c.nationkey < BIGINT '20'
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoinWithIsNullFilter()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                JOIN orders o ON c.custkey = o.custkey AND (c.nationkey IS NULL OR c.nationkey <> o.orderkey)
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testInnerJoinWithInfallibleCastFilter()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                -- the cast is infallible so might be executed before the join
                JOIN orders o ON c.custkey = o.custkey AND CAST(c.acctbal AS double) < CAST(o.totalprice AS double)
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testLeftJoinWithFilterReferencingOnlyLeftSide()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                LEFT JOIN orders o ON c.custkey = o.custkey AND c.nationkey > BIGINT '10'
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testJoinWithIn()
    {
        assertThat(query(
                """
                SELECT n.nationkey, r.regionkey
                FROM nation n
                JOIN region r ON n.name = r.name AND n.nationkey IN (r.regionkey, 5)
                """))
                .executesWithGpu(JoinNode.class);

        assertThat(query(
                """
                SELECT n.nationkey, r.regionkey
                FROM nation n
                LEFT JOIN region r ON n.regionkey = r.regionkey AND (n.nationkey IN (2, 1, 3, 7) OR r.regionkey = 42)
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testJoinWithBetween()
    {
        assertThat(query(
                """
                SELECT c.custkey, o.orderkey
                FROM customer c
                JOIN (SELECT orderkey, custkey, totalprice - DOUBLE '1' AS lowprice, totalprice + DOUBLE '1' AS highprice FROM orders) o
                  ON c.custkey = o.custkey AND c.acctbal BETWEEN o.lowprice AND o.highprice
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testTpchQ21()
    {
        assertThat(query(
                """
                SELECT
                  s.name,
                  count(*) as numwait
                FROM
                  "supplier" s,
                  "lineitem" l1,
                  "orders" o,
                  "nation" n
                WHERE
                  s.suppkey = l1.suppkey
                  AND o.orderkey = l1.orderkey
                  AND o.orderstatus = 'F'
                  AND l1.receiptdate> l1.commitdate
                  AND EXISTS (
                    SELECT
                      *
                    FROM
                      "lineitem" l2
                    WHERE
                      l2.orderkey = l1.orderkey
                      AND l2.suppkey <> l1.suppkey
                  )
                  AND NOT EXISTS (
                    SELECT
                      *
                    FROM
                      "lineitem" l3
                    WHERE
                      l3.orderkey = l1.orderkey
                      AND l3.suppkey <> l1.suppkey
                      AND l3.receiptdate > l3.commitdate
                  )
                  AND s.nationkey = n.nationkey
                  AND n.name = 'SAUDI ARABIA'
                GROUP BY
                  s.name
                ORDER BY
                  numwait DESC,
                  s.name
                LIMIT
                  100
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuSemiJoin()
    {
        assertThat(query(
                """
                SELECT o.custkey
                FROM orders o
                WHERE o.custkey NOT IN (
                    SELECT c.custkey FROM customer c WHERE c.nationkey > 10
                )
                """))
                .executesWithGpu(SemiJoinNode.class);

        assertThat(query(
                """
                SELECT o.custkey, o.custkey IN (SELECT c.custkey FROM customer c WHERE c.nationkey > 10)
                FROM orders o
                """))
                .executesWithGpu(SemiJoinNode.class);
    }
}
