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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.SUPPLIER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestGpuJoinQueries
        extends AbstractTestJoinQueries
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

    @Override
    @Test
    public void testOutputDuplicatesInsensitiveJoin()
    {
        // test cases copied from super, retained for at least checking the correctness (which is done implicitly by executesWithGpu / executesWithoutGpu)
        {
            assertThat(query(
                    "SELECT n.nationkey, count(*) " +
                            "FROM nation n " +
                            "JOIN (SELECT regionkey FROM nation ORDER BY regionkey LIMIT 2) t(x) " +
                            "ON n.nationkey = t.x GROUP BY n.nationkey"))
                    .executesWithGpu(JoinNode.class);

            assertThat(query(
                    "SELECT n.nationkey " +
                            "FROM nation n " +
                            "JOIN (SELECT regionkey FROM nation ORDER BY regionkey LIMIT 2) t(x) " +
                            "ON n.nationkey = t.x GROUP BY n.nationkey"))
                    .executesWithGpu(JoinNode.class);

            assertThat(query("SELECT n.nationkey FROM nation n LEFT JOIN (VALUES 0, 0) t(x) ON n.nationkey = t.x WHERE n.nationkey = 0 GROUP BY n.nationkey"))
                    .executesWithGpu(JoinNode.class);

            assertThat(query("SELECT n.nationkey FROM nation n RIGHT JOIN (VALUES 0, 0) t(x) ON n.nationkey = t.x GROUP BY n.nationkey"))
                    .executesWithGpu(AggregationNode.class);

            // nation has 25 rows, so two '0' build rows are matched
            assertThat(query("SELECT t.x FROM nation n FULL JOIN (VALUES BIGINT '0', BIGINT '0') t(x) ON n.nationkey = t.x WHERE (n.nationkey <= 0 OR n.nationkey IS NULL) GROUP BY t.x"))
                    .executesWithGpu(AggregationNode.class);

            assertThat(query(
                    "SELECT n.nationkey " +
                            "FROM nation n " +
                            "JOIN (SELECT regionkey FROM nation ORDER BY regionkey LIMIT 2) t(x) " +
                            "ON n.nationkey = t.x GROUP BY GROUPING SETS (n.nationkey), (n.nationkey, n.nationkey)"))
                    .executesWithGpu(AggregationNode.class);

            assertThat(query("SELECT t.x FROM nation n JOIN (VALUES BIGINT '0', BIGINT '0', BIGINT '-1') t(x) ON n.nationkey = t.x GROUP BY t.x"))
                    .executesWithGpu(AggregationNode.class);

            assertThat(query("SELECT t.y FROM nation n JOIN (VALUES (BIGINT '0', BIGINT '0'), (BIGINT '0', BIGINT '0'), (BIGINT '-1', BIGINT '0')) t(x, y) ON n.nationkey = t.x GROUP BY t.y"))
                    .executesWithGpu(AggregationNode.class);
        }

        // The isMaySkipOutputDuplicates / outputSingleMatch optimization is a CPU-side
        // LookupJoinOperator feature that depends on LookupJoinOperator preserving the probe side row
        // order.
        // The assertion in the base class filters operator stats
        // by "LookupJoinOperator" which the GPU path never produces — skip the test.
        abort("GPU join does not implement isMaySkipOutputDuplicates");
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
