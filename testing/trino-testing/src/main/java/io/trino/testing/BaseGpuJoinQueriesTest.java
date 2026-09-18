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

import com.google.common.collect.MoreCollectors;
import io.trino.Session;
import io.trino.spi.QueryId;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseGpuJoinQueriesTest
        extends AbstractTestJoinQueries
{
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

    @Override
    protected void assertLookupOuterOperatorParallelization(Session session, @Language("SQL") String actual, String expected, int expectedDriversAtTaskLevel)
    {
        // Ensure results are correct
        assertQuery(session, actual, expected);

        QueryRunner.MaterializedResultWithPlan resultWithQueryId = getDistributedQueryRunner().executeWithPlan(session, actual);
        Plan plan = getDistributedQueryRunner().getQueryPlan(resultWithQueryId.queryId());
        JoinNode joinNode = (JoinNode) PlanNodeSearcher.searchFrom(plan.getRoot()).where(node -> node instanceof JoinNode).findFirst().get();

        if (joinNode.getType() == RIGHT) {
            assertThat(outerDrainDriverCountAtTaskLevel(resultWithQueryId.queryId(), joinNode.getId()))
                    .isEqualTo(1);
        }
        else {
            // FULL join still runs on the CPU LookupOuterOperator
            assertThat(extractOperatorStatsAtTaskLevel(resultWithQueryId.queryId(), joinNode.getId(), "LookupOuterOperator").getTotalDrivers())
                    .isEqualTo(expectedDriversAtTaskLevel);
        }
    }

    private int outerDrainDriverCountAtTaskLevel(QueryId queryId, PlanNodeId joinNodeId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getStages()
                .orElseThrow()
                .getStages().stream()
                .flatMap(stage -> stage.tasks().stream().findFirst().stream())
                .flatMap(task -> task.stats().pipelines().stream())
                .filter(pipeline -> !pipeline.isInputPipeline())
                // The drain writes into the join's exchange, unlike the pipeline reading from it
                .filter(pipeline -> pipeline.getOperatorSummaries().stream()
                        .anyMatch(summary -> joinNodeId.equals(summary.getPlanNodeId()) && summary.getOperatorType().equals("SentinelSinkOperator")))
                .filter(pipeline -> pipeline.getOperatorSummaries().stream().limit(1)
                        .anyMatch(summary -> joinNodeId.equals(summary.getPlanNodeId()) && summary.getOperatorType().equals("GpuOperator")))
                .collect(MoreCollectors.onlyElement())
                .getTotalDrivers();
    }

    @Test
    public void testGpuRightJoinWithLimit()
    {
        Session session = Session.builder(noJoinReordering())
                .setSystemProperty("task_concurrency", "4")
                .build();
        String join = "SELECT o.orderkey, c.custkey FROM orders o RIGHT JOIN customer c ON o.custkey = c.custkey";

        // The limit closes probe drivers mid-input, so the outer side is dropped; the rows already produced
        // satisfy the limit, so the result is still correct
        assertThat(computeActual(session, join + " LIMIT 5").getRowCount()).isEqualTo(5);

        // Only never-matched build rows pass the filter, so nothing closes the probes early
        String outerOnly = join + " WHERE o.orderkey IS NULL";
        assertThat(query(session, outerOnly)).executesWithGpu(JoinNode.class);
        assertThat(computeActual(session, outerOnly + " LIMIT 5").getRowCount()).isEqualTo(5);
    }

    @Test
    public void testGpuChainedRightJoins()
    {
        Session session = noJoinReordering();
        assertThat(query(session,
                """
                SELECT t.orderkey, t.custkey AS probe_custkey, c2.custkey AS build_custkey
                FROM (
                        SELECT o.orderkey, c.custkey
                        FROM orders o RIGHT JOIN customer c ON o.custkey = c.custkey
                        WHERE c.custkey > 200) t
                RIGHT JOIN (SELECT custkey FROM customer WHERE custkey < 500) c2 ON t.custkey = c2.custkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuRightJoinFusedWithLeftJoinOnEmptyBuild()
    {
        // The LEFT join above pads its empty build side with NULL columns sized from buildOutputTypes,
        // so mixing up the probe and build type lists is visible on the drained rows too
        Session session = noJoinReordering();
        assertThat(query(session,
                """
                SELECT t.orderkey, t.custkey, c2.name
                FROM (
                        SELECT o.orderkey, c.custkey
                        FROM orders o RIGHT JOIN customer c ON o.custkey = c.custkey) t
                LEFT JOIN (SELECT custkey, name FROM customer WHERE custkey < 0) c2 ON t.custkey = c2.custkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuRightJoinWithOperationsAbove()
    {
        // The filter cannot be pushed through the join, so the drained rows must go through it and the projection
        Session session = noJoinReordering();
        assertThat(query(session,
                """
                SELECT o.orderkey, c.custkey * 2
                FROM orders o RIGHT JOIN customer c ON o.custkey = c.custkey
                WHERE o.orderkey IS NULL OR c.custkey % 3 = 0
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuRightJoinOverLocalExchange()
    {
        Session session = Session.builder(noJoinReordering())
                .setSystemProperty("task_concurrency", "4")
                .build();
        // Probe fed by a round-robin local exchange
        assertThat(query(session,
                """
                SELECT t.custkey, c.custkey
                FROM (SELECT custkey FROM orders WHERE orderkey < 1000 UNION ALL SELECT custkey FROM orders WHERE orderkey > 59000) t
                RIGHT JOIN customer c ON t.custkey = c.custkey
                """))
                .executesWithGpu(JoinNode.class);
        // Probe fed by a gathering local exchange, so the probe runs one driver and the operators above the join run several
        assertThat(query(session,
                """
                SELECT t.custkey, c.custkey
                FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 100) t
                RIGHT JOIN customer c ON t.custkey = c.custkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuRightJoinWithFilter()
    {
        // Customers whose orders all fail the filter are drained alongside those with no orders
        assertThat(query(noJoinReordering(),
                """
                SELECT o.orderkey, c.custkey
                FROM orders o
                RIGHT JOIN customer c ON o.custkey = c.custkey AND o.orderkey < c.custkey
                """))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuCrossJoin()
    {
        assertThat(query("SELECT n.name, r.name FROM nation n CROSS JOIN region r"))
                .executesWithGpu(JoinNode.class);

        // Neither side contributes output columns
        assertThat(query("SELECT count(*) FROM nation CROSS JOIN region"))
                .executesWithGpu(JoinNode.class);

        // Only the build side contributes output columns
        assertThat(query("SELECT r.name FROM nation n CROSS JOIN region r"))
                .executesWithGpu(JoinNode.class);

        // Only the probe side contributes output columns
        assertThat(query("SELECT n.name FROM nation n CROSS JOIN region r"))
                .executesWithGpu(JoinNode.class);

        // Empty build side
        assertThat(query("SELECT n.name, r.name FROM nation n CROSS JOIN (SELECT * FROM region WHERE regionkey < 0) r"))
                .executesWithGpu(JoinNode.class);
    }

    @Test
    public void testGpuCrossJoinWithScalarSubquery()
    {
        assertThat(query("SELECT n.name, (SELECT count(*) FROM region) FROM nation n"))
                .executesWithGpu(JoinNode.class);
    }
}
