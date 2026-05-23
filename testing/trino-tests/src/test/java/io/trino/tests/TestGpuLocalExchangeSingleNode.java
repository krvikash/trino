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

import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuLocalExchangeSingleNode
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Reads tpch.* directly; setInitialTables would trigger a CTAS that fails under
        // experimental.force-single-node-query=true with "TableExecuteContext not registered".
        return MemoryQueryRunner.builder()
                .addExtraProperty("gpu-execution", "true")
                .addExtraProperty("task.gpu-execution.enabled", "true")
                .addWorkerProperty("gpu.memory.pool-size", "4GB")
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                // Keep everything in one fragment so local exchanges aren't replaced by remote ones.
                .addExtraProperty("experimental.force-single-node-query", "true")
                .build();
    }

    private Session singleNodePartitionedSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                // Force HASH LE on partitioned joins regardless of build size (default cutoff is 1M).
                .setSystemProperty(JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT, "0")
                .build();
    }

    @Test
    public void testHashLocalExchangeNotChosenWhenConsumerIsCpu()
    {
        // regexp_like join filter forces the join to CPU; both pipelines of its paired HASH LE
        // must stay on CPU so build and probe routing agree.
        Session session = singleNodePartitionedSession();
        String sql =
                """
                SELECT count(*)
                FROM tpch.sf1.orders o
                JOIN tpch.sf1.nation n
                    ON o.custkey = n.nationkey
                   AND regexp_like(o.orderstatus || ':' || n.name, '^(O:FRANCE|F:GERMANY)$')
                """;
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(session, sql);
        Set<PlanNodeId> hashExchanges = findHashLocalExchanges(result);
        Set<PlanNodeId> gpuPlanNodes = QueryAssertions.QueryAssert.collectGpuPlanNodes(queryStats(result));
        assertThat(hashExchanges).as("plan must include a HASH local exchange").isNotEmpty();
        assertThat(gpuPlanNodes).as("query must exercise GPU operators").isNotEmpty();
        assertThat(Sets.intersection(hashExchanges, gpuPlanNodes))
                .as("HASH local exchange must stay on CPU when paired with a CPU lookup join")
                .isEmpty();

        Session cpuOnly = Session.builder(session)
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .build();
        MaterializedResult expected = getQueryRunner().execute(cpuOnly, sql);
        assertThat(result.result().getMaterializedRows())
                .as("GPU+CPU mixed execution must give same result as CPU-only")
                .isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testHashLocalExchangeRunsOnGpuForGroupBy()
    {
        // GROUP BY's HASH LE has no sibling exchange to disagree with, so MURMUR3 routing is safe.
        Session session = singleNodePartitionedSession();
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                session,
                "SELECT custkey, count(*) FROM tpch.sf1.orders GROUP BY custkey");
        Set<PlanNodeId> hashExchanges = findHashLocalExchanges(result);
        Set<PlanNodeId> gpuPlanNodes = QueryAssertions.QueryAssert.collectGpuPlanNodes(queryStats(result));
        assertThat(hashExchanges).as("plan must include a HASH local exchange").isNotEmpty();
        assertThat(Sets.intersection(hashExchanges, gpuPlanNodes))
                .as("group-by HASH local exchange must run on GPU")
                .containsAll(hashExchanges);
    }

    @Test
    public void testRoundRobinLocalExchangeRunsOnGpu()
    {
        // AddIntermediateAggregations inserts a ROUND_ROBIN LE before INTERMEDIATE; no hash
        // means the paired-LE gate doesn't apply.
        Session session = Session.builder(singleNodePartitionedSession())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .build();
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                session,
                "SELECT count(*) FROM tpch.sf1.orders");
        Set<PlanNodeId> roundRobinExchanges = findLocalExchanges(result, FIXED_ARBITRARY_DISTRIBUTION);
        Set<PlanNodeId> gpuPlanNodes = QueryAssertions.QueryAssert.collectGpuPlanNodes(queryStats(result));
        assertThat(roundRobinExchanges).as("plan must include a ROUND_ROBIN local exchange").isNotEmpty();
        assertThat(Sets.intersection(roundRobinExchanges, gpuPlanNodes))
                .as("round-robin local exchange must run on GPU")
                .containsAll(roundRobinExchanges);
    }

    @Test
    public void testHashLocalExchangeRunsOnGpuForGpuJoin()
    {
        // GPU lookup join clears the gate on its build sub-context, so the paired build HASH LE
        // lands on GPU. The build is fed by a GROUP BY whose FINAL aggregation is a GpuOperator,
        // satisfying the LE's upstream-is-GPU producer check; a plain TableScan source would be
        // fused into a single CPU ScanFilterProject operator.
        Session session = singleNodePartitionedSession();
        String sql =
                """
                SELECT count(*)
                FROM tpch.sf1.orders o
                JOIN (SELECT custkey FROM tpch.sf1.customer GROUP BY custkey) c
                  ON o.custkey = c.custkey
                """;
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(session, sql);
        Set<PlanNodeId> hashExchanges = findHashLocalExchanges(result);
        Set<PlanNodeId> gpuPlanNodes = QueryAssertions.QueryAssert.collectGpuPlanNodes(queryStats(result));
        assertThat(hashExchanges).as("plan must include a HASH local exchange").isNotEmpty();
        assertThat(Sets.intersection(hashExchanges, gpuPlanNodes))
                .as("HASH local exchange must run on GPU when its consumer lookup join is GPU")
                .isNotEmpty();

        Session cpuOnly = Session.builder(session)
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .build();
        MaterializedResult expected = getQueryRunner().execute(cpuOnly, sql);
        assertThat(result.result().getMaterializedRows())
                .as("GPU lookup join must give same result as CPU-only")
                .isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testHashLocalExchangeUnderAggregationIsCpuWhenConsumerIsCpu()
    {
        // The build's GROUP BY supplies the join's build partitioning, so the only hash local
        // exchange sits below the aggregation, not directly below the join. The join filter
        // forces CPU; if that hash local exchange escaped to GPU MURMUR3, build partitioning
        // would disagree with DefaultPageJoiner's Trino hash and rows would be dropped.
        Session session = singleNodePartitionedSession();
        String sql =
                """
                SELECT count(*)
                FROM tpch.sf1.orders o
                JOIN (SELECT custkey FROM tpch.sf1.customer GROUP BY custkey) c
                    ON o.custkey = c.custkey
                   AND regexp_like(o.orderstatus || ':' || cast(c.custkey AS varchar), '^(O:1|F:2)$')
                """;
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(session, sql);
        Set<PlanNodeId> hashExchanges = findHashLocalExchanges(result);
        Set<PlanNodeId> gpuPlanNodes = QueryAssertions.QueryAssert.collectGpuPlanNodes(queryStats(result));
        assertThat(hashExchanges).as("plan must include a HASH local exchange").isNotEmpty();
        assertThat(Sets.intersection(hashExchanges, gpuPlanNodes))
                .as("HASH local exchange under aggregation must stay on CPU when consumer lookup join is CPU")
                .isEmpty();

        Session cpuOnly = Session.builder(session)
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .build();
        MaterializedResult expected = getQueryRunner().execute(cpuOnly, sql);
        assertThat(result.result().getMaterializedRows())
                .as("CPU lookup join with GPU-eligible upstream must give same result as CPU-only")
                .isEqualTo(expected.getMaterializedRows());
    }

    private static Set<PlanNodeId> findHashLocalExchanges(MaterializedResultWithPlan result)
    {
        return findLocalExchanges(result, FIXED_HASH_DISTRIBUTION);
    }

    private static Set<PlanNodeId> findLocalExchanges(MaterializedResultWithPlan result, PartitioningHandle partitioning)
    {
        PlanNode root = result.queryPlan().orElseThrow().getRoot();
        return PlanNodeSearcher.searchFrom(root)
                .where(node -> node instanceof ExchangeNode exchange
                        && exchange.getPartitioningScheme().getPartitioning().getHandle().equals(partitioning))
                .findAll().stream()
                .map(PlanNode::getId)
                .collect(toImmutableSet());
    }

    private QueryStats queryStats(MaterializedResultWithPlan result)
    {
        return getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getQueryStats();
    }
}
