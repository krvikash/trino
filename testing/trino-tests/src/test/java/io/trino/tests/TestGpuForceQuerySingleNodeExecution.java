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

import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that a single node query analyzed by `EXPLAIN ANALYZE` really runs on a worker. GPU execution is
/// enabled on the worker only, so a query body pinned to the coordinator would silently lose GPU execution.
///
/// Reads `tpch`, because single node execution is only chosen when all read tables support it and the `memory`
/// connector does not. See [TestForceQuerySingleNodeExecution] for the non GPU coverage, including writes.
final class TestGpuForceQuerySingleNodeExecution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setWorkerCount(1)
                // Enables GPU execution on the worker only, and keeps the coordinator out of scheduling.
                .configureGpuDistributedExecution()
                .addExtraProperty("experimental.query-distribution-mode", "SINGLE_NODE")
                .build();
    }

    @Test
    void testExplainAnalyzeRunsQueryBodyOnGpuWorker()
    {
        // GROUP BY yields a GpuOperator for the aggregation, so GPU usage is observable in operator stats.
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                "EXPLAIN ANALYZE SELECT custkey, count(*) FROM tpch.tiny.orders GROUP BY custkey");
        QueryInfo queryInfo = getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
        StagesInfo stages = queryInfo.getStages().orElseThrow();

        // The ExplainAnalyze node runs alone on the coordinator; the query body gets a single node stage of its own.
        StageInfo outputStage = stages.getOutputStage();
        assertThat(outputStage.plan().getPartitioning()).isEqualTo(COORDINATOR_DISTRIBUTION);
        StageInfo queryBodyStage = getOnlyElement(stages.getSubStages(outputStage.stageId()));
        assertThat(queryBodyStage.plan().getPartitioning()).isEqualTo(SINGLE_DISTRIBUTION);

        String coordinatorNodeId = getQueryRunner().getCoordinator().getCurrentNode().getNodeIdentifier();
        assertThat(queryBodyStage.tasks())
                .singleElement()
                .satisfies(task -> assertThat(task.taskStatus().nodeId())
                        .as("query body must not be executed on the coordinator")
                        .isNotEqualTo(coordinatorNodeId));

        assertThat(QueryAssert.collectGpuPlanNodes(queryInfo.getQueryStats()))
                .as("query body must use GPU execution")
                .isNotEmpty();

        // Input row count proves the reported statistics are the final ones for the analyzed stage,
        // not a snapshot of a stage that is still running.
        assertThat((String) result.result().getOnlyValue())
                .contains("Fragment 1 [SINGLE]")
                .contains("GPU: supported")
                .contains("Input: 15000 rows");
    }
}
