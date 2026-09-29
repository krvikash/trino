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
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;

/// Tests `experimental.query-distribution-mode=SINGLE_NODE`, where the whole query body runs on one node without any remote
/// exchange. Plan portions that must run on the coordinator still get a stage of their own, so that they do not
/// drag the query body onto the coordinator with them.
///
/// Single node execution is only chosen when every table in the analysis supports it
/// ([io.trino.spi.connector.ConnectorMetadata#supportsSingleNodeExecution]), so the catalogs here are picked for
/// that: reads come from `tpch`, and the `memory` connector, which does not support it, is only ever a `CREATE
/// TABLE AS` target, where the table does not exist yet and so is not part of the analysis. `INSERT` does register
/// its target table, so it writes to `blackhole` instead.
final class TestForceQuerySingleNodeExecution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                // Two workers, so a query body spread over the workers is distinguishable from one running on a single node.
                .setWorkerCount(2)
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .addExtraProperty("experimental.query-distribution-mode", "SINGLE_NODE")
                .build();
        try {
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testExplainAnalyze()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT regionkey, count(*) FROM tpch.tiny.nation GROUP BY regionkey",
                "Trino version: .*",
                "Fragment 1 \\[SINGLE]",
                "TableScan\\[table = tpch:tiny:nation]");
    }

    @Test
    void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT count(*) FROM tpch.tiny.nation",
                "Trino version: .*",
                "TableScan\\[table = tpch:tiny:nation]");
    }

    /// Single node execution must not become coordinator execution: only the `ExplainAnalyze` node itself belongs
    /// on the coordinator, while the analyzed plan runs on a single worker like it would without `EXPLAIN ANALYZE`.
    @Test
    void testExplainAnalyzeRunsQueryBodyOnSingleWorker()
    {
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                "EXPLAIN ANALYZE SELECT regionkey, count(*) FROM tpch.tiny.nation GROUP BY regionkey");
        assertQueryBodyRunsOnSingleWorker(result, 1);

        // Input row count proves the reported statistics are the final ones for the analyzed stage,
        // not a snapshot of a stage that is still running.
        assertThat((String) result.result().getOnlyValue())
                .contains("Input: 25 rows");
    }

    /// `TableFinish` is coordinator only, so it needs a stage of its own for the same reason `ExplainAnalyze` does.
    /// Without one the single fragment carrying the whole query is upgraded from `SINGLE` to `COORDINATOR_ONLY`,
    /// so the query body runs on the coordinator instead of on a worker.
    @Test
    void testCreateTableAsSelect()
    {
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                "CREATE TABLE test_ctas_single_node AS SELECT regionkey, name FROM tpch.tiny.nation");
        assertThat(result.result().getOnlyValue()).isEqualTo(25L);
        assertQueryBodyRunsOnSingleWorker(result, 1);

        assertThat(query("SELECT count(*) FROM test_ctas_single_node")).matches("VALUES BIGINT '25'");
        assertUpdate("DROP TABLE test_ctas_single_node");
    }

    /// Writes to `blackhole` rather than `memory`: `INSERT` registers its target table in the analysis, and a
    /// target that does not support single node execution turns single node execution off for the whole query.
    @Test
    void testInsert()
    {
        assertUpdate("CREATE TABLE blackhole.default.test_insert_single_node (regionkey bigint, name varchar(25))");
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                "INSERT INTO blackhole.default.test_insert_single_node SELECT regionkey, name FROM tpch.tiny.nation");
        assertThat(result.result().getOnlyValue()).isEqualTo(25L);
        assertQueryBodyRunsOnSingleWorker(result, 1);

        assertUpdate("DROP TABLE blackhole.default.test_insert_single_node");
    }

    /// `EXPLAIN ANALYZE` of a `CREATE TABLE ... AS SELECT` combines both rewrites in one plan: `ExplainAnalyze` and
    /// the `TableFinish` below it are both coordinator only, so each gets a stage of its own, with the query body in
    /// a single node stage below both.
    @Test
    void testExplainAnalyzeCreateTableAsSelect()
    {
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                "EXPLAIN ANALYZE CREATE TABLE test_explain_analyze_ctas_single_node AS SELECT regionkey, name FROM tpch.tiny.nation");
        assertQueryBodyRunsOnSingleWorker(result, 2);

        assertThat(query("SELECT count(*) FROM test_explain_analyze_ctas_single_node")).matches("VALUES BIGINT '25'");
        assertUpdate("DROP TABLE test_explain_analyze_ctas_single_node");
    }

    /// Asserts the shape single node execution must produce: `coordinatorStageCount` stages, one per coordinator only
    /// node, and the whole query body in one single node stage below them, running as one task off the coordinator.
    /// One task and no further sub stages is what makes this single node execution rather than distributed execution.
    private void assertQueryBodyRunsOnSingleWorker(MaterializedResultWithPlan result, int coordinatorStageCount)
    {
        StagesInfo stages = queryInfo(result).getStages().orElseThrow();

        StageInfo stage = stages.getOutputStage();
        for (int i = 0; i < coordinatorStageCount; i++) {
            assertThat(stage.plan().getPartitioning())
                    .as("partitioning of coordinator stage %s", i)
                    .isEqualTo(COORDINATOR_DISTRIBUTION);
            stage = getOnlyElement(stages.getSubStages(stage.stageId()));
        }

        StageInfo queryBodyStage = stage;
        assertThat(queryBodyStage.plan().getPartitioning()).isEqualTo(SINGLE_DISTRIBUTION);
        assertThat(stages.getSubStages(queryBodyStage.stageId()))
                .as("the query body must not be split into further stages")
                .isEmpty();

        String coordinatorNodeId = getQueryRunner().getCoordinator().getCurrentNode().getNodeIdentifier();
        assertThat(queryBodyStage.tasks())
                .singleElement()
                .satisfies(task -> assertThat(task.taskStatus().nodeId())
                        .as("query body must not be executed on the coordinator")
                        .isNotEqualTo(coordinatorNodeId));
    }

    private QueryInfo queryInfo(MaterializedResultWithPlan result)
    {
        return getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
    }
}
