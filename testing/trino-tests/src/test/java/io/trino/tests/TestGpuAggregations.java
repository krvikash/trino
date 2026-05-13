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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addExtraProperty("gpu-execution", "true")
                .addExtraProperty("task.gpu-execution.enabled", "true")
                .addWorkerProperty("gpu.memory.pool-size", "4GB")
                // GPU is disabled on coordinator unless include-coordinator is set. Disable include-coordinator to force coordinator into more production-like setup.
                // This is needed to expose potential problems where operators on workers and coordinator do not match.
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                // PushAggregationIntoValues uses CPU execution path. Disable it to get more exposure for GPU execution code path
                .addExtraProperty("optimizer.push-aggregation-into-values-enabled", "false")
                .build();
    }

    @Test
    public void testGpuAvgDecomposition()
    {
        assertThat(query(
                """
                SELECT avg(a)
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT avg(a)
                FROM (SELECT IF(rand()<42, CAST(i AS double)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT avg(a)
                FROM (SELECT IF(rand()<42, CAST(i AS real)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b, avg(a)
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                HAVING avg(a) > 3
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuSumDecimal()
    {
        // Short decimal (precision <= 18) — global SUM. Verifies PARTIAL → FINAL on GPU.
        assertThat(query(
                """
                SELECT sum(a)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(12, 2))) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        // Short decimal — grouped SUM. Same path as q18's FINAL on orderkey.
        assertThat(query(
                """
                SELECT b, sum(a)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(12, 2))) AS a, i % 10 AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);

        // Long decimal (precision > 18) — exercises the chunked Int128 PARTIAL plus the new FINAL.
        assertThat(query(
                """
                SELECT b, sum(a)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(26, 4))) AS a, i % 10 AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuBoolOr()
    {
        // rand() < 42 is always true but prevents constant folding;
        // AND i % 3 != 0 makes ~1/3 of rows null

        assertThat(query(
                """
                SELECT bool_or(a)
                FROM (SELECT IF(rand()<42 AND i % 3 != 0, i % 2 = 0) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b, bool_or(a)
                FROM (SELECT IF(rand()<42 AND i % 3 != 0, i % 2 = 0) AS a, i % 10 AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuBoolAnd()
    {
        // rand() < 42 is always true but prevents constant folding;
        // AND i % 3 != 0 makes ~1/3 of rows null

        assertThat(query(
                """
                SELECT bool_and(a)
                FROM (SELECT IF(rand()<42 AND i % 3 != 0, i % 2 = 0) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b, bool_and(a)
                FROM (SELECT IF(rand()<42 AND i % 3 != 0, i % 2 = 0) AS a, i % 10 AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuAvgShortDecimal()
    {
        assertThat(query(
                """
                SELECT avg(a)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(12, 2))) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b, avg(a)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(12, 2))) AS a, i % 10 AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuAnyValue()
    {
        assertThat(query(
                """
                SELECT any_value(a)
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT any_value(a)
                FROM (SELECT IF(rand()<42, CAST(i AS varchar)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b, any_value(a)
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);

        assertThat(query(
                """
                SELECT b, any_value(a)
                FROM (SELECT IF(rand()<42, CAST(i AS double)) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testAvgDecompositionRemovesRedundantCast()
    {
        // RewriteAvgAsSumOverCount always introduces CAST(input AS double) before sum/count.
        // For bigint input this cast is needed and should remain in the plan.
        assertThat(getExplainPlan(
                """
                SELECT avg(a)
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """,
                ExplainType.Type.DISTRIBUTED))
                .contains("avg_input := CAST(");

        // For double input the cast is redundant and SimplifyRedundantCast should remove it.
        assertThat(getExplainPlan(
                """
                SELECT avg(a)
                FROM (SELECT IF(rand()<42, CAST(i AS double)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                """,
                ExplainType.Type.DISTRIBUTED))
                .doesNotContain("avg_input := CAST(");
    }
}
