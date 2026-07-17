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

import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedGpuEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .configureGpuDistributedExecution()
                .build();
        try {
            queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Test
    public void testGpuLikeFilter()
    {
        assertThat(query(
                """
                SELECT s
                FROM (SELECT CAST(i AS varchar) AS s FROM (UNNEST(sequence(0, 1000, 13))) t(i))
                WHERE s LIKE '%6%7%'
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT s
                -- Use rand() to prevent Projection from being inlined in Filter, so that LIKE operates directly on a varchar input column
                FROM (SELECT IF(rand()<42, CAST(i AS varchar)) AS s FROM (UNNEST(sequence(0, 1000, 13))) t(i))
                WHERE s LIKE '%6%7%'
                """))
                .executesWithoutGpu();

        assertThat(query("SELECT name FROM nation WHERE comment LIKE '%a%a___a%'"))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuComparisonFilter()
    {
        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a < b
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a <= b
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a > b
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a >= b
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a = b
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuArithmeticFilter()
    {
        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a + b > 50
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a - b > 50
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a * b > 100
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a / b > 10
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a % b > 5
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuAndFilter()
    {
        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a > 50 AND b < 5 AND a < 99
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuOrFilter()
    {
        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a > 90 OR b < 2
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuNotFilter()
    {
        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE NOT(a > 50)
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuIsNullFilter()
    {
        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, NULLIF(i % 10, 0)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IS NULL
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, NULLIF(i % 10, 0)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IS NOT NULL
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuBetweenFilter()
    {
        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a BETWEEN 20 AND 80
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuCoalesceFilter()
    {
        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, NULLIF(i % 10, 0)) AS a, IF(rand()<42, NULLIF(i % 5, 0)) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE COALESCE(a, b) > 3
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuInFilter()
    {
        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IN (10, 20, 30, 40, 50)
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IN (10, NULL, 50)
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT s
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, CAST(i AS varchar)) AS s FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE s IN ('10', '20', '30')
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a
                FROM (SELECT IF(rand()<42, ARRAY[i, i+1]) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IN (ARRAY[10, 11], ARRAY[20, 21])
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a
                FROM (SELECT IF(rand()<42, ARRAY[i, i+1]) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IN (ARRAY[10, 11], NULL)
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT a
                FROM (SELECT IF(rand()<42, ARRAY[i, i+1]) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IN (ARRAY[10, 11], ARRAY[20, NULL])
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT m
                FROM (SELECT IF(rand()<42, MAP(ARRAY[i], ARRAY[i+1])) AS m FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE m IN (MAP(ARRAY[10], ARRAY[11]), MAP(ARRAY[20], ARRAY[21]))
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT m
                FROM (SELECT IF(rand()<42, MAP(ARRAY[i], ARRAY[i+1])) AS m FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE m IN (MAP(ARRAY[10], ARRAY[11]), NULL)
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT m
                FROM (SELECT IF(rand()<42, MAP(ARRAY[i], ARRAY[i+1])) AS m FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE m IN (MAP(ARRAY[10], ARRAY[11]), MAP(ARRAY[20], ARRAY[CAST(NULL AS bigint)]))
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT r
                FROM (SELECT IF(rand()<42, ROW(i, i+1)) AS r FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE r IN (ROW(10, 11), ROW(20, 21))
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT r
                FROM (SELECT IF(rand()<42, ROW(i, i+1)) AS r FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE r IN (ROW(10, 11), NULL)
                """))
                .executesWithoutGpu();

        assertThat(query(
                """
                SELECT r
                FROM (SELECT IF(rand()<42, ROW(i, i+1)) AS r FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE r IN (ROW(10, 11), ROW(20, NULL))
                """))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuAggregation()
    {
        assertThat(query(
                """
                SELECT count(*), count(a), count(s), sum(a), min(a), max(a)
                FROM (SELECT IF(rand()<42, NULLIF(i % 10, 0)) AS a, IF(rand()<42, CAST(i AS varchar)) AS s FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuGroupByAggregation()
    {
        assertThat(query(
                """
                SELECT b, count(*), sum(a)
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);
        assertThat(query(
                """
                SELECT b, c, count(*), min(a), max(a)
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b, IF(rand()<42, i % 5) AS c FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b, c
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuAggregationInHaving()
    {
        assertThat(query(
                """
                SELECT b, sum(a)
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                HAVING count(*) > 5
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Override
    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze(
                noJoinReordering(BROADCAST),
                "EXPLAIN ANALYZE SELECT * FROM (SELECT nationkey, regionkey FROM nation GROUP BY nationkey, regionkey) a, nation b WHERE a.regionkey = b.regionkey",
                "Trino version: .*");
        // GPU join uses "GpuOperator" for both probe and build pipelines, so the EXPLAIN output
        // shows merged "Input avg.:" without separate "Left (probe)" / "Right (build)" labels.
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Input avg\\.: .* rows, Input std\\.dev\\.: .*");
        assertExplainAnalyze(
                Session.builder(getSession())
                        .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                        .build(),
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Input avg\\.: .* rows, Input std\\.dev\\.: .*");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Estimates: \\{rows: .* \\(.*\\), cpu: .*, memory: .*, network: .*}");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "GPU: supported",
                "GPU: unsupported",
                "Non-GPU upstream pipeline");
        assertThat((String) computeActual(
                Session.builder(getSession())
                        .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                        .build(),
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey")
                .getOnlyValue())
                .doesNotContain("GPU");
    }

    @Test
    public void testGpuAggregationWithExpression()
    {
        assertThat(query(
                """
                SELECT sum(a + b), max(a * 2)
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuVarbinaryAggregation()
    {
        // VARBINARY maps to nested DType.LIST<UINT8>; cuDF can't reduce LIST for min/max for now
        String source = "(SELECT IF(rand()<42, to_utf8(CAST(i AS varchar))) AS v FROM (UNNEST(sequence(0, 100))) t(i))";

        assertThat(query("SELECT min(v) FROM " + source))
                .executesWithoutGpu();
        assertThat(query("SELECT max(v) FROM " + source))
                .executesWithoutGpu();
        assertThat(query("SELECT count(v) FROM " + source))
                .executesWithGpu(AggregationNode.class);
    }

    @Test
    public void testGpuVarbinaryFilter()
    {
        // VARBINARY maps to nested DType.LIST<UINT8>; cuDF binary_op does not support nested
        // operands, so comparison/between/in on VARBINARY columns must stay on CPU.
        String source = "(SELECT to_utf8(CAST(i AS varchar)) AS v FROM (UNNEST(sequence(0, 100))) t(i))";

        assertThat(query("SELECT v FROM " + source + " WHERE v = X'3432'"))
                .executesWithoutGpu();
        assertThat(query("SELECT v FROM " + source + " WHERE v != X'3432'"))
                .executesWithoutGpu();
        assertThat(query("SELECT v FROM " + source + " WHERE v < X'3432'"))
                .executesWithoutGpu();
        assertThat(query("SELECT v FROM " + source + " WHERE v BETWEEN X'3130' AND X'3530'"))
                .executesWithoutGpu();
        assertThat(query("SELECT v FROM " + source + " WHERE v IN (X'3432', X'3433')"))
                .executesWithoutGpu();
    }

    @Test
    public void testGpuSemiJoin()
    {
        // non-empty build side without nulls
        // probe values matching not-null, not matching not-null, null
        assertThat(query(
                """
                SELECT x, x IN (SELECT y FROM (VALUES 1, 2, 3) b(y)) AS r
                FROM (VALUES 1, 4, NULL) p(x)
                """))
                .executesWithGpu(SemiJoinNode.class);

        // non-empty build side with nulls
        // probe values matching not-null, not matching not-null, null
        assertThat(query(
                """
                SELECT x, x IN (SELECT y FROM (VALUES 1, 2, NULL) b(y)) AS r
                FROM (VALUES 1, 4, NULL) p(x)
                """))
                .executesWithGpu(SemiJoinNode.class);

        // empty build side
        assertThat(query(
                """
                SELECT x, x IN (SELECT y FROM (VALUES 1) b(y) WHERE rand() > 2) AS r
                FROM (VALUES 1, 4, NULL) p(x)
                """))
                .executesWithGpu(SemiJoinNode.class);

        // empty probe side
        assertThat(query(
                """
                SELECT x, x IN (SELECT y FROM (VALUES 1, 2) b(y)) AS r
                FROM (SELECT * FROM (VALUES 1, 2) p(x) WHERE rand() > 2) p
                """))
                .executesWithGpu(SemiJoinNode.class);
    }

    @Test
    public void testGpuSumDecimalAggregation()
    {
        // sum(short_decimal) — input precision 15 (DECIMAL64). PARTIAL on GPU casts to DECIMAL128
        // before aggregating; CPU FINAL deserializes the 16-byte VARBINARY.
        assertThat(query(
                """
                SELECT b, sum(d)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(15,2))) AS d, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);

        // sum(long_decimal) — input precision 25 (DECIMAL128). PARTIAL extracts four 32-bit
        // chunks and sums each; combineInt64SumChunks reassembles + checks overflow.
        assertThat(query(
                """
                SELECT b, sum(d)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(25,2))) AS d, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                GROUP BY b
                """))
                .executesWithGpu(AggregationNode.class);

        // Global aggregation (no GROUP BY) with long decimal — exercises the GpuGlobalAggregation
        // code path with chunked sums.
        assertThat(query(
                """
                SELECT sum(d)
                FROM (SELECT IF(rand()<42, CAST(i AS decimal(25,2))) AS d FROM (UNNEST(sequence(0, 100))) t(i))
                """))
                .executesWithGpu(AggregationNode.class);
    }
}
