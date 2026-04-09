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

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
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
                .addExtraProperty("gpu-acceleration.enabled", "true")
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
                .executesWithGpu(FilterNode.class);

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
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a <= b
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a > b
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a >= b
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a = b
                """))
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a - b > 50
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a * b > 100
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a / b > 10
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a, b
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a, IF(rand()<42, i % 10 + 1) AS b FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a % b > 5
                """))
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, NULLIF(i % 10, 0)) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IS NOT NULL
                """))
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);
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
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT a
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, i) AS a FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE a IN (10, NULL, 50)
                """))
                .executesWithGpu(FilterNode.class);

        assertThat(query(
                """
                SELECT s
                -- Use rand() to prevent Projection from being inlined in Filter
                FROM (SELECT IF(rand()<42, CAST(i AS varchar)) AS s FROM (UNNEST(sequence(0, 100))) t(i))
                WHERE s IN ('10', '20', '30')
                """))
                .executesWithGpu(FilterNode.class);

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
}
