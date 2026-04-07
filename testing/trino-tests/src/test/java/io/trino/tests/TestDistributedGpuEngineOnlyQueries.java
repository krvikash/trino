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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Override
    @Test
    public void testCorrelatedExistsSubqueries()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10115
        assertThatThrownBy(super::testCorrelatedExistsSubqueries)
                .hasStackTraceContaining("Unsupported type: date");
    }

    @Override
    @Test
    public void testScalarSubquery()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10115
        assertThatThrownBy(super::testScalarSubquery)
                .hasStackTraceContaining("Unsupported type: decimal(2,1)");
    }

    @Override
    @Test
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10115
        assertThatThrownBy(super::testCorrelatedScalarSubqueriesWithScalarAggregation)
                .hasStackTraceContaining("Unsupported type: date");
    }

    @Override
    @Test
    public void testTopNRank()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10115
        assertThatThrownBy(super::testTopNRank)
                .hasStackTraceContaining("Unsupported type: date");
    }

    @Override
    @Test
    public void testIntersect()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10115
        assertThatThrownBy(super::testIntersect)
                .hasStackTraceContaining("Unsupported type: decimal(11,1)");
    }

    @Override
    @Test
    public void testExcept()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10115
        assertThatThrownBy(super::testExcept)
                .hasStackTraceContaining("Unsupported type: decimal(11,1)");
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
}
