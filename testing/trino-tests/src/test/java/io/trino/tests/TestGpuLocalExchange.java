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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static io.trino.tpch.TpchTable.ORDERS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuLocalExchange
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(ImmutableList.of(ORDERS))
                .configureGpuDistributedExecution()
                .build();
    }

    private Session sessionWithIntermediateAggregations()
    {
        return Session.builder(getSession())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .build();
    }

    @Test
    public void testGatherLocalExchangeRunsOnGpu()
    {
        // AddIntermediateAggregations inserts a GATHER LE between PARTIAL and INTERMEDIATE.
        assertThat(query(
                sessionWithIntermediateAggregations(),
                "SELECT COUNT(*) FROM orders"))
                .executesWithGpu(ExchangeNode.class);
    }

    @Test
    public void testFilterAboveLocalExchangeRunsOnGpu()
    {
        assertThat(query(
                sessionWithIntermediateAggregations(),
                """
                SELECT c
                FROM (SELECT COUNT(*) AS c FROM orders)
                WHERE c > 100
                """))
                .executesWithGpu(ExchangeNode.class);
    }

    @Test
    public void testProjectionAboveLocalExchangeRunsOnGpu()
    {
        assertThat(query(
                sessionWithIntermediateAggregations(),
                """
                SELECT c + 1 AS c1
                FROM (SELECT COUNT(*) AS c FROM orders)
                """))
                .executesWithGpu(ExchangeNode.class);
    }

    @Test
    public void testFilterAndProjectionAboveLocalExchangeRunOnGpu()
    {
        assertThat(query(
                sessionWithIntermediateAggregations(),
                """
                SELECT c * 2 AS c2
                FROM (SELECT COUNT(*) AS c FROM orders)
                WHERE c > 100
                """))
                .executesWithGpu(ExchangeNode.class);
    }
}
