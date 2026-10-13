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
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.AbstractTestWindowQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowQueries
        extends AbstractTestWindowQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .configureGpuDistributedExecution()
                .build();
    }

    @Test
    void testNoPartitionBy()
    {
        // Without PARTITION BY, the rows are gathered into a single task before the window
        assertThat(query(
                """
                SELECT row_number() OVER (ORDER BY orderkey)
                FROM orders
                """))
                .executesWithGpu(WindowNode.class);
    }

    @Test
    void testAllFunctions()
    {
        // Window functions are tested on a single node in TestGpuWindowRangeFrames and TestGpuWindowRowsFrames. Here
        // they run on workers, with the coordinator excluded from scheduling.
        assertThat(query(
                """
                SELECT
                    avg(price) OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    rank() OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    dense_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey),
                    row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey)
                FROM (SELECT orderstatus, orderkey, CAST(totalprice AS decimal(12, 2)) AS price FROM orders)
                """))
                .executesWithGpu(WindowNode.class);
    }
}
