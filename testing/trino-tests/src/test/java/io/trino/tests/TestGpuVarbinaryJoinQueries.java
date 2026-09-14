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
import io.trino.sql.planner.plan.JoinNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Query-level coverage for a VARBINARY-key join, with and without a non-equi filter.
 */
public class TestGpuVarbinaryJoinQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .configureGpuDistributedExecution()
                .build();
    }

    @Test
    public void testGpuVarbinaryJoin()
    {
        // Plain equi-join on a VARBINARY key runs on GPU.
        assertThat(query(
                """
                SELECT p.v
                FROM (SELECT to_utf8(CAST(i AS varchar)) AS v, i AS n FROM (UNNEST(sequence(0, 20))) t(i)) p
                JOIN (SELECT to_utf8(CAST(i AS varchar)) AS v, i AS n FROM (UNNEST(sequence(0, 20))) t(i)) b
                    ON p.v = b.v
                """))
                .executesWithGpu(JoinNode.class)
                .matches("SELECT to_utf8(CAST(i AS varchar)) FROM (UNNEST(sequence(0, 20))) t(i)");

        // TODO ENG-27258 temporary guard: the non-equi p.n + b.n > 5 makes it a mixed join, which the guard
        // must keep on CPU; without the guard it runs on GPU and faults.
        assertThat(query(
                """
                SELECT p.v
                FROM (SELECT to_utf8(CAST(i AS varchar)) AS v, i AS n FROM (UNNEST(sequence(0, 20))) t(i)) p
                JOIN (SELECT to_utf8(CAST(i AS varchar)) AS v, i AS n FROM (UNNEST(sequence(0, 20))) t(i)) b
                    ON p.v = b.v AND p.n + b.n > 5
                """))
                .executesWithoutGpu()
                .matches("SELECT to_utf8(CAST(i AS varchar)) FROM (UNNEST(sequence(3, 20))) t(i)");
    }
}
