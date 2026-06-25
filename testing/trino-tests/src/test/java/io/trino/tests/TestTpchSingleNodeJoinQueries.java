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

import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTpchSingleNodeJoinQueries
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setWorkerCount(0)
                .addExtraProperty("node-scheduler.include-coordinator", "true")
                .addExtraProperty("experimental.force-single-node-query", "true")
                .build();
    }

    @Test
    void testMultiTableJoinNoRemoteExchange()
    {
        MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                """
                SELECT count(*)
                FROM orders o
                JOIN customer c ON o.custkey = c.custkey
                JOIN nation n ON c.nationkey = n.nationkey
                """);
        assertThat(PlanNodeSearcher.searchFrom(result.queryPlan().orElseThrow().getRoot())
                .where(node -> node instanceof ExchangeNode exchange && exchange.getScope() == REMOTE)
                .findAll())
                .as("experimental.force-single-node-query must keep all source stages in one fragment")
                .isEmpty();
    }
}
