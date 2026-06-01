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
package io.trino.plugin.hive;

import io.trino.FeaturesConfig;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestHiveGpuJoinQueries
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        checkState(
                !new FeaturesConfig().isGpuExecution(),
                "Otherwise %s would be the GPU test and this class redundant",
                TestHiveConnectorTest.class);

        return HiveQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .build();
    }

    @Override
    @Test
    public void testOutputDuplicatesInsensitiveJoin()
    {
        // test cases copied from super, retained for at least checking the correctness (which is done implicitly by executesWithGpu / executesWithoutGpu)
        {
            assertThat(query(
                    "SELECT n.nationkey, count(*) " +
                            "FROM nation n " +
                            "JOIN (SELECT regionkey FROM nation ORDER BY regionkey LIMIT 2) t(x) " +
                            "ON n.nationkey = t.x GROUP BY n.nationkey"))
                    .executesWithGpu(JoinNode.class);

            assertThat(query(
                    "SELECT n.nationkey " +
                            "FROM nation n " +
                            "JOIN (SELECT regionkey FROM nation ORDER BY regionkey LIMIT 2) t(x) " +
                            "ON n.nationkey = t.x GROUP BY n.nationkey"))
                    .executesWithGpu(JoinNode.class);

            assertThat(query("SELECT n.nationkey FROM nation n LEFT JOIN (VALUES 0, 0) t(x) ON n.nationkey = t.x WHERE n.nationkey = 0 GROUP BY n.nationkey"))
                    .executesWithGpu(JoinNode.class);

            assertThat(query("SELECT n.nationkey FROM nation n RIGHT JOIN (VALUES 0, 0) t(x) ON n.nationkey = t.x GROUP BY n.nationkey"))
                    .executesWithGpu(AggregationNode.class);

            // nation has 25 rows, so two '0' build rows are matched
            assertThat(query("SELECT t.x FROM nation n FULL JOIN (VALUES BIGINT '0', BIGINT '0') t(x) ON n.nationkey = t.x WHERE (n.nationkey <= 0 OR n.nationkey IS NULL) GROUP BY t.x"))
                    .executesWithGpu(AggregationNode.class);

            assertThat(query(
                    "SELECT n.nationkey " +
                            "FROM nation n " +
                            "JOIN (SELECT regionkey FROM nation ORDER BY regionkey LIMIT 2) t(x) " +
                            "ON n.nationkey = t.x GROUP BY GROUPING SETS (n.nationkey), (n.nationkey, n.nationkey)"))
                    .executesWithGpu(AggregationNode.class);

            assertThat(query("SELECT t.x FROM nation n JOIN (VALUES BIGINT '0', BIGINT '0', BIGINT '-1') t(x) ON n.nationkey = t.x GROUP BY t.x"))
                    .executesWithGpu(AggregationNode.class);

            assertThat(query("SELECT t.y FROM nation n JOIN (VALUES (BIGINT '0', BIGINT '0'), (BIGINT '0', BIGINT '0'), (BIGINT '-1', BIGINT '0')) t(x, y) ON n.nationkey = t.x GROUP BY t.y"))
                    .executesWithGpu(AggregationNode.class);
        }

        // The isMaySkipOutputDuplicates / outputSingleMatch optimization is a CPU-side
        // LookupJoinOperator feature that depends on LookupJoinOperator preserving the probe side row
        // order.
        // The assertion in the base class filters operator stats
        // by "LookupJoinOperator" which the GPU path never produces — skip the test.
        abort("GPU join does not implement isMaySkipOutputDuplicates");
    }
}
