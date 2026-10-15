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

import com.google.common.collect.ImmutableList;
import io.trino.FeaturesConfig;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.testing.BaseGpuJoinQueriesTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveGpuJoinQueries
        extends BaseGpuJoinQueriesTest
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

    @Test
    public void testGpuRightJoinWithProbeSplitsMissingOnSomeNodes()
    {
        // A single probe row leaves most nodes without a probe split; their drain must still emit the build rows
        try (TestTable probe = newTrinoTable("gpu_right_probe_", "(k bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 8)", ImmutableList.of("1"));
                TestTable build = newTrinoTable("gpu_right_build_", "(k bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 8)")) {
            assertUpdate("INSERT INTO " + build.getName() + " SELECT nationkey FROM nation", 25);

            assertThat(query(noJoinReordering(), "SELECT p.k, b.k FROM %s p RIGHT JOIN %s b ON p.k = b.k".formatted(probe.getName(), build.getName())))
                    .executesWithGpu(JoinNode.class);
        }
    }

    @Test
    public void testGpuFullJoinWithProbeSplitsMissingOnSomeNodes()
    {
        // Two probe rows leave most nodes without a probe split; their drain must still emit the build rows, alongside
        // the probe row that matches no build row
        try (TestTable probe = newTrinoTable("gpu_full_probe_", "(k bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 8)", ImmutableList.of("1", "100"));
                TestTable build = newTrinoTable("gpu_full_build_", "(k bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 8)")) {
            assertUpdate("INSERT INTO " + build.getName() + " SELECT nationkey FROM nation", 25);

            assertThat(query(noJoinReordering(), "SELECT p.k, b.k FROM %s p FULL JOIN %s b ON p.k = b.k".formatted(probe.getName(), build.getName())))
                    .executesWithGpu(JoinNode.class);
        }
    }
}
