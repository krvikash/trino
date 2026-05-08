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
import io.trino.Session;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveSingleNodeJoins
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                // single node, the coordinator is the only worker
                .setWorkerCount(0)
                .addExtraProperty("node-scheduler.include-coordinator", "true")
                .addExtraProperty("experimental.force-single-node-query", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void verifyDynamicFilteringEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering')");
    }

    @Test
    public void testJoinWithEmptyBuildSide()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                session,
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice = 123.4567");
        assertThat(result.result().getRowCount()).isEqualTo(0);
    }

    @Test
    public void testBucketedJoins()
    {
        try (TestTable leftBucketedTable = new TestTable(getQueryRunner()::execute, "leftBucketedTable", "(key bigint, value varchar, part_col bigint) WITH (partitioned_by=ARRAY['part_col'], bucketed_by = ARRAY['key'], bucket_count = 20)", ImmutableList.of("1, 'a', 1", "2, 'b', 1"));
                TestTable right = new TestTable(getQueryRunner()::execute, "rightNonBucketedTable", "(key bigint, value varchar, part_col bigint) WITH (partitioned_by=ARRAY['part_col'])", ImmutableList.of("2, 'c', 1", "3, 'd', 1"));
                TestTable rightBucketedTable = new TestTable(getQueryRunner()::execute, "rightBucketedTable", "(key bigint, value varchar, part_col bigint) WITH (partitioned_by=ARRAY['part_col'], bucketed_by = ARRAY['key'], bucket_count = 20)", ImmutableList.of("2, 'c', 1", "3, 'd', 1"))) {
            assertLookupOuterOperatorParallelization(
                    2,
                    "SELECT l.value, r.value FROM %s l FULL OUTER JOIN %s r ON l.key = r.key AND l.part_col = r.part_col".formatted(leftBucketedTable.getName(), right.getName()),
                    "VALUES ('a', null), ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    2,
                    "SELECT l.value, r.value FROM %s l FULL OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), rightBucketedTable.getName()),
                    "VALUES ('a', null), ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    2,
                    "SELECT l.value, r.value FROM %s l RIGHT OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), right.getName()),
                    "VALUES ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    2,
                    "SELECT l.value, r.value FROM %s l RIGHT OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), rightBucketedTable.getName()),
                    "VALUES ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    4,
                    "SELECT l.value, r.value FROM %s l FULL OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), right.getName()),
                    "VALUES ('a', null), ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    4,
                    "SELECT l.value, r.value FROM %s l FULL OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), rightBucketedTable.getName()),
                    "VALUES ('a', null), ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    4,
                    "SELECT l.value, r.value FROM %s l RIGHT OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), right.getName()),
                    "VALUES ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    4,
                    "SELECT l.value, r.value FROM %s l RIGHT OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), rightBucketedTable.getName()),
                    "VALUES ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    8,
                    "SELECT l.value, r.value FROM %s l FULL OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), right.getName()),
                    "VALUES ('a', null), ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    8,
                    "SELECT l.value, r.value FROM %s l FULL OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), rightBucketedTable.getName()),
                    "VALUES ('a', null), ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    8,
                    "SELECT l.value, r.value FROM %s l RIGHT OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), right.getName()),
                    "VALUES ('b', 'c'), (null, 'd')");

            assertLookupOuterOperatorParallelization(
                    8,
                    "SELECT l.value, r.value FROM %s l RIGHT OUTER JOIN %s r ON l.key = r.key".formatted(leftBucketedTable.getName(), rightBucketedTable.getName()),
                    "VALUES ('b', 'c'), (null, 'd')");
        }
    }
}
