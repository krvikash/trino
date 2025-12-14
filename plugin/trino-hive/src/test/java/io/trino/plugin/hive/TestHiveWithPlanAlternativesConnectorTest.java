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

import io.trino.Session;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveWithPlanAlternativesConnectorTest
        extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BaseHiveConnectorTest.createHiveQueryRunner(HiveQueryRunner.builder()
                .withPlanAlternatives());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CTE_REUSE -> false; // not supported for MockPlanAlternativePlugin
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testBucketedSelectWithFilter()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_bucketed_select\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1, custkey FROM orders",
                    15000);
            Session planWithTableNodePartitioning = Session.builder(getSession())
                    .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "true")
                    .build();
            Session planWithoutTableNodePartitioning = Session.builder(getSession())
                    .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "false")
                    .build();

            @Language("SQL") String query = "SELECT key1, count(value1) FROM test_bucketed_select WHERE custkey > 5 GROUP BY key1";
            @Language("SQL") String expectedQuery = "SELECT orderkey, count(comment) FROM orders WHERE custkey > 5 GROUP BY orderkey";

            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_select");
        }
    }

    @Test
    public void testExplain()
    {
        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 8",
                "ChooseAlternativeNode",
                "TableScan",  // filter is subsumed by the connector
                Pattern.quote("nationkey = bigint '8'"),  // filter is not subsumed by the connector
                "Estimates: \\{rows: .* \\(.*\\), cpu: .*, memory: .*, network: .*}",
                "Trino version: .*");
    }

    @Override
    @Test
    public void testExplainAnalyzeScanFilterProjectWallTime()
    {
        // 'Filter CPU time' is not expected when connector uses an alternative in which the filter is subsumed
    }

    @Override
    @Test
    public void testMultipleWriters()
    {
        // Not applicable with plan alternatives
    }

    @Override
    @Test
    public void testMultipleWritersWithSkewedData()
    {
        // Not applicable with plan alternatives
    }

    @Override
    @Test
    public void testMultipleWritersWhenTaskScaleWritersIsEnabled()
    {
        // Not applicable with plan alternatives
    }

    @Override
    @Test
    public void testTaskWritersDoesNotScaleWithLargeMinWriterSize()
    {
        // Not applicable with plan alternatives
    }

    @Override
    @Test
    public void testTargetMaxFileSize()
    {
        // Not applicable with plan alternatives
    }

    @Override
    @Test
    public void testTargetMaxFileSizePartitioned()
    {
        // Not applicable with plan alternatives
    }

    @Override
    @Test
    public void testTimestampWithTimeZone()
    {
        // There's no clean way to access HiveMetastoreFactory and this test doesn't exercise plan alternatives anyway
    }

    @Test
    public void testFilterColumnPruning()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT count(*) FROM nation WHERE nationkey = 8 AND regionkey = 2 LIMIT 1",
                "ChooseAlternativeNode",
                "ScanFilterProject",
                "Input: 1 row \\(9B\\), Filter");  // 9B - only one of the columns is collected
    }

    @Override
    protected Consumer<Plan> assertNoReadPartitioning(String... columnNames)
    {
        return plan -> {
            List<TableScanNode> tableScanNodes = searchFrom(plan.getRoot()).where(node -> node instanceof TableScanNode)
                    .findAll().stream()
                    .map(TableScanNode.class::cast)
                    .toList();
            for (TableScanNode tableScanNode : tableScanNodes) {
                assertThat(tableScanNode.getUseConnectorNodePartitioning().orElseThrow()).isFalse();

                HiveTableHandle tableHandle = switch (tableScanNode.getTable().connectorHandle()) {
                    case HiveTableHandle hiveTableHandle -> hiveTableHandle;
                    case MockPlanAlternativeTableHandle mockPlanAlternativeTableHandle -> (HiveTableHandle) mockPlanAlternativeTableHandle.delegate();
                    default -> throw new IllegalStateException("Unexpected connector handle: " + tableScanNode.getTable().connectorHandle());
                };

                // hive table should have partitioning for the columns but should not be active
                assertThat(tableHandle.getTablePartitioning()).isPresent();
                assertThat(tableHandle.getTablePartitioning().orElseThrow().columns().stream().map(HiveColumnHandle::getName).collect(toSet())).containsExactlyInAnyOrder(columnNames);
                assertThat(tableHandle.getTablePartitioning().orElseThrow().active()).isFalse();
            }
        };
    }
}
