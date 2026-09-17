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
package io.trino.plugin.iceberg;

import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.server.DynamicFilterService;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.plugin.iceberg.IcebergSessionProperties.BUCKET_EXECUTION_ENABLED;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CTE_REUSE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DEREFERENCE_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_TYPE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIcebergForceQuerySingleNodeExecutionConnectorTest
        extends BaseIcebergParquetConnectorTest
{
    TestIcebergForceQuerySingleNodeExecutionConnectorTest()
    {
        super(2);
    }

    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return super.createQueryRunnerBuilder()
                .setWorkerCount(1)
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .addExtraProperty("experimental.force-single-node-query", "true");
    }

    // In distributed mode with SCALE_WRITERS=false and USE_PREFERRED_WRITE_PARTITIONING=false,
    // multiple writer tasks (one per worker) each write to all partitions, producing
    // expectedFiles = partitions × workers (e.g. 3 × 3 = 9). In SNEM all writing happens
    // on a single node, so only one file per partition is produced (3).
    @Override
    protected void testStatsBasedRepartitionData(boolean ctas)
    {
        String catalog = getSession().getCatalog().orElseThrow();
        try (TestTable sourceTable = new TestTable(
                sql -> assertQuerySucceeds(
                        Session.builder(getSession())
                                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, "true")
                                .build(),
                        sql),
                "temp_table_analyzed",
                "AS SELECT orderkey, custkey, orderstatus FROM tpch.\"sf0.03\".orders")) {
            Session sessionRepartitionMany = Session.builder(getSession())
                    .setSystemProperty(SCALE_WRITERS, "false")
                    .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                    .build();
            String sourceRelation = "(SELECT DISTINCT orderkey, custkey, orderstatus FROM " + sourceTable.getName() + ")";
            testRepartitionData(
                    getSession(),
                    sourceRelation,
                    ctas,
                    "'orderstatus'",
                    3);
            assertEventually(new Duration(3, MINUTES), () -> {
                testRepartitionData(
                        sessionRepartitionMany,
                        sourceRelation,
                        ctas,
                        "'orderstatus'",
                        3);
            });
        }
    }

    @Test
    @Override // Overridden because SNEM changes the number of fragments asserted by this test.
    public void testCteReuse()
    {
        String query =
                """
                WITH t as (SELECT * FROM nation WHERE nationkey > 3)
                SELECT cardinality(histogram(regionkey)) FROM t UNION ALL SELECT cardinality(histogram(nationkey)) FROM t
                """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(enableCteReuse(), explainQuery).getOnlyValue();

        if (!hasBehavior(SUPPORTS_CTE_REUSE)) {
            assertThat(planWithoutCteReuse).isEqualTo(planWithCteReuse);
            assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(3);
            return;
        }

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(1);
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(1);
        assertThat(countRegexOccurrences(planWithCteReuse, "RemoteSource")).isEqualTo(0);
        // results are ok
        assertQueryWithCteReuse(query, "VALUES 5, 21");
    }

    @Test
    @Override // Overridden because SNEM changes the number of fragments asserted by this test.
    public void testCteReuseWithDereferencePushdown()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CTE_REUSE) && hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN) && hasBehavior(SUPPORTS_ROW_TYPE));

        String tableName = "test_dup_deref_" + randomNameSuffix();
        String createTable = "CREATE TABLE " + tableName + " AS SELECT nationkey, CAST(ROW(regionkey, nationkey) AS row(regionkey bigint, x bigint)) r FROM nation";
        assertUpdate(createTable, 25);

        String query =
                """
                WITH t as (SELECT * FROM %s)
                SELECT count(r.regionkey) FROM t UNION ALL SELECT max(nationkey) FROM t
                """.formatted(tableName);
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(enableCteReuse(), explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(1);
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(1);
        assertThat(countRegexOccurrences(planWithCteReuse, "RemoteSource")).isEqualTo(0);

        // results are ok
        assertQueryWithCteReuse(query, "VALUES 25, 24");

        // dereference pushdown really happens
        // TODO: ideally we should use enableCteReuse() here but then .isFullyPushedDown() does not work as it depends on plan in old IR shape
        PlanMatchPattern expectedPlan =
                node(OutputNode.class,
                        node(ExchangeNode.class,
                                node(AggregationNode.class,
                                        node(ExchangeNode.class,
                                                node(AggregationNode.class,
                                                        tableScan(tableName)))), // no projection
                                node(AggregationNode.class,
                                        node(ExchangeNode.class,
                                                node(AggregationNode.class,
                                                        tableScan(tableName)))))); // no projection
        assertThat(query(query)).matches(expectedPlan);
    }

    @Test
    @Override
    public void ensureDistributedQueryRunner()
    {
        assertThatThrownBy(super::ensureDistributedQueryRunner)
                .hasMessageContaining("[query runner node count] \n" +
                        "Expecting actual:\n" +
                        "  2\n" +
                        "to be greater than or equal to:\n" +
                        "  3");
    }

    @Test
    @Override
    public void testLimitPushdown()
    {
        // In distributed mode, AddExchanges inserts a partial LimitNode directly above
        // TableScanNode, so the base test's pattern anyTree(LimitNode → TableScanNode) matches that partial Limit.
        // In SNEM, AddExchanges is skipped — there is no partial Limit and no RemoteExchange.
        // AddLocalExchanges inserts a LocalExchange[SINGLE] gather between the single Limit
        // and TableScan, so no LimitNode has TableScanNode as a direct child.
        assertThatThrownBy(super::testLimitPushdown)
                .hasMessageContaining("Plan does not match, expected [\n" +
                        "\n" +
                        "- anyTree\n" +
                        "    - node(LimitNode)\n" +
                        "        - node(TableScanNode)\n" +
                        "\n" +
                        "] but found [")
                .hasMessageContaining("Output[columnNames = [name]]\n" +
                        "│   Layout: [name:varchar]\n" +
                        "└─ Limit[count = 30]\n" +
                        "   │   Layout: [name:varchar]\n" +
                        "   └─ LocalExchange[partitioning = SINGLE]\n" +
                        "      │   Layout: [name:varchar]")
                .hasMessageMatching("(?s).*TableScan\\[table = iceberg:tpch\\.nation\\$data@.* LIMIT 30 prefer_small_inital_reads].*");
        assertThat(query("SELECT name FROM nation LIMIT 30"))
                .isNotFullyPushedDown(node(LimitNode.class, node(ExchangeNode.class, node(TableScanNode.class))));
    }

    @Test
    @Override // Overridden because this test asserts the number of lazy dynamic filters, which depends on the number of stages, and that in turn is affected by SNEM.
    public void testDynamicFilterWithExplicitPartitionFilter()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        try (TestTable salesTable = newTrinoTable("sales_table", "(date date, receipt_id varchar, amount decimal(10,2)) with (partitioning=array['date'])");
                TestTable dimensionTable = newTrinoTable("dimension_table", "(date date, following_holiday boolean, year int)")) {
            assertUpdate(
                    """
                    INSERT INTO %s
                    VALUES
                        (DATE '2023-01-01' , false, 2023),
                        (DATE '2023-01-02' , true, 2023),
                        (DATE '2023-01-03' , false, 2023)""".formatted(dimensionTable.getName()),
                    3);
            assertUpdate(
                    """
                    INSERT INTO %s
                    VALUES
                        (DATE '2023-01-02' , '#2023#1', DECIMAL '122.12'),
                        (DATE '2023-01-02' , '#2023#2', DECIMAL '124.12'),
                        (DATE '2023-01-02' , '#2023#3', DECIMAL '99.99'),
                        (DATE '2023-01-02' , '#2023#4', DECIMAL '95.12'),
                        (DATE '2023-01-03' , '#2023#5', DECIMAL '199.12'),
                        (DATE '2023-01-04' , '#2023#6', DECIMAL '99.55'),
                        (DATE '2023-01-05' , '#2023#7', DECIMAL '50.11'),
                        (DATE '2023-01-05' , '#2023#8', DECIMAL '60.20'),
                        (DATE '2023-01-05' , '#2023#9', DECIMAL '70.75'),
                        (DATE '2023-01-05' , '#2023#10', DECIMAL '80.12')""".formatted(salesTable.getName()),
                    10);

            String selectQuery =
                    """
                    SELECT receipt_id
                    FROM %s s
                    JOIN %s d
                        ON  s.date = d.date
                    WHERE
                        d.following_holiday = true AND
                        d.date BETWEEN DATE '2023-01-01' AND DATE '2024-01-01'""".formatted(salesTable.getName(), dimensionTable.getName());
            QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, DYNAMIC_FILTERING_WAIT_TIMEOUT, "10s")
                            .build(),
                    selectQuery);
            MaterializedResult expected = computeActual(
                    Session.builder(getSession())
                            .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                            .build(),
                    selectQuery);
            assertEqualsIgnoreOrder(result.result(), expected);

            DynamicFilterService.DynamicFiltersStats dynamicFiltersStats = getDistributedQueryRunner().getCoordinator()
                    .getQueryManager()
                    .getFullQueryInfo(result.queryId())
                    .getQueryStats()
                    .getDynamicFiltersStats();
            // The dynamic filter reduces the range specified for the partition column `date` from `date :: [[2023-01-01, 2024-01-01]]` to `date :: {[2023-01-02]}`
            assertThat(dynamicFiltersStats.getTotalDynamicFilters()).isEqualTo(1L);
            assertThat(dynamicFiltersStats.getLazyDynamicFilters()).isEqualTo(0L);
            assertThat(dynamicFiltersStats.getReplicatedDynamicFilters()).isEqualTo(0L);
            assertThat(dynamicFiltersStats.getDynamicFiltersCompleted()).isEqualTo(1L);
        }
    }

    @Test
    @Override // Overridden because SNEM changes the number of fragments asserted by this test.
    public void testBucketedSelect()
    {
        try {
            // table is partitioned on (key1), (key2), or (key1, key2)
            assertUpdate(
                    "CREATE TABLE test_bucketed_select WITH (partitioning = ARRAY['bucket(key1, 13)', 'bucket(key2, 17)']) AS SELECT orderkey key1, custkey key2, comment value1 FROM orders",
                    15000);
            Session planWithTableNodePartitioning = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", BUCKET_EXECUTION_ENABLED, "true")
                    .build();
            Session planWithoutTableNodePartitioning = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", BUCKET_EXECUTION_ENABLED, "false")
                    .build();

            // a basic scan should not use a partitioned read as it is not helpful to the query
            @Language("SQL") String query = "SELECT value1 FROM test_bucketed_select WHERE key1 < 10";
            @Language("SQL") String expectedQuery = "SELECT comment FROM orders WHERE orderkey < 10";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertNoReadPartitioning("key1", "key2"));

            // aggregation on key1, key2, or (key1, key2) should not require a remote exchange
            query = "SELECT count(value1) FROM test_bucketed_select GROUP BY key1";
            expectedQuery = "SELECT count(comment) FROM orders GROUP BY orderkey";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            query = "SELECT count(value1) FROM test_bucketed_select GROUP BY key2";
            expectedQuery = "SELECT count(comment) FROM orders GROUP BY custkey";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            query = "SELECT count(value1) FROM test_bucketed_select GROUP BY key2, key1";
            expectedQuery = "SELECT count(comment) FROM orders GROUP BY custkey, orderkey";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            // join on key1, key2, or (key1, key2) should not require a remote exchange
            query = "SELECT key1 FROM test_bucketed_select JOIN test_bucketed_select USING (key1)";
            expectedQuery = "SELECT a.orderkey FROM orders a JOIN orders USING (orderkey)";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            query = "SELECT key2 FROM test_bucketed_select JOIN test_bucketed_select USING (key2)";
            expectedQuery = "SELECT a.custkey FROM orders a JOIN orders USING (custkey)";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            query = "SELECT key2, key1 FROM test_bucketed_select JOIN test_bucketed_select USING (key2, key1)";
            expectedQuery = "SELECT a.custkey, a.orderkey FROM orders a JOIN orders USING (custkey, orderkey)";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            query = "SELECT a.key2, b.key1 FROM test_bucketed_select a JOIN test_bucketed_select b on a.key1 = b.key2";
            expectedQuery = "SELECT a.custkey, b.orderkey FROM orders a JOIN orders b on a.orderkey = b.custkey";
            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));

            // optimize should not require a remote exchange between the scan and the execute nodes
            query = "ALTER TABLE test_bucketed_select EXECUTE OPTIMIZE";
            assertUpdate(planWithTableNodePartitioning, query, assertRemoteExchangesCount(1));
            assertUpdate(planWithoutTableNodePartitioning, query, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_select");
        }
    }
}
