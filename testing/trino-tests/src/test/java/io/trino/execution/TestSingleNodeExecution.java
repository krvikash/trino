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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.client.StageStats;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.MIN_INPUT_ROWS_PER_TASK;
import static io.trino.SystemSessionProperties.MIN_INPUT_SIZE_PER_TASK;
import static io.trino.SystemSessionProperties.QUERY_DISTRIBUTION_MODE;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSingleNodeExecution
        extends AbstractTestQueryFramework
{
    private static final int WORKER_COUNT = 2;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunner.builder()
                .setWorkerCount(WORKER_COUNT)
                .addExtraProperty("experimental.query-distribution-mode", "AUTOMATIC")
                // pin the memory limit so the tests do not depend on the JVM heap size, and the driver count
                // so the memory the distributed queries actually use does not depend on the CPU count
                .addExtraProperty("query.max-memory-per-node", "64MB")
                .addExtraProperty("task.concurrency", "2")
                .withConnectorProperties(ImmutableMap.of("tpch.splits-per-node", "4"))
                .build();
        try {
            // The mock tables have no statistics, except "rowcount" whose row count is known while the size of
            // its varchar column is not, and accept writes
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("a", BIGINT), new ColumnMetadata("v", VARCHAR)))
                    .withData(_ -> ImmutableList.of(ImmutableList.of(1L, "x"), ImmutableList.of(2L, "y")))
                    .withGetTableStatistics(table -> table.equals(new SchemaTableName("default", "rowcount"))
                            ? TableStatistics.builder().setRowCount(Estimate.of(2)).build()
                            : TableStatistics.empty())
                    .build()));
            queryRunner.createCatalog("mock", "mock");
            // a table whose size is known, served by a connector that declines single node execution
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withName("mock_unsupported")
                    .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("a", BIGINT)))
                    .withData(_ -> ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L)))
                    .withGetTableStatistics(_ -> TableStatistics.builder().setRowCount(Estimate.of(2)).build())
                    .withMetadataWrapper(metadata -> new ClassLoaderSafeConnectorMetadata(metadata, TestSingleNodeExecution.class.getClassLoader())
                    {
                        @Override
                        public boolean supportsSingleNodeExecution(ConnectorSession session, ConnectorTableHandle tableHandle)
                        {
                            return false;
                        }
                    })
                    .build()));
            queryRunner.createCatalog("unsupported", "mock_unsupported");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testQueryFittingOnOneNodeRunsOnSingleNode()
    {
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);
    }

    @Test
    public void testMultiKeyJoinRunsOnSingleNode()
    {
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey AND o.custkey = l.suppkey");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(37L);
    }

    @Test
    public void testUnionsRunOnSingleNode()
    {
        // unions are planned as exchanges only when exchanges are added, so the decision derives the
        // properties of the union nodes themselves
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM orders UNION ALL SELECT count(*) FROM customer");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getRowCount()).isEqualTo(2);

        result = getQueryRunner().execute("SELECT custkey FROM customer EXCEPT SELECT custkey FROM orders");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getRowCount()).isEqualTo(500);
    }

    @Test
    public void testValuesStayDistributed()
    {
        // only table scans are estimated
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM orders o JOIN (VALUES 'F', 'O') v(s) ON o.orderstatus = v.s");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(14637L);
    }

    @Test
    public void testScalarJoinSidesStayDistributed()
    {
        // a join with a scalar side outputs at most its other side, whatever the estimate; but the join node's own
        // output symbols are pruned to none by count(*), and its row count is not known to the cost estimator, so
        // the row-count floor turns that unknown into an unknown byte estimate that disqualifies single-node
        // execution, even though the true output is small
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM orders WHERE custkey = (SELECT min(custkey) FROM customer)");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(9L);

        result = getQueryRunner().execute("SELECT count(*) FROM orders o JOIN (SELECT min(custkey) k FROM customer) t ON o.custkey = t.k");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(9L);
    }

    @Test
    public void testQueryExceedingNodeMemoryStaysDistributed()
    {
        // The two grouping keys of sf1.lineitem are estimated at about 100MB of input, above half of the
        // per-node memory limit of the test cluster, while the aggregation actually produces about 800K
        // groups and fits when distributed
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM (SELECT partkey, suppkey FROM tpch.sf1.lineitem GROUP BY partkey, suppkey)");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(799541L);

        // the orderkey column of sf1.lineitem is estimated at about 54MB, below the memory limit and above half of it
        result = getQueryRunner().execute("SELECT count(orderkey) FROM tpch.sf1.lineitem");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(6001215L);
    }

    @Test
    public void testZeroBoundDisablesSingleNodeExecution()
    {
        MaterializedResult result = getQueryRunner().execute(withSystemProperty(getSession(), MIN_INPUT_SIZE_PER_TASK, "0B"), "SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);

        result = getQueryRunner().execute(withSystemProperty(getSession(), MIN_INPUT_ROWS_PER_TASK, "0"), "SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);
    }

    @Test
    public void testQueryExceedingInputSizeStaysDistributed()
    {
        // The orderkey column of sf1.orders is estimated at over ten megabytes of input, above the input bound
        // the test lowers and below half of the memory limit of the test cluster
        MaterializedResult result = getQueryRunner().execute("SELECT count(orderkey) FROM tpch.sf1.orders");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(1500000L);

        result = getQueryRunner().execute(withSystemProperty(getSession(), MIN_INPUT_SIZE_PER_TASK, "1MB"), "SELECT count(orderkey) FROM tpch.sf1.orders");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(1500000L);
    }

    @Test
    public void testCountStarMovesNoData()
    {
        // The scan of count(*) outputs no column, so it is estimated at zero bytes whatever its row count and
        // fits under any input size bound
        MaterializedResult result = getQueryRunner().execute(withSystemProperty(getSession(), MIN_INPUT_SIZE_PER_TASK, "1MB"), "SELECT count(*) FROM tpch.sf1.orders");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(1500000L);

        // the scan outputs as many rows as it reads, so a row bound below its row count does not catch it either
        result = getQueryRunner().execute(withSystemProperty(getSession(), MIN_INPUT_ROWS_PER_TASK, "1000000"), "SELECT count(*) FROM tpch.sf1.orders");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(1500000L);
    }

    @Test
    public void testQueryExceedingInputRowsStaysDistributed()
    {
        // A join that outputs no column is estimated at zero bytes however many rows it produces. This one builds
        // hundreds of thousands of rows from three thousand read, above the row bound the test lowers
        MaterializedResult result = getQueryRunner().execute(withSystemProperty(getSession(), MIN_INPUT_ROWS_PER_TASK, "100000"), "SELECT count(*) FROM customer c JOIN customer c2 ON c.mktsegment = c2.mktsegment");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(451994L);

        // this one builds millions of rows from thousands, above the default row bound. The plan is only explained:
        // the join is too slow to run for a test of its plan
        assertThat(fragmentCount("SELECT count(*) FROM orders o JOIN orders o2 ON o.orderstatus = o2.orderstatus")).isEqualTo(4);
    }

    @Test
    public void testUnknownAggregationSizeStaysDistributed()
    {
        // The filter is not estimatable and the default filter factor is off, so the size of the groups the
        // aggregation retains is unknown; the whole-plan check disqualifies single-node execution on that unknown
        // output alone, even though the scan input is small and known
        MaterializedResult result = getQueryRunner().execute("SELECT custkey, count(*) FROM orders WHERE regexp_like(comment, 'a') GROUP BY custkey");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getRowCount()).isEqualTo(1000);
    }

    @Test
    public void testUnknownInputStaysDistributed()
    {
        MaterializedResult result = getQueryRunner().execute("SELECT count(a) FROM mock.default.nostats");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(2L);

        result = getQueryRunner().execute("SELECT count(*) FROM mock.default.nostats");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(2L);
    }

    @Test
    public void testUnknownColumnWidthUsesDefaultSize()
    {
        // the row count bounds the fixed width column
        MaterializedResult result = getQueryRunner().execute("SELECT count(a) FROM mock.default.rowcount");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(2L);

        // the varchar column has no size statistic and is estimated at the default width, as in every other
        // cost-based decision
        result = getQueryRunner().execute("SELECT count(v) FROM mock.default.rowcount");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(2L);
    }

    @Test
    public void testOutputExceedingInputStaysDistributed()
    {
        // A join on a key of three distinct values outputs millions of rows from thousands. The plan is only
        // explained: the join is too slow to run for a test of its plan
        assertThat(fragmentCount("SELECT count(o.orderkey) FROM orders o JOIN orders o2 ON o.orderstatus = o2.orderstatus")).isGreaterThan(1);

        // a join without a column statistic on its key has no output estimate
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM mock.default.rowcount r JOIN mock.default.rowcount r2 ON r.a = r2.a");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(2L);

        // an outer join without equality criteria
        result = getQueryRunner().execute("SELECT count(*) FROM orders o LEFT JOIN nation n ON o.totalprice > n.nationkey");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(375000L);

        // grouping sets repeat their input once per set
        result = getQueryRunner().execute("SELECT count(*) FROM (SELECT orderstatus, orderpriority FROM orders GROUP BY CUBE (orderstatus, orderpriority))");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(24L);

        // pattern recognition may return every row of every match
        result = getQueryRunner().execute("SELECT count(*) FROM orders MATCH_RECOGNIZE (ORDER BY orderkey ALL ROWS PER MATCH PATTERN (A+) DEFINE A AS true)");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);
    }

    @Test
    public void testTableFunctionStaysDistributed()
    {
        // a table function produces splits without an input estimate
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM TABLE(sequence(1, 100, 1))");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(100L);
    }

    @Test
    public void testTableWithoutSingleNodeSupportStaysDistributed()
    {
        // the table is small and its size is known, so only the connector's answer keeps the query distributed
        MaterializedResult result = getQueryRunner().execute("SELECT count(a) FROM unsupported.default.t");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(2L);

        result = getQueryRunner().execute("SELECT count(*) FROM system.runtime.tasks");
        assertDistributedExecution(rootStage(result));
    }

    @Test
    public void testWriteBodyStaysDistributed()
    {
        // TableFinishNode always runs on the coordinator alone, whatever the decision; its own unknown output
        // estimate disqualifies single-node execution for the whole plan, so the query body that feeds it stays
        // distributed too
        MaterializedResult result = getQueryRunner().execute("INSERT INTO mock.default.sink SELECT orderkey, comment FROM orders");
        assertQueryBodyStaysDistributed(rootStage(result));
    }

    @Test
    public void testExplainAnalyzeBodyRunsOnSingleNode()
    {
        // ExplainAnalyzeNode always runs on the coordinator alone in a stage of its own and is left out of the
        // estimate check, so the query body it explains collapses the way the query alone does
        MaterializedResult result = getQueryRunner().execute("EXPLAIN ANALYZE SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        StageStats explainStage = rootStage(result);
        assertThat(explainStage.getNodes()).isEqualTo(1);
        assertSingleNodeExecution(getOnlyElement(explainStage.getSubStages()));
    }

    @Test
    public void testDistributedModeStaysDistributed()
    {
        MaterializedResult result = getQueryRunner().execute(withSystemProperty(getSession(), QUERY_DISTRIBUTION_MODE, "DISTRIBUTED"), "SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);
    }

    @Test
    public void testTaskRetriesStayDistributed()
    {
        // fault-tolerant execution schedules and retries stages independently, so single-node execution is skipped
        // under task retries even though the query would otherwise fit on one node
        Session session = withSystemProperty(getSession(), RETRY_POLICY, "TASK");
        MaterializedResult result = getQueryRunner().execute(session, "SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        assertDistributedExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);
    }

    @Test
    public void testSingleNodeModeIgnoresEstimate()
    {
        Session session = withSystemProperty(getSession(), QUERY_DISTRIBUTION_MODE, "SINGLE_NODE");

        // the input bound would keep this query distributed under the automatic decision
        MaterializedResult result = getQueryRunner().execute(withSystemProperty(session, MIN_INPUT_SIZE_PER_TASK, "1MB"), "SELECT count(orderkey) FROM tpch.sf1.orders");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(1500000L);

        // a table without single node support stays distributed in single node mode
        result = getQueryRunner().execute(session, "SELECT count(*) FROM system.runtime.tasks");
        assertDistributedExecution(rootStage(result));
    }

    @Test
    public void testSingleNodeModeIgnoresTaskRetries()
    {
        // task retries only skip the cost-based (AUTOMATIC) decision; a forced SINGLE_NODE mode still
        // collapses the query into a single fragment, same as it ignores the size estimate above
        Session session = withSystemProperty(getSession(), QUERY_DISTRIBUTION_MODE, "SINGLE_NODE");
        session = withSystemProperty(session, RETRY_POLICY, "TASK");
        MaterializedResult result = getQueryRunner().execute(session, "SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey");
        assertSingleNodeExecution(rootStage(result));
        assertThat(result.getOnlyValue()).isEqualTo(15000L);
    }

    private int fragmentCount(String sql)
    {
        String plan = (String) getQueryRunner().execute("EXPLAIN (TYPE DISTRIBUTED) " + sql).getOnlyValue();
        return (int) plan.lines().filter(line -> line.startsWith("Fragment ")).count();
    }

    private static Session withSystemProperty(Session session, String property, String value)
    {
        return Session.builder(session)
                .setSystemProperty(property, value)
                .build();
    }

    private static void assertSingleNodeExecution(StageStats rootStage)
    {
        assertThat(rootStage.getSubStages()).isEmpty();
        assertThat(rootStage.getNodes()).isEqualTo(1);
    }

    private static void assertDistributedExecution(StageStats rootStage)
    {
        assertThat(rootStage.getSubStages()).isNotEmpty();
    }

    /**
     * A node that must run on the coordinator gets a stage of its own, so the query body below it is what stays
     * distributed here.
     */
    private static void assertQueryBodyStaysDistributed(StageStats rootStage)
    {
        assertThat(maxNodesPerStage(rootStage)).isGreaterThan(1);
    }

    private static int maxNodesPerStage(StageStats stage)
    {
        return stage.getSubStages().stream()
                .mapToInt(TestSingleNodeExecution::maxNodesPerStage)
                .max()
                .stream()
                .reduce(stage.getNodes(), Math::max);
    }

    private static StageStats rootStage(MaterializedResult result)
    {
        return result.getStatementStats().orElseThrow().getRootStage();
    }
}
