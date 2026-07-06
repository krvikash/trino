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

import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.sql.analyzer.QueryExplainer.DEPRECATED_TYPE_LOGICAL_WARNING;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.trino.sql.tree.ExplainType.Type.IO;
import static io.trino.sql.tree.ExplainType.Type.LOGICAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedEngineOnlyQueriesWithCteReuse
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .addExtraProperty("optimizer.reuse-common-subqueries", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .withExchange("filesystem")
                .build();
        try {
            queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Override
    @Test
    public void testDefaultExplainTextFormat()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testDefaultExplainGraphvizFormat()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getGraphvizExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getGraphvizExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testDefaultExplainJsonFormat()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT JSON) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getJsonExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders";
        result = computeActual("EXPLAIN (FORMAT JSON) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getJsonExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testLogicalExplainTextFormat()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        @Language("SQL") String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(DEPRECATED_TYPE_LOGICAL_WARNING + getExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(DEPRECATED_TYPE_LOGICAL_WARNING + getExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testLogicalExplain()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(DEPRECATED_TYPE_LOGICAL_WARNING + getExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(DEPRECATED_TYPE_LOGICAL_WARNING + getExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testExplainExecute()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM orders")
                .addPreparedStatement("my_query_cte", "SELECT * FROM orders, orders")
                .build();

        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query");
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan("SELECT * FROM orders", LOGICAL));

        result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query_cte");
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan("SELECT * FROM orders, orders", LOGICAL));
    }

    @Override
    @Test
    public void testExplainExecuteWithUsing()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM orders WHERE orderkey < ?")
                .addPreparedStatement("my_query_cte", "SELECT * FROM orders o1, orders o2 WHERE o1.orderkey < ?")
                .build();

        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query USING 7");
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan("SELECT * FROM orders WHERE orderkey < 7", LOGICAL));

        result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query_cte USING 7");
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan("SELECT * FROM orders o1, orders o2 WHERE o1.orderkey < 7", LOGICAL));
    }

    @Override
    @Test
    public void testLogicalExplainGraphvizFormat()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getGraphvizExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getGraphvizExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getGraphvizExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getGraphvizExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testLogicalExplainJsonFormat()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT JSON) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getJsonExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getJsonExplainPlan(query, DISTRIBUTED));

        // query qualifies for CTE reuse, so the new IR representation is used
        query = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT JSON) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getJsonExplainPlan(query, LOGICAL));
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getJsonExplainPlan(query, DISTRIBUTED));
    }

    @Override
    @Test
    public void testIoExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, IO));

        String queryCteReuse = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (TYPE IO) " + queryCteReuse);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(queryCteReuse, IO));
    }

    @Override
    @Test
    public void testIoExplainJsonFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, IO));

        String queryCteReuse = "SELECT * FROM orders, orders";
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + queryCteReuse);
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(queryCteReuse, IO));
    }

    @Override
    @Test
    public void testExplainValidate()
    {
        // query does not qualify for CTE reuse, so the old IR representation is used
        MaterializedResult result = computeActual("EXPLAIN (TYPE VALIDATE) SELECT * FROM orders");
        assertThat(result.getOnlyValue()).isEqualTo(true);

        // query qualifies for CTE reuse, so the new IR representation is used
        result = computeActual("EXPLAIN (TYPE VALIDATE) SELECT * FROM orders, orders");
        assertThat(result.getOnlyValue()).isEqualTo(true);
    }

    @Override
    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze(
                noJoinReordering(BROADCAST),
                "EXPLAIN ANALYZE SELECT * FROM (SELECT nationkey, regionkey FROM nation GROUP BY nationkey, regionkey) a, nation b WHERE a.regionkey = b.regionkey",
                "Trino version: .*");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Left \\(probe\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*",
                "Right \\(build\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*");
        assertExplainAnalyze(
                Session.builder(getSession())
                        .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                        .build(),
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Left \\(probe\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*",
                "Right \\(build\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*");

        // The last test case is removed: in this case, the plan is fragmented from the new IR. The estimates are not available.
    }

    @Test
    public void testCteReuseOverUnionAggregation()
    {
        // Reproduces ENG-19211: a CTE whose body aggregates over a UNION ALL of several scans
        // produces a RemoteSource referencing multiple exchange source fragments. Previously failed
        // scheduling when the CTE was reused.
        @Language("SQL") String query =
                """
                WITH anchor AS (
                    SELECT MAX(d) AS as_of
                    FROM (
                        SELECT o.orderdate AS d FROM orders o JOIN nation n ON o.custkey = n.nationkey
                        UNION ALL
                        SELECT l.shipdate AS d FROM lineitem l JOIN nation n ON l.suppkey = n.nationkey
                        UNION ALL
                        SELECT o.orderdate AS d FROM orders o JOIN region r ON o.custkey = r.regionkey
                    )
                )
                SELECT 'a' AS tag, as_of FROM anchor
                UNION ALL
                SELECT 'b' AS tag, as_of FROM anchor
                """;
        String plan = getExplainPlan(query, DISTRIBUTED);
        // Both uses of anchor read from the same three spooled union-branch fragments.
        // Verify the specific multi-exchange RemoteSource appears twice — once per CTE reference.
        String multiExchangeRemoteSource = "RemoteSource[sourceFragmentIds = [1, 4, 7]]";
        int firstOccurrence = plan.indexOf(multiExchangeRemoteSource);
        assertThat(firstOccurrence).as("multi-exchange RemoteSource not found in plan").isNotNegative();
        assertThat(plan.indexOf(multiExchangeRemoteSource, firstOccurrence + 1))
                .as("multi-exchange RemoteSource appears only once — expected twice, one per anchor CTE use")
                .isNotNegative();
        assertQuerySucceeds(query);
    }
}
