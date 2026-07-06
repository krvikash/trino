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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = TpchQueryRunner.builder().build();
        queryRunner.installPlugin(new TpcdsPlugin());
        queryRunner.createCatalog("tpcds", "tpcds", ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testUnionFromDifferentCatalogs()
    {
        @Language("SQL")
        String query = "SELECT count(*) FROM (SELECT nationkey FROM tpch.tiny.nation UNION ALL SELECT ss_sold_date_sk FROM tpcds.tiny.store_sales) n JOIN tpch.tiny.region r ON n.nationkey = r.regionkey";
        assertQuery(query, "VALUES(5)");
    }

    @Test
    public void testUnionAllOnConnectorPartitionedTables()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, OptimizerConfig.JoinDistributionType.BROADCAST.name()).build();

        @Language("SQL")
        String query = "SELECT count(*) FROM ((SELECT orderkey FROM orders) union all (SELECT nationkey FROM nation)) o JOIN nation n ON o.orderkey = n.nationkey";
        assertQuery(session, query, "VALUES(32)");
    }

    @Test
    public void testUnionAllMixedDistributionWithReorderedOutputLayout()
    {
        // Regression test for a ClassCastException in AddExchanges.visitUnion mixed case (a partitioned child
        // plus a single-node child), where the partitioned child is wrapped in a single-source REMOTE GATHER
        // ExchangeNode. That ExchangeNode's constructor realigns its output layout to the source order via
        // getAlignedPartitioningSchemeAndInputs (see starburstdata/cork#2332), so its getOutputSymbols() are
        // reordered. visitUnion previously used those reordered symbols as the positional layout of the local
        // union, cross-wiring the varchar output column onto a bigint channel and vice versa.
        //
        // The partitioned branch emits [mktsegment, count] (an AggregationNode outputs grouping keys first)
        // while the union output order is [count, mktsegment], so the GATHER exchange gets realigned.
        @Language("SQL")
        String query =
                """
                SELECT count(*) AS n, mktsegment AS t FROM customer GROUP BY mktsegment
                UNION ALL
                SELECT BIGINT '1', 'b'
                """;

        assertQuery(
                query,
                "SELECT count(*), mktsegment FROM customer GROUP BY mktsegment UNION ALL SELECT 1, 'b'",
                plan -> {
                    // Guard that the query still exercises the fixed code path: a local union exchange over a
                    // REMOTE GATHER whose output layout was realigned to differ from the union output order
                    boolean coversMixedCaseGather = searchFrom(plan.getRoot())
                            .where(node -> node instanceof ExchangeNode localUnion
                                    && localUnion.getScope() == LOCAL
                                    && outputTypes(localUnion).equals(ImmutableList.of(BIGINT, createVarcharType(10))))
                            .findAll().stream()
                            .anyMatch(localUnion -> localUnion.getSources().stream()
                                    .anyMatch(source -> source instanceof ExchangeNode remoteGather
                                            && remoteGather.getScope() == REMOTE
                                            && remoteGather.getType() == GATHER
                                            && outputTypes(remoteGather).equals(ImmutableList.of(createVarcharType(10), BIGINT))));

                    assertThat(coversMixedCaseGather)
                            .describedAs("plan should contain a local union over a realigned mixed-case REMOTE GATHER")
                            .isTrue();
                });
    }

    private static List<Type> outputTypes(ExchangeNode exchange)
    {
        return exchange.getOutputSymbols().stream()
                .map(Symbol::type)
                .collect(toImmutableList());
    }
}
