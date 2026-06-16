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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.TestingTableFunctions.PassThroughFunction;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestTableFunctionFilterPushDown
        extends BasePlanTest
{
    private static final String MOCK_CATALOG = "mock";

    @BeforeAll
    public void setup()
    {
        getPlanTester().installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new PassThroughFunction()))
                .build()));
        getPlanTester().createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());
    }

    @Test
    public void testFilterOnPassThroughColumnPushedBelowJsonTable()
    {
        // A filter on a pass-through column `orderkey` is pushed below the table function
        @Language("SQL") String sql =
                """
                SELECT x
                FROM orders, JSON_TABLE(comment, 'lax $' AS root COLUMNS(x BIGINT PATH 'lax $'))
                WHERE orderkey = 1
                """;

        assertPlan(sql,
                output(
                        tableFunctionProcessor(
                                builder -> builder.name("$json_table"),
                                project(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L)),
                                                tableScan("orders", ImmutableMap.of("comment", "comment", "orderkey", "orderkey")))))));
    }

    @Test
    public void testFilterOnPassThroughColumnPushedBelowOuterJsonTable()
    {
        // A filter on a pass-through column `orderkey` is pushed below the table function
        // even when JSON_TABLE is planned as an OUTER join to the input row.
        @Language("SQL") String sql =
                """
                SELECT x
                FROM orders
                LEFT JOIN JSON_TABLE(comment, 'lax $[100]' AS root COLUMNS(x BIGINT PATH 'lax $')) ON TRUE
                WHERE orderkey = 1
                """;

        assertPlan(sql,
                output(
                        tableFunctionProcessor(
                                builder -> builder.name("$json_table"),
                                project(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L)),
                                                tableScan("orders", ImmutableMap.of("comment", "comment", "orderkey", "orderkey")))))));
    }

    @Test
    public void testFilterOnProperOutputColumnRemainsAboveJsonTable()
    {
        // A filter on a proper output column `x` cannot be pushed below
        @Language("SQL") String sql =
                """
                SELECT orderkey, x
                FROM orders, JSON_TABLE(comment, 'lax $' AS root COLUMNS(x BIGINT PATH 'lax $'))
                WHERE x = 1
                """;

        assertPlan(sql,
                output(
                        filter(
                                comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)),
                                tableFunctionProcessor(
                                        builder -> builder
                                                .name("$json_table")
                                                .properOutputs(ImmutableList.of("x")),
                                        project(
                                                tableScan("orders"))))));
    }

    @Test
    public void testMixedConjunctRemainsAboveJsonTable()
    {
        ResolvedFunction addFunction = new TestingFunctionResolution().resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
        // A single conjunct that references both a pass-through column and a proper output column
        // cannot be pushed — the whole conjunct stays above
        @Language("SQL") String sql =
                """
                SELECT orderkey, x
                FROM orders, JSON_TABLE(comment, 'lax $' AS root COLUMNS(x BIGINT PATH 'lax $'))
                WHERE orderkey + x > 0
                """;

        assertPlan(sql,
                anyTree(
                        filter(
                                comparison(
                                        GREATER_THAN,
                                        new Call(addFunction, ImmutableList.of(new Reference(BIGINT, "orderkey"), new Reference(BIGINT, "x"))),
                                        new Constant(BIGINT, 0L)),
                                tableFunctionProcessor(
                                        builder -> builder
                                                .name("$json_table")
                                                .properOutputs(ImmutableList.of("x")),
                                        project(
                                                tableScan("orders", ImmutableMap.of("comment", "comment", "orderkey", "orderkey")))))));
    }

    @Test
    public void testPartialPushdownThroughJsonTable()
    {
        // One conjunct on a pass-through column is pushed below; one on a proper output stays above.
        @Language("SQL") String sql =
                """
                SELECT x
                FROM orders, JSON_TABLE(comment, 'lax $' AS root COLUMNS(x BIGINT PATH 'lax $'))
                WHERE orderkey = 1 AND x = 2
                """;

        assertPlan(sql,
                anyTree(
                        filter(
                                comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 2L)),
                                tableFunctionProcessor(
                                        builder -> builder
                                                .name("$json_table")
                                                .passThroughSymbols(ImmutableList.of(ImmutableList.of()))
                                                .properOutputs(ImmutableList.of("x")),
                                        project(
                                                filter(
                                                        comparison(EQUAL, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L)),
                                                        tableScan("orders", ImmutableMap.of("comment", "comment", "orderkey", "orderkey"))))))));
    }

    @Test
    public void testFilterOnPartitioningColumnPushedThroughTableFunction()
    {
        // A filter on a partitioning column is pushed below the table function
        @Language("SQL") String sql =
                """
                SELECT *
                FROM TABLE(mock.system.pass_through_function(
                    INPUT => TABLE(SELECT orderkey, totalprice FROM orders) PARTITION BY orderkey))
                WHERE orderkey = 1
                """;

        assertPlan(sql,
                output(
                        tableFunctionProcessor(
                                builder -> builder
                                        .name("pass_through_function"),
                                exchange(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L)),
                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey")))))));
    }

    @Test
    public void testFilterOnNonPartitioningPassThroughColumnNotPushedForArbitraryTableFunction()
    {
        // A filter on a non-partitioning pass-through column must not be pushed for non-JsonTable functions.
        @Language("SQL") String sql =
                """
                SELECT *
                FROM TABLE(mock.system.pass_through_function(
                    INPUT => TABLE(SELECT orderkey, totalprice FROM orders) PARTITION BY orderkey))
                WHERE totalprice > 100.0
                """;

        assertPlan(sql,
                output(
                        filter(
                                comparison(GREATER_THAN, new Reference(DOUBLE, "totalprice"), new Constant(DOUBLE, 100e0)),
                                tableFunctionProcessor(
                                        builder -> builder
                                                .name("pass_through_function"),
                                        exchange(
                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey")))))));
    }
}
