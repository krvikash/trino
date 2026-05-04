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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;

final class TestRewriteAvgAsSumOverCount
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction DIVIDE_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(DOUBLE, DOUBLE));

    @Test
    void testRewriteAvgBigint()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol output = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input)));
                })
                .matches(project(
                        ImmutableMap.of("out", expression(new Call(DIVIDE_DOUBLE, ImmutableList.of(
                                new Reference(DOUBLE, "sum"),
                                new Cast(new Reference(BIGINT, "count"), DOUBLE))))),
                        aggregation(
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("avg_input")),
                                        "count", aggregationFunction("count", ImmutableList.of("avg_input"))),
                                project(
                                        ImmutableMap.of("avg_input", expression(new Cast(new Reference(BIGINT, "col"), DOUBLE))),
                                        values("col")))));
    }

    @Test
    void testRewriteAvgDouble()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", DOUBLE);
                    Symbol output = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DOUBLE, "col"))),
                                    ImmutableList.of(DOUBLE))
                            .source(p.values(input)));
                })
                .matches(project(
                        ImmutableMap.of("out", expression(new Call(DIVIDE_DOUBLE, ImmutableList.of(
                                new Reference(DOUBLE, "sum"),
                                new Cast(new Reference(BIGINT, "count"), DOUBLE))))),
                        aggregation(
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("avg_input")),
                                        "count", aggregationFunction("count", ImmutableList.of("avg_input"))),
                                project(
                                        ImmutableMap.of("avg_input", expression(new Cast(new Reference(DOUBLE, "col"), DOUBLE))),
                                        values("col")))));
    }

    @Test
    void testRewriteAvgReal()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", REAL);
                    Symbol output = p.symbol("out", REAL);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(REAL, "col"))),
                                    ImmutableList.of(REAL))
                            .source(p.values(input)));
                })
                .matches(project(
                        ImmutableMap.of("out", expression(new Cast(
                                new Call(DIVIDE_DOUBLE, ImmutableList.of(
                                        new Reference(DOUBLE, "sum"),
                                        new Cast(new Reference(BIGINT, "count"), DOUBLE))),
                                REAL))),
                        aggregation(
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("avg_input")),
                                        "count", aggregationFunction("count", ImmutableList.of("avg_input"))),
                                project(
                                        ImmutableMap.of("avg_input", expression(new Cast(new Reference(REAL, "col"), DOUBLE))),
                                        values("col")))));
    }

    @Test
    void testRewriteAvgWithGroupBy()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol groupKey = p.symbol("grp", BIGINT);
                    Symbol output = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .singleGroupingSet(groupKey)
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input, groupKey)));
                })
                .matches(project(
                        ImmutableMap.of(
                                "grp", expression(new Reference(BIGINT, "grp")),
                                "out", expression(new Call(DIVIDE_DOUBLE, ImmutableList.of(
                                        new Reference(DOUBLE, "sum"),
                                        new Cast(new Reference(BIGINT, "count"), DOUBLE))))),
                        aggregation(
                                singleGroupingSet("grp"),
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("avg_input")),
                                        "count", aggregationFunction("count", ImmutableList.of("avg_input"))),
                                project(
                                        ImmutableMap.of("avg_input", expression(new Cast(new Reference(BIGINT, "col"), DOUBLE))),
                                        values("col", "grp")))));
    }

    @Test
    void testRewriteMixedAggregates()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol avgOutput = p.symbol("avg_out", DOUBLE);
                    Symbol sumOutput = p.symbol("sum_out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input)));
                })
                .matches(project(
                        aggregation(
                                ImmutableMap.of(
                                        "sum_out", aggregationFunction("sum", ImmutableList.of("col")),
                                        "sum", aggregationFunction("sum", ImmutableList.of("avg_input")),
                                        "count", aggregationFunction("count", ImmutableList.of("avg_input"))),
                                project(
                                        ImmutableMap.of("avg_input", expression(new Cast(new Reference(BIGINT, "col"), DOUBLE))),
                                        values("col")))));
    }

    @Test
    void testDoesNotFireWhenGpuDisabled()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol output = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireOnPartialAggregation()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol output = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .step(PARTIAL)
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForDecimalAvg()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", createDecimalType(10, 2));
                    Symbol output = p.symbol("out", createDecimalType(10, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(createDecimalType(10, 2), "col"))),
                                    ImmutableList.of(createDecimalType(10, 2)))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireWithoutAvg()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol output = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForDistinctAvg()
    {
        tester().assertThat(new RewriteAvgAsSumOverCount(tester().getPlannerContext()))
                .setSystemProperty(GPU_EXECUTION_ENABLED, "true")
                .on(p -> {
                    Symbol input = p.symbol("col", BIGINT);
                    Symbol output = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    output,
                                    PlanBuilder.aggregation("avg", true, ImmutableList.of(new Reference(BIGINT, "col"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }
}
