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
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.type.Reals;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.trino.SystemSessionProperties.REWRITE_SUM_WITH_LITERAL_ENABLED;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteSumWithLiteralAsSumAndCount
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction SUBTRACT_BIGINT = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction ADD_REAL = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(REAL, REAL));
    private static final DecimalType DECIMAL_10_2 = DecimalType.createDecimalType(10, 2);
    private static final ResolvedFunction ADD_DECIMAL = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(DECIMAL_10_2, DECIMAL_10_2));

    // Input shape: sum(CAST(CAST(smallint_col AS INT) ± int_literal AS BIGINT)).
    // This mirrors what the planner emits for SUM(smallint_col ± int_literal)
    // after operator coercion.
    private static Expression smallintArithmeticInput(OperatorType op, long literal, boolean columnOnLeft)
    {
        ResolvedFunction call = op == OperatorType.ADD ? ADD_INTEGER : SUBTRACT_INTEGER;
        Expression castCol = new Cast(new Reference(SMALLINT, "col"), INTEGER);
        Expression constant = new Constant(INTEGER, literal);
        return new Cast(
                new Call(call, columnOnLeft ? ImmutableList.of(castCol, constant) : ImmutableList.of(constant, castCol)),
                BIGINT);
    }

    private static Expression literalTimesCount(long literal)
    {
        return new Call(MULTIPLY_BIGINT, ImmutableList.of(
                new Cast(new Constant(INTEGER, literal), BIGINT),
                new Reference(BIGINT, "count")));
    }

    // Match pattern for the rewrite with a primary output plus a second
    // matching aggregate (sum(col + 99)) — the second is present so the
    // rule's multi-match-per-column gate is satisfied in single-shape
    // fires tests.
    private static PlanMatchPattern rewriteOverSmallintCol(Expression outerExpression)
    {
        return project(
                ImmutableMap.of(
                        "out", expression(outerExpression),
                        "out_second", expression(new Call(ADD_BIGINT, ImmutableList.of(
                                new Reference(BIGINT, "sum"), literalTimesCount(99))))),
                aggregation(
                        ImmutableMap.of(
                                "sum", aggregationFunction("sum", ImmutableList.of("cast_col")),
                                "count", aggregationFunction("count", ImmutableList.of("col"))),
                        project(
                                ImmutableMap.of("cast_col", expression(new Cast(new Reference(SMALLINT, "col"), BIGINT))),
                                values("col"))));
    }

    private static PlanNode sumOverSmallintAggregation(PlanBuilder p, Expression boundInput)
    {
        Symbol col = p.symbol("col", SMALLINT);
        Symbol expr = p.symbol("expr", BIGINT);
        Symbol exprSecond = p.symbol("expr_second", BIGINT);
        Symbol out = p.symbol("out", BIGINT);
        Symbol outSecond = p.symbol("out_second", BIGINT);
        return p.aggregation(a -> a
                .globalGrouping()
                .addAggregation(
                        out,
                        PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                        ImmutableList.of(BIGINT))
                .addAggregation(
                        outSecond,
                        PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_second"))),
                        ImmutableList.of(BIGINT))
                .source(p.project(
                        Assignments.builder()
                                .put(expr, boundInput)
                                .put(exprSecond, smallintArithmeticInput(OperatorType.ADD, 99L, true))
                                .build(),
                        p.values(col))));
    }

    @Test
    public void testFiresForColPlusLiteral()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> sumOverSmallintAggregation(p, smallintArithmeticInput(OperatorType.ADD, 1L, true)))
                .matches(rewriteOverSmallintCol(
                        new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(1)))));
    }

    @Test
    public void testFiresForLiteralPlusCol()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> sumOverSmallintAggregation(p, smallintArithmeticInput(OperatorType.ADD, 1L, false)))
                .matches(rewriteOverSmallintCol(
                        new Call(ADD_BIGINT, ImmutableList.of(literalTimesCount(1), new Reference(BIGINT, "sum")))));
    }

    @Test
    public void testFiresForColMinusLiteral()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> sumOverSmallintAggregation(p, smallintArithmeticInput(OperatorType.SUBTRACT, 1L, true)))
                .matches(rewriteOverSmallintCol(
                        new Call(SUBTRACT_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(1)))));
    }

    @Test
    public void testFiresForLiteralMinusCol()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> sumOverSmallintAggregation(p, smallintArithmeticInput(OperatorType.SUBTRACT, 1L, false)))
                .matches(rewriteOverSmallintCol(
                        new Call(SUBTRACT_BIGINT, ImmutableList.of(literalTimesCount(1), new Reference(BIGINT, "sum")))));
    }

    @Test
    public void testFiresForNegativeLiteral()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> sumOverSmallintAggregation(p, smallintArithmeticInput(OperatorType.ADD, -5L, true)))
                .matches(rewriteOverSmallintCol(
                        new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(-5)))));
    }

    @Test
    public void testFiresWithGroupBy()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol groupKey = p.symbol("grp", BIGINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol exprSecond = p.symbol("expr_second", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    Symbol outSecond = p.symbol("out_second", BIGINT);
                    return p.aggregation(a -> a
                            .singleGroupingSet(groupKey)
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    outSecond,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_second"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(groupKey)
                                            .put(expr, smallintArithmeticInput(OperatorType.ADD, 2L, true))
                                            .put(exprSecond, smallintArithmeticInput(OperatorType.ADD, 99L, true))
                                            .build(),
                                    p.values(col, groupKey))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "out", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(2)))),
                                "out_second", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(99))))),
                        aggregation(
                                singleGroupingSet("grp"),
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("cast_col")),
                                        "count", aggregationFunction("count", ImmutableList.of("col"))),
                                project(
                                        ImmutableMap.of("cast_col", expression(new Cast(new Reference(SMALLINT, "col"), BIGINT))),
                                        values("col", "grp")))));
    }

    @Test
    public void testFiresForMixedAggregates()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol exprSecond = p.symbol("expr_second", BIGINT);
                    Symbol sumOut = p.symbol("sum_out", BIGINT);
                    Symbol sumOutSecond = p.symbol("sum_out_second", BIGINT);
                    Symbol countOut = p.symbol("cnt_out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    sumOut,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    sumOutSecond,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_second"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    countOut,
                                    PlanBuilder.aggregation("count", ImmutableList.of(new Reference(SMALLINT, "col"))),
                                    ImmutableList.of(SMALLINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(col)
                                            .put(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true))
                                            .put(exprSecond, smallintArithmeticInput(OperatorType.ADD, 99L, true))
                                            .build(),
                                    p.values(col))));
                })
                .matches(project(
                        aggregation(
                                ImmutableMap.of("rewritten_sum", aggregationFunction("sum", ImmutableList.of("cast_col"))),
                                project(
                                        ImmutableMap.of("cast_col", expression(new Cast(new Reference(SMALLINT, "col"), BIGINT))),
                                        values("col")))));
    }

    @Test
    public void testDeduplicatesAggregatesForSameColumn()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr1 = p.symbol("expr_1", BIGINT);
                    Symbol expr2 = p.symbol("expr_2", BIGINT);
                    Symbol expr3 = p.symbol("expr_3", BIGINT);
                    Symbol out1 = p.symbol("out_1", BIGINT);
                    Symbol out2 = p.symbol("out_2", BIGINT);
                    Symbol out3 = p.symbol("out_3", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out1,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_1"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    out2,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_2"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    out3,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_3"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .put(expr1, smallintArithmeticInput(OperatorType.ADD, 1L, true))
                                            .put(expr2, smallintArithmeticInput(OperatorType.ADD, 2L, true))
                                            .put(expr3, smallintArithmeticInput(OperatorType.ADD, 3L, true))
                                            .build(),
                                    p.values(col))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "out_1", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(1)))),
                                "out_2", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(2)))),
                                "out_3", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(3))))),
                        aggregation(
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("cast_col")),
                                        "count", aggregationFunction("count", ImmutableList.of("col"))),
                                project(
                                        ImmutableMap.of("cast_col", expression(new Cast(new Reference(SMALLINT, "col"), BIGINT))),
                                        values("col")))));
    }

    @Test
    public void testDoesNotFireWithoutArithmetic()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> sumOverSmallintAggregation(p, new Cast(new Reference(SMALLINT, "col"), BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithTwoColumns()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col1 = p.symbol("col1", SMALLINT);
                    Symbol col2 = p.symbol("col2", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_INTEGER, ImmutableList.of(
                                                            new Cast(new Reference(SMALLINT, "col1"), INTEGER),
                                                            new Cast(new Reference(SMALLINT, "col2"), INTEGER))),
                                                    BIGINT)),
                                    p.values(col1, col2))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMultiplyOperator()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(MULTIPLY_BIGINT, ImmutableList.of(
                                                            new Cast(new Reference(SMALLINT, "col"), BIGINT),
                                                            new Constant(BIGINT, 2L))),
                                                    BIGINT)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForCount()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinct()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFilteredAggregate()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", false, ImmutableList.of(new Reference(BIGINT, "expr")), mask),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(mask)
                                            .put(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true))
                                            .build(),
                                    p.values(col, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithMask()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT),
                                    mask)
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(mask)
                                            .put(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true))
                                            .build(),
                                    p.values(col, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithOrderBy()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol orderingSym = p.symbol("ord", BIGINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    OrderingScheme ordering = new OrderingScheme(
                            ImmutableList.of(orderingSym),
                            ImmutableMap.of(orderingSym, ASC_NULLS_FIRST));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation(
                                            "sum",
                                            ImmutableList.of(new Reference(BIGINT, "expr")),
                                            ordering),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(orderingSym)
                                            .put(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true))
                                            .build(),
                                    p.values(col, orderingSym))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithTwoLiterals()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                                                    BIGINT)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNullLiteral()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_INTEGER, ImmutableList.of(
                                                            new Cast(new Reference(SMALLINT, "col"), INTEGER),
                                                            new Constant(INTEGER, null))),
                                                    BIGINT)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForIntegerColumn()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", INTEGER);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "col"), new Constant(INTEGER, 1L))),
                                                    BIGINT)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForBigintColumn()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", BIGINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "col"), new Constant(BIGINT, 1L)))),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNarrowColumnWithNarrowLiteral()
    {
        // SUM(smallint_col + SMALLINT '1') — inner add runs in SMALLINT, which
        // can throw NUMERIC_VALUE_OUT_OF_RANGE. The rewrite would silently
        // succeed, so the rule must not fire.
        ResolvedFunction addSmallint = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(SMALLINT, SMALLINT));
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(addSmallint, ImmutableList.of(
                                                            new Reference(SMALLINT, "col"),
                                                            new Constant(SMALLINT, 1L))),
                                                    BIGINT)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresForTinyintColumnViaCoercionCasts()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", TINYINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol exprSecond = p.symbol("expr_second", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    Symbol outSecond = p.symbol("out_second", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    outSecond,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr_second"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .put(expr, new Cast(
                                                    new Call(ADD_INTEGER, ImmutableList.of(
                                                            new Cast(new Reference(TINYINT, "col"), INTEGER),
                                                            new Constant(INTEGER, 1L))),
                                                    BIGINT))
                                            .put(exprSecond, new Cast(
                                                    new Call(ADD_INTEGER, ImmutableList.of(
                                                            new Cast(new Reference(TINYINT, "col"), INTEGER),
                                                            new Constant(INTEGER, 99L))),
                                                    BIGINT))
                                            .build(),
                                    p.values(col))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "out", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(1)))),
                                "out_second", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), literalTimesCount(99))))),
                        aggregation(
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("cast_col")),
                                        "count", aggregationFunction("count", ImmutableList.of("col"))),
                                project(
                                        ImmutableMap.of("cast_col", expression(new Cast(new Reference(TINYINT, "col"), BIGINT))),
                                        values("col")))));
    }

    @Test
    public void testDoesNotFireForNarrowingIntegerCast()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", BIGINT);
                    Symbol expr = p.symbol("expr", INTEGER);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(INTEGER, "expr"))),
                                    ImmutableList.of(INTEGER))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "col"), new Constant(BIGINT, 1L))),
                                                    INTEGER)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForBigintToRealCast()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", BIGINT);
                    Symbol expr = p.symbol("expr", REAL);
                    Symbol out = p.symbol("out", REAL);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(REAL, "expr"))),
                                    ImmutableList.of(REAL))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "col"), new Constant(BIGINT, 1L))),
                                                    REAL)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForBigintToDoubleCast()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", BIGINT);
                    Symbol expr = p.symbol("expr", DOUBLE);
                    Symbol out = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DOUBLE, "expr"))),
                                    ImmutableList.of(DOUBLE))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "col"), new Constant(BIGINT, 1L))),
                                                    DOUBLE)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForBigintToDecimalCast()
    {
        DecimalType decimalType = DecimalType.createDecimalType(30, 2);
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", BIGINT);
                    Symbol expr = p.symbol("expr", decimalType);
                    Symbol out = p.symbol("out", DecimalType.createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(decimalType, "expr"))),
                                    ImmutableList.of(decimalType))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Cast(
                                                    new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "col"), new Constant(BIGINT, 1L))),
                                                    decimalType)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenSourceNotProject()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(SMALLINT, "col"))),
                                    ImmutableList.of(SMALLINT))
                            .source(p.values(col)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenArgumentIsNotProvidedByChildProject()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(SMALLINT, "col"))),
                                    ImmutableList.of(SMALLINT))
                            .source(p.project(
                                    Assignments.builder().putIdentity(col).build(),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnPartialAggregation()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", SMALLINT);
                    Symbol expr = p.symbol("expr", BIGINT);
                    Symbol out = p.symbol("out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .step(AggregationNode.Step.PARTIAL)
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "expr"))),
                                    ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.of(expr, smallintArithmeticInput(OperatorType.ADD, 1L, true)),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenSessionPropertyDisabled()
    {
        Session disabled = Session.builder(tester().getSession())
                .setSystemProperty(REWRITE_SUM_WITH_LITERAL_ENABLED, "false")
                .build();
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .withSession(disabled)
                .on(p -> sumOverSmallintAggregation(p, smallintArithmeticInput(OperatorType.ADD, 1L, true)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDoubleColumn()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", DOUBLE);
                    Symbol expr = p.symbol("expr", DOUBLE);
                    Symbol out = p.symbol("out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DOUBLE, "expr"))),
                                    ImmutableList.of(DOUBLE))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "col"), new Constant(DOUBLE, 1.5d)))),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForRealColumn()
    {
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", REAL);
                    Symbol expr = p.symbol("expr", REAL);
                    Symbol out = p.symbol("out", REAL);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(REAL, "expr"))),
                                    ImmutableList.of(REAL))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Call(ADD_REAL, ImmutableList.of(new Reference(REAL, "col"), new Constant(REAL, Reals.toReal(1.5f))))),
                                    p.values(col))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDecimalColumn()
    {
        DecimalType addResultType = DecimalType.createDecimalType(11, 2);
        DecimalType sumReturnType = DecimalType.createDecimalType(38, 2);
        tester().assertThat(new RewriteSumWithLiteralAsSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol col = p.symbol("col", DECIMAL_10_2);
                    Symbol expr = p.symbol("expr", addResultType);
                    Symbol out = p.symbol("out", sumReturnType);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    out,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(addResultType, "expr"))),
                                    ImmutableList.of(addResultType))
                            .source(p.project(
                                    Assignments.of(
                                            expr,
                                            new Call(ADD_DECIMAL, ImmutableList.of(
                                                    new Reference(DECIMAL_10_2, "col"),
                                                    new Constant(DECIMAL_10_2, Decimals.valueOfShort(new BigDecimal("1.50")))))),
                                    p.values(col))));
                })
                .doesNotFire();
    }
}
