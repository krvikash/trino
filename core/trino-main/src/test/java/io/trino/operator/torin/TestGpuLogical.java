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
package io.trino.operator.torin;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Set;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBigintBlock;
import static io.trino.operator.torin.GpuTestUtils.divisorPages;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.comparison;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuLogical
{
    // The nine {true, false, null} x {true, false, null} combinations, column-wise.
    private static final Boolean[] TRUTH_TABLE_LEFT = {true, true, true, false, false, false, null, null, null};
    private static final Boolean[] TRUTH_TABLE_RIGHT = {true, false, null, true, false, null, true, false, null};

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAnd(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // AND of two comparisons: a < 50 AND b > 0
        Expression left = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression right = comparison(
                ComparisonOperator.LESS_THAN,
                new Constant(BIGINT, 0L),
                field(channelB, BIGINT));
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(left, right));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testOr(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // OR of two comparisons: a < 50 OR b > 0
        Expression left = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression right = comparison(
                ComparisonOperator.LESS_THAN,
                new Constant(BIGINT, 0L),
                field(channelB, BIGINT));
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(left, right));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testAndNullSemantics()
    {
        List<Type> inputTypes = List.of(BOOLEAN, BOOLEAN);
        List<Page> inputPages = List.of(new Page(booleanBlock(TRUTH_TABLE_LEFT), booleanBlock(TRUTH_TABLE_RIGHT)));
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(field(0, BOOLEAN), field(1, BOOLEAN)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testOrNullSemantics()
    {
        List<Type> inputTypes = List.of(BOOLEAN, BOOLEAN);
        List<Page> inputPages = List.of(new Page(booleanBlock(TRUTH_TABLE_LEFT), booleanBlock(TRUTH_TABLE_RIGHT)));
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(field(0, BOOLEAN), field(1, BOOLEAN)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNot(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // NOT of a comparison: NOT(a < 50)
        Expression comparison = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    private static Expression not(Expression expression)
    {
        return new Call(FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)), ImmutableList.of(expression));
    }

    /// `10 / coalesce(divisor, 7)`, which can raise on any row: the check the divide runs is masked by
    /// the dividend being non-NULL, and a constant dividend never is.
    private static Expression divideConstantByCoalesce(Expression divisor)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(
                        new Constant(BIGINT, 10L),
                        new Coalesce(ImmutableList.of(divisor, new Constant(BIGINT, 7L)))));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAndFallibleOperandFallibleExceptAllNulls(NullsProvider nullsProvider)
    {
        // b <> 0 AND a / b = 42: the divide is skipped where b is 0, and yields NULL where b is NULL
        Expression expression = andDivides(divide(field(0, BIGINT), field(1, BIGINT)));
        assertGpuMatchesCpu(divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAndFallibleOperand(NullsProvider nullsProvider)
    {
        // b <> 0 AND 10 / coalesce(b, 7) = 42: coalesce revives a divisor from a NULL b, so the divide
        // can raise on any row, and stays in range on the rows the AND still has to evaluate it on
        Expression expression = andDivides(divideConstantByCoalesce(field(1, BIGINT)));
        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression, Set.of(1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testOrFallibleOperandFallibleExceptAllNulls(NullsProvider nullsProvider)
    {
        // b = 0 OR a / b = 42
        Expression expression = orDivides(divide(field(0, BIGINT), field(1, BIGINT)));
        assertGpuMatchesCpu(divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testOrFallibleOperand(NullsProvider nullsProvider)
    {
        // b = 0 OR 10 / coalesce(b, 7) = 42
        Expression expression = orDivides(divideConstantByCoalesce(field(1, BIGINT)));
        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression, Set.of(1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAndFallibleOperandBehindInfallibleOnes(NullsProvider nullsProvider)
    {
        // a < 0 AND b <> 0 AND 10 / coalesce(b, 7) = 42
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(
                comparison(ComparisonOperator.LESS_THAN, field(0, BIGINT), new Constant(BIGINT, 0L)),
                not(comparison(ComparisonOperator.EQUAL, field(1, BIGINT), new Constant(BIGINT, 0L))),
                comparison(ComparisonOperator.EQUAL, divideConstantByCoalesce(field(1, BIGINT)), new Constant(BIGINT, 42L))));
        assertGpuMatchesCpu(divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testOrFallibleOperandBehindInfallibleOnes(NullsProvider nullsProvider)
    {
        // a < 0 OR b = 0 OR 10 / coalesce(b, 7) = 42
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(
                comparison(ComparisonOperator.LESS_THAN, field(0, BIGINT), new Constant(BIGINT, 0L)),
                comparison(ComparisonOperator.EQUAL, field(1, BIGINT), new Constant(BIGINT, 0L)),
                comparison(ComparisonOperator.EQUAL, divideConstantByCoalesce(field(1, BIGINT)), new Constant(BIGINT, 42L))));
        assertGpuMatchesCpu(divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAndFallibleOperandBehindNullHead(NullsProvider nullsProvider)
    {
        // b <> 0 AND 10 / coalesce(b, 7) <> 42
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(
                not(comparison(ComparisonOperator.EQUAL, field(1, BIGINT), new Constant(BIGINT, 0L))),
                not(comparison(ComparisonOperator.EQUAL, divideConstantByCoalesce(field(1, BIGINT)), new Constant(BIGINT, 42L)))));
        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression, Set.of(1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testOrFallibleOperandBehindNullHead(NullsProvider nullsProvider)
    {
        // b = 0 OR 10 / coalesce(b, 7) <> 42
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(
                comparison(ComparisonOperator.EQUAL, field(1, BIGINT), new Constant(BIGINT, 0L)),
                not(comparison(ComparisonOperator.EQUAL, divideConstantByCoalesce(field(1, BIGINT)), new Constant(BIGINT, 42L)))));
        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression, Set.of(1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLogicalPreservesEnclosingBinding(NullsProvider nullsProvider)
    {
        Symbol x = new Symbol(BIGINT, "x");
        Expression quotient = divide(new Constant(BIGINT, 10L), x.toSymbolReference());
        for (Expression body : List.of(andDivides(quotient), orDivides(quotient))) {
            Expression expression = new Let(x, field(1, BIGINT), body);
            // TODO divisorPages generates two columns, but the test uses only one
            assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression, Set.of(1));
        }
    }

    /// `b <> 0 AND divide = 42`, where the CPU never evaluates `divide` on a zero `b`.
    private static Expression andDivides(Expression divide)
    {
        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(1, BIGINT), new Constant(BIGINT, 0L))));
        return new Logical(Logical.Operator.AND, ImmutableList.of(
                bNotZero,
                comparison(ComparisonOperator.EQUAL, divide, new Constant(BIGINT, 42L))));
    }

    /// `b = 0 OR divide = 42`, where the CPU never evaluates `divide` on a zero `b`.
    private static Expression orDivides(Expression divide)
    {
        return new Logical(Logical.Operator.OR, ImmutableList.of(
                comparison(ComparisonOperator.EQUAL, field(1, BIGINT), new Constant(BIGINT, 0L)),
                comparison(ComparisonOperator.EQUAL, divide, new Constant(BIGINT, 42L))));
    }

    private static Expression divide(Expression dividend, Expression divisor)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(dividend, divisor));
    }

    private static Block booleanBlock(Boolean[] values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, values.length);
        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }
        return builder.build();
    }
}
