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
import com.google.common.collect.Lists;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBigintBlock;
import static io.trino.operator.torin.GpuTestUtils.createBlock;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuCoalesce
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    private static Expression add(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }

    @ParameterizedTest
    @MethodSource("coalesceTestCases")
    void testCoalesce(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Coalesce(ImmutableList.of(field(channelA, type), field(channelB, type)));

        if (type == VARBINARY) {
            // cudf replaceNulls has no implementation for the LIST<UINT8> column carrying varbinary
            assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    private static Stream<Object[]> coalesceTestCases()
    {
        return Lists.cartesianProduct(TESTED_GPU_TYPES, ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCoalesceManyOperands(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // The trailing constant is never NULL, so with ALL_NULLS inputs the last operand is the one selected.
        Expression expression = new Coalesce(ImmutableList.of(
                field(0, BIGINT),
                field(1, BIGINT),
                field(2, BIGINT),
                field(3, BIGINT),
                new Constant(BIGINT, 42L)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCoalesceWithCoercedOperand(NullsProvider nullsProvider)
    {
        // COALESCE(bigint, integer) is coerced by the analyzer, so the IR carries a Cast operand.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, INTEGER);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(BIGINT, positionsCount, nullsProvider),
                createBlock(INTEGER, positionsCount, nullsProvider)));

        Expression expression = new Coalesce(ImmutableList.of(
                field(channelA, BIGINT),
                new Cast(field(channelB, INTEGER), BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCoalesceSelectsFirstNonNullOperand()
    {
        int positionsCount = 60;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        int firstSelected = 0;
        int secondSelected = 0;
        int neitherSelected = 0;
        for (int i = 0; i < positionsCount; i++) {
            boolean aIsNull = i % 3 == 0;
            boolean bIsNull = i % 2 == 0;
            if (aIsNull) {
                aBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(aBuilder, i);
            }
            if (bIsNull) {
                bBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(bBuilder, 1000 + i);
            }
            if (!aIsNull) {
                firstSelected++;
            }
            else if (!bIsNull) {
                secondSelected++;
            }
            else {
                neitherSelected++;
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        // test's self-test: the NULL pattern above has to make COALESCE pick the first operand on some
        // rows, the second operand on others, and neither on the rest. Without all three outcomes the
        // assertion below would still pass while never exercising operands past the first.
        assertThat(firstSelected).as("rows taking the first operand").isEqualTo(40);
        assertThat(secondSelected).as("rows taking the second operand").isEqualTo(10);
        assertThat(neitherSelected).as("rows where both operands are NULL").isEqualTo(10);

        Expression expression = new Coalesce(ImmutableList.of(field(0, BIGINT), field(1, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCoalesceFallibleOperandFallibleExceptAllNulls()
    {
        // COALESCE(a, 10 / b): the divide is skipped where a is not NULL, which is where b is 0
        Expression expression = new Coalesce(ImmutableList.of(
                field(0, BIGINT),
                divide(new Constant(BIGINT, 10L), field(1, BIGINT))));
        assertGpuMatchesCpu(coalescePages(), List.of(BIGINT, BIGINT), expression);
    }

    @Test
    void testCoalesceFallibleOperand()
    {
        // COALESCE(a, 10 / coalesce(b, 0)): the inner coalesce turns a NULL b into a zero divisor, so
        // the divide can raise on any row it reaches, and reaches none where a is not NULL
        Expression expression = new Coalesce(ImmutableList.of(
                field(0, BIGINT),
                divide(new Constant(BIGINT, 10L), new Coalesce(ImmutableList.of(field(1, BIGINT), new Constant(BIGINT, 0L))))));
        assertGpuMatchesCpu(coalescePages(), List.of(BIGINT, BIGINT), expression);
    }

    @Test
    void testCoalesceFallibleOperandAheadOfInfallibleOnes()
    {
        // COALESCE(a, 10 / coalesce(b, 0), 42): the trailing operand keeps being evaluated over every
        // row, which only the operand ahead of it is allowed to depend on
        Expression expression = new Coalesce(ImmutableList.of(
                field(0, BIGINT),
                divide(new Constant(BIGINT, 10L), new Coalesce(ImmutableList.of(field(1, BIGINT), new Constant(BIGINT, 0L)))),
                new Constant(BIGINT, 42L)));
        assertGpuMatchesCpu(coalescePages(), List.of(BIGINT, BIGINT), expression);
    }

    @Test
    void testCoalesceFallibleOperandBehindInfallibleOnes()
    {
        // COALESCE(NULL, a, 10 / coalesce(b, 0))
        Expression expression = new Coalesce(ImmutableList.of(
                new Constant(BIGINT, null),
                field(0, BIGINT),
                divide(new Constant(BIGINT, 10L), new Coalesce(ImmutableList.of(field(1, BIGINT), new Constant(BIGINT, 0L))))));
        assertGpuMatchesCpu(coalescePages(), List.of(BIGINT, BIGINT), expression);
    }

    @Test
    void testCoalesceFallibleOperandsSeparatedByInfallibleOne()
    {
        // COALESCE(a, 10 / coalesce(b, 0), 42, 10 / coalesce(b, 0))
        Expression fallible = divide(new Constant(BIGINT, 10L), new Coalesce(ImmutableList.of(field(1, BIGINT), new Constant(BIGINT, 0L))));
        Expression expression = new Coalesce(ImmutableList.of(
                field(0, BIGINT),
                fallible,
                new Constant(BIGINT, 42L),
                fallible));
        assertGpuMatchesCpu(coalescePages(), List.of(BIGINT, BIGINT), expression);
    }

    @Test
    void testCoalescePreservesNestedEnclosingBindings()
    {
        Symbol x = new Symbol(BIGINT, "x");
        Symbol y = new Symbol(BIGINT, "y");
        Symbol z = new Symbol(BIGINT, "z");
        Expression expression = new Let(x, field(1, BIGINT), new Let(y, new Constant(BIGINT, 10L),
                new Coalesce(
                        field(0, BIGINT),
                        new Constant(BIGINT, null),
                        new Let(z, add(y.toSymbolReference(), new Constant(BIGINT, 1L)), divide(z.toSymbolReference(), x.toSymbolReference())))));
        assertGpuMatchesCpu(coalescePages(), List.of(BIGINT, BIGINT), expression);
    }

    private static Expression divide(Expression dividend, Expression divisor)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(dividend, divisor));
    }

    /// A page of `a` and `b` where a NULL `a` always comes with a `b` that divides safely, and every
    /// `b` that does not is paired with a non-NULL `a` that keeps the CPU from reaching it.
    private static List<Page> coalescePages()
    {
        int positionsCount = 64;
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            switch (i % 4) {
                case 0 -> {
                    aBuilder.appendNull();
                    BIGINT.writeLong(bBuilder, 7);
                }
                case 1 -> {
                    aBuilder.appendNull();
                    BIGINT.writeLong(bBuilder, -3);
                }
                case 2 -> {
                    BIGINT.writeLong(aBuilder, 5);
                    BIGINT.writeLong(bBuilder, 0);
                }
                default -> {
                    BIGINT.writeLong(aBuilder, 5);
                    bBuilder.appendNull();
                }
            }
        }
        return List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));
    }
}
