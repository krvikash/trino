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
package io.trino.operator.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createRepeatingValuesBlock;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.TestingIr.nullIf;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuNullIf
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
    @MethodSource("nullIfTestCases")
    void testNullIf(Type type, List<Object> values, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        // Both operands cycle through the same few values, the second one advancing a value only
        // once the first has cycled through all of them. That walks every pair, so the operands are
        // equal on a quarter of the rows. The shared block generators would not do: each reseeds
        // Random with the same constant, so two columns come out identical and every row takes the
        // NULL branch, leaving the pass-through branch unexercised.
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, values, 1),
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, values, values.size())));

        // NULLIF(a, b)
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, type), field(channelB, type));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    private static Stream<Arguments> nullIfTestCases()
    {
        Map<Type, List<Object>> valuesByType = ImmutableMap.of(
                BIGINT, ImmutableList.of(-1L, 0L, 7L, 42L),
                INTEGER, ImmutableList.of(-1L, 0L, 7L, 42L),
                DATE, ImmutableList.of(-1L, 0L, 7L, 42L),
                createDecimalType(10, 2), ImmutableList.of(-1L, 0L, 7L, 42L),
                DOUBLE, ImmutableList.of(-1.5, 0.0, 7.25, 42.5),
                VARCHAR, ImmutableList.of(Slices.utf8Slice(""), Slices.utf8Slice("alpha"), Slices.utf8Slice("beta"), Slices.utf8Slice("\u0141\u0105ka")));
        return valuesByType.entrySet().stream()
                .flatMap(entry -> Stream.of(NullsProvider.values())
                        .map(nullsProvider -> arguments(entry.getKey(), entry.getValue(), nullsProvider)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNullIfWithNullSecondOperand(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // NULLIF(a, NULL): the equality is NULL on every row, so the CASE must fall through to its
        // default and yield `a` — a NULL condition is not-matched, neither a match nor a NULL result.
        // Unlike the plain IF tests, the branches differ on every non-null row here, so treating the
        // NULL condition as a match would show up as a wrongly NULLed-out result.
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, BIGINT), new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNullIfWithNonTrivialValue(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -3, 3),
                createBigintBlock(positionsCount, nullsProvider, -3, 3)));

        // NULLIF(a + b, 0) — the non-trivial first operand is bound with a Let so it is evaluated once
        Expression expression = nullIf(
                emptySymbolAllocator(),
                add(field(channelA, BIGINT), field(channelB, BIGINT)),
                new Constant(BIGINT, 0L));
        assertThat(expression).isInstanceOf(Let.class);

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @MethodSource("nullIfWithIntegerZeroTestCases")
    void testNullIfWithIntegerZero(Type type, List<Object> values, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        int positionsCount = 64;
        // A quarter of the values are zero, so the NULL-producing branch is actually taken.
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, values, 1)));

        // NULLIF(a, integer 0), the divide-by-zero guard in `x / NULLIF(y, 0)`: the integer zero is
        // cast to the column type inside the comparison, while the result keeps the column type.
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, type), new Constant(INTEGER, 0L), type);
        assertThat(expression.type()).isEqualTo(type);

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    private static Stream<Arguments> nullIfWithIntegerZeroTestCases()
    {
        // decimal(38,4) is a long decimal, carried on cuDF's DECIMAL128 rather than the INT64 that
        // short decimals use, so it exercises a different code path than the decimal in testNullIf.
        Map<Type, List<Object>> valuesByType = ImmutableMap.of(
                BIGINT, ImmutableList.of(0L, 1L, -7L, 42L),
                createDecimalType(38, 4), ImmutableList.of(
                        Int128.ZERO,
                        Int128.valueOf(1),
                        Int128.valueOf(-70000),
                        Int128.valueOf("12345678901234567890123456789012345678")));
        return valuesByType.entrySet().stream()
                .flatMap(entry -> Stream.of(NullsProvider.values())
                        .map(nullsProvider -> arguments(entry.getKey(), entry.getValue(), nullsProvider)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNullIfWithMixedOperandTypes(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(INTEGER, BIGINT);
        int positionsCount = 64;
        // Few enough values that the operands are equal on a quarter of the rows — see testNullIf.
        List<Object> values = ImmutableList.of(-1L, 0L, 7L, 42L);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(INTEGER, positionsCount, nullsProvider, values, 1),
                createRepeatingValuesBlock(BIGINT, positionsCount, nullsProvider, values, values.size())));

        // NULLIF(a, b) over an integer and a bigint operand: the equality is performed at bigint, so a
        // Cast appears inside the condition, while the result keeps the integer type of the first operand.
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, INTEGER), field(channelB, BIGINT), BIGINT);
        assertThat(expression.type()).isEqualTo(INTEGER);

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }
}
