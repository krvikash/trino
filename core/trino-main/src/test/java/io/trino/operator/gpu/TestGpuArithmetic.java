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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertDeclaredBehaviorOnAllNulls;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.cartesianProduct;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuArithmetic
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testRealToIntegerCastPrecision()
    {
        // Odd integers in [2^23, 2^24) are exactly representable in float32 but adding 0.5
        // requires 25 significand bits, exceeding float32's 24. IEEE 754 round-to-nearest-even
        // rounds the sum up to value+1 (because the last kept bit is 1), producing an off-by-one
        // after truncation. The GPU must widen to float64 for the rounding arithmetic to match
        // CPU semantics.
        int[] values = {
                (1 << 23) + 1,
                (1 << 24) - 1,
                -((1 << 23) + 1),
                -((1 << 24) - 1),
        };

        Expression expression = new Cast(field(0, REAL), INTEGER);
        Map<Symbol, Integer> layout = layoutFor(List.of(REAL));
        CompiledExpression compiledGpu = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);

        for (int value : values) {
            long realBits = Float.floatToIntBits((float) value);
            Page input = new Page(writeNativeValue(REAL, realBits));
            assertGpuMatchesCpuForCast(List.of(input), List.of(REAL), expression, cpuProcessor, compiledGpu);
        }
    }

    @Test
    void testNumericOperatorsCorrectnessSmoke()
    {
        List<Type> testedTypes = List.of(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                createDecimalType(1, 0),
                createDecimalType(3, 0), // can hold max tinyint
                createDecimalType(5, 0), // can hold max smallint
                createDecimalType(10, 0), // can hold max integer
                createDecimalType(19, 0), // can hold max bigint
                createDecimalType(13, 0),
                createDecimalType(13, 2),
                createDecimalType(27, 0),
                createDecimalType(27, 5),
                createDecimalType(38),
                NUMBER,
                createVarcharType(1),
                createVarcharType(3),
                createVarcharType(5),
                createVarcharType(20),
                VARCHAR);
        List<TrinoNumber> testedNumbers = numericValuesToTest();

        LoadingCache<Type, List<@Nullable Object>> testedTypeValues = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(type -> {
                    List<@Nullable Object> values = tryCastToAndFilter(NUMBER, type, testedNumbers);
                    verify(!values.isEmpty());
                    return values;
                }));

        LoadingCache<Type, List<Page>> unaryInputs = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(type -> testedTypeValues.getUnchecked(type).stream()
                        .map(value -> new Page(writeNativeValue(type, value)))
                        .collect(toImmutableList())));

        LoadingCache<Pair<Type, Type>, List<Page>> binaryInputs = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(pair -> cartesianProduct(
                        pair.first(),
                        testedTypeValues.getUnchecked(pair.first()),
                        pair.second(),
                        testedTypeValues.getUnchecked(pair.second()))));

        Set<String> testedOperators = new HashSet<>();
        Set<String> gpuEnabledOperators = new HashSet<>();

        // Unary operators
        for (Type type : testedTypes) {
            for (OperatorType operator : OperatorType.values()) {
                if (operator.getArgumentCount() != 1) {
                    continue;
                }
                if (operator == OperatorType.CAST || operator == OperatorType.SATURATED_FLOOR_CAST) {
                    // Casts tested below in the (fromType, toType) cross-product.
                    continue;
                }

                ResolvedFunction function;
                try {
                    function = FUNCTION_RESOLUTION.resolveOperator(operator, List.of(type));
                }
                catch (OperatorNotFoundException e) {
                    continue;
                }

                // Inspect coercions if any
                Type coerced = getOnlyElement(function.signature().getArgumentTypes());
                if (type.equals(coerced)) {
                    // Let's test, this is what we're here for
                }
                else if (testedTypes.contains(coerced)) {
                    // Will be tested explicitly on separate round
                    continue;
                }
                else {
                    // Might not be covered by explicit tests; expect this only for decimal coercions
                    verify(type instanceof DecimalType,
                            "Unexpected coercion for unary %s on %s: argument types %s",
                            operator,
                            type,
                            function.signature().getArgumentTypes());
                }

                testedOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                Expression expression = buildUnaryOperatorExpression(operator, function, field(0, coerced));
                Map<Symbol, Integer> layout = layoutFor(List.of(coerced));
                Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY);
                if (gpuExpression.isEmpty()) {
                    // Not supported for GPU execution
                    continue;
                }
                gpuEnabledOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                    for (Page input : unaryInputs.getUnchecked(coerced)) {
                        assertThat(input.getPositionCount()).isEqualTo(1);
                        assertGpuMatchesCpu(List.of(input), List.of(coerced), expression, cpuProcessor, compiledGpu, false);
                    }
                }
                catch (AssertionError failure) {
                    failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                    throw failure;
                }
            }
        }

        // Cast operators: cross-product over (fromType, toType).
        for (Type fromType : testedTypes) {
            for (Type toType : testedTypes) {
                // CPU's REAL→BIGINT silently saturates for ±Inf and finite OOR (uses (long) cast which clamps),
                // while GPU correctly throws INVALID_CAST_ARGUMENT. Skip until CPU is fixed.
                // TODO: re-enable once CPU REAL→BIGINT throws consistently.
                if (fromType.equals(REAL) && toType.equals(BIGINT)) {
                    continue;
                }
                try {
                    FUNCTION_RESOLUTION.getCoercion(fromType, toType);
                }
                catch (OperatorNotFoundException e) {
                    continue;
                }

                String label = "CAST %s AS %s".formatted(fromType.getDisplayName(), toType.getDisplayName());
                testedOperators.add(label);

                Expression expression = new Cast(field(0, fromType), toType);
                Map<Symbol, Integer> layout = layoutFor(List.of(fromType));
                Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY);
                if (gpuExpression.isEmpty()) {
                    continue;
                }
                gpuEnabledOperators.add(label);

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                    for (Page input : unaryInputs.getUnchecked(fromType)) {
                        assertThat(input.getPositionCount()).isEqualTo(1);
                        assertGpuMatchesCpuForCast(List.of(input), List.of(fromType), expression, cpuProcessor, compiledGpu);
                    }
                }
                catch (AssertionError failure) {
                    failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                    throw failure;
                }
            }
        }

        // Binary operators: cross-product over (leftType, rightType)
        for (Type leftType : testedTypes) {
            for (Type rightType : testedTypes) {
                for (OperatorType operator : OperatorType.values()) {
                    if (operator.getArgumentCount() != 2) {
                        continue;
                    }
                    if (operator == OperatorType.EQUAL || operator == OperatorType.LESS_THAN || operator == OperatorType.LESS_THAN_OR_EQUAL) {
                        continue;
                    }

                    ResolvedFunction function;
                    try {
                        function = FUNCTION_RESOLUTION.resolveOperator(operator, List.of(leftType, rightType));
                    }
                    catch (OperatorNotFoundException e) {
                        continue;
                    }

                    switch (operator) {
                        case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO, NEGATION, SUBSCRIPT -> {
                            // Binary operators tested here
                        }
                        case EQUAL, COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST, LESS_THAN, LESS_THAN_OR_EQUAL, IDENTICAL -> {
                            // Covered by testComparison
                            assertThat(Set.copyOf(function.signature().getArgumentTypes()))
                                    .as(
                                            """
                                            Expect comparison operators to require pre-coerced inputs. If this is the case, comparisons have test coverage in testComparison.
                                            Otherwise test coverage needs to be revisited.
                                            """)
                                    .hasSize(1);
                        }
                        case CAST, SATURATED_FLOOR_CAST, HASH_CODE, XX_HASH_64, INDETERMINATE, READ_VALUE -> throw new AssertionError("Unreachable, not a binary operator");
                    }

                    // Inspect coercions if any
                    if (List.of(leftType, rightType).equals(function.signature().getArgumentTypes())) {
                        // Let's test, this is what we're here for
                    }
                    else if (testedTypes.containsAll(function.signature().getArgumentTypes())) {
                        // Will be tested explicitly on separate round
                        continue;
                    }
                    else {
                        // Might not be covered by explicit tests, so let's test this.
                        // This should be the case only for decimal types.
                        verify(leftType instanceof DecimalType || rightType instanceof DecimalType, "Neither is decimal: %s, %s", leftType, rightType);
                    }

                    assertThat(function.signature().getArgumentTypes()).hasSize(2);
                    Type leftCoerced = function.signature().getArgumentTypes().get(0);
                    Type rightCoerced = function.signature().getArgumentTypes().get(1);

                    testedOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    Expression expression = new Call(function, ImmutableList.of(field(0, leftCoerced), field(1, rightCoerced)));
                    Map<Symbol, Integer> layout = layoutFor(List.of(leftCoerced, rightCoerced));
                    Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY);
                    if (gpuExpression.isEmpty()) {
                        // Not supported for GPU execution
                        continue;
                    }
                    gpuEnabledOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    try {
                        CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                        assertThat(compiledGpu.inputChannels().getInputChannels())
                                .containsExactlyInAnyOrderElementsOf(Set.of(0, 1));
                        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                        for (Page input : binaryInputs.getUnchecked(new Pair<>(leftCoerced, rightCoerced))) {
                            assertThat(input.getPositionCount()).isEqualTo(1);
                            assertGpuMatchesCpu(List.of(input), List.of(leftCoerced, rightCoerced), expression, cpuProcessor, compiledGpu, false);
                        }
                    }
                    catch (AssertionError failure) {
                        failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                        throw failure;
                    }
                }
            }
        }

        // Self-test
        assertThat(testedOperators)
                .contains(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint",
                        "bigint + bigint",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)",
                        "CAST bigint AS integer",
                        "CAST double AS bigint",
                        "CAST real AS tinyint");

        // Self-test
        assertThat(gpuEnabledOperators)
                .contains(
                        "bigint + bigint",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(13,0) + decimal(13,0)",
                        "decimal(13,0) + decimal(13,2)",
                        "decimal(3,0) - decimal(3,0)",
                        "decimal(13,0) - decimal(13,0)",
                        "decimal(13,0) - decimal(13,2)",
                        "decimal(3,0) * decimal(3,0)",
                        "decimal(13,0) * decimal(13,0)",
                        "decimal(13,0) * decimal(13,2)",
                        "CAST bigint AS integer",
                        "CAST double AS bigint",
                        "CAST real AS tinyint",
                        "CAST double AS real",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)",
                        "decimal(27,0) * decimal(3,0)")
                .doesNotContain(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint");
    }

    private static List<@Nullable TrinoNumber> numericValuesToTest()
    {
        List<Long> initial = List.of(
                0L,
                -1L,
                1L,
                (long) Byte.MIN_VALUE,
                (long) Byte.MAX_VALUE,
                (long) Short.MIN_VALUE,
                (long) Short.MAX_VALUE,
                (long) Integer.MIN_VALUE,
                (long) Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MAX_VALUE);
        List<BigDecimal> offsets = Stream.of("0", "1", "-1", "0.33", "0.5", "0.66", "-0.33", "-0.5", "-0.66")
                .map(BigDecimal::new)
                .toList();
        List<TrinoNumber> testedNumbers = new ArrayList<>();
        testedNumbers.add(null);
        for (long value : initial) {
            BigDecimal asBigDecimal = BigDecimal.valueOf(value);
            for (BigDecimal offset : offsets) {
                testedNumbers.add(TrinoNumber.from(asBigDecimal.add(offset)));
            }
        }
        testedNumbers.add(TrinoNumber.from(BigDecimal.valueOf(Math.PI)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.Infinity(false)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.Infinity(true)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.NotANumber()));
        return testedNumbers;
    }

    private List<@Nullable Object> tryCastToAndFilter(Type sourceType, Type targetType, List<@Nullable ?> sourceNativeValues)
    {
        List<Object> nativeValues = new ArrayList<>();
        Expression castExpression = new Cast(field(0, sourceType), targetType);
        Map<Symbol, Integer> layout = layoutFor(List.of(sourceType));
        PageProcessor pageProcessor = compileCpuExpression(castExpression, layout);
        for (@Nullable Object sourceValue : sourceNativeValues) {
            Page sourcePage = new Page(writeNativeValue(sourceType, sourceValue));
            List<Page> result;
            try {
                result = executeWithCpu(pageProcessor, List.of(sourcePage));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()) || e.getErrorCode().equals(INVALID_CAST_ARGUMENT.toErrorCode())) {
                    continue;
                }
                throw e;
            }
            assertThat(result).hasSize(1);
            Page resultPage = getOnlyElement(result);
            assertThat(resultPage.getPositionCount()).isEqualTo(1);
            assertThat(resultPage.getChannelCount()).isEqualTo(1);
            @Nullable Object targetValue = readNativeValue(targetType, resultPage.getBlock(0), 0);
            nativeValues.add(targetValue);
        }
        return nativeValues;
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testArithmeticOperators(NullsProvider nullsProvider)
    {
        testArithmetic(OperatorType.ADD, nullsProvider);
        testArithmetic(OperatorType.SUBTRACT, nullsProvider);
        testArithmetic(OperatorType.MULTIPLY, nullsProvider);
        testArithmetic(OperatorType.DIVIDE, nullsProvider);
        testArithmetic(OperatorType.MODULO, nullsProvider);
    }

    private void testArithmetic(OperatorType operatorType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000),
                createBigintBlock(positionsCount, nullsProvider, 1, 100)));  // non-zero to avoid division by zero

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(operatorType, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    /**
     * Like {@link GpuTestUtils#assertGpuMatchesCpu(List, List, Expression, PageProcessor, CompiledExpression, boolean)},
     * but accepts {@code INVALID_CAST_ARGUMENT} from GPU when CPU returns {@code NUMERIC_VALUE_OUT_OF_RANGE}.
     * Used in the cast smoke loop.
     */
    // TODO: remove once CPU is fixed to consistently throw INVALID_CAST_ARGUMENT for ±Inf and finite OOR
    //  in float→int casts; switch the call site to plain assertGpuMatchesCpu.
    private void assertGpuMatchesCpuForCast(List<Page> inputPages, List<Type> inputTypes, Expression expression, PageProcessor cpuProcessor, CompiledExpression gpuExpression)
    {
        assertDeclaredBehaviorOnAllNulls(inputPages, inputTypes, expression, gpuExpression);

        List<Page> cpuResults;
        try {
            cpuResults = executeWithCpu(cpuProcessor, inputPages);
        }
        catch (TrinoException cpuExecutionException) {
            assertThat(inputPages.stream().mapToLong(Page::getPositionCount).sum())
                    .describedAs("Cast smoke loop is single-row per page")
                    .isEqualTo(1);
            ErrorCodeSupplier[] acceptable = NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().equals(cpuExecutionException.getErrorCode())
                    ? new ErrorCodeSupplier[] {NUMERIC_VALUE_OUT_OF_RANGE, INVALID_CAST_ARGUMENT}
                    : new ErrorCodeSupplier[] {cpuExecutionException::getErrorCode};
            try {
                assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                        .hasErrorCode(acceptable);
            }
            catch (AssertionError failure) {
                failure.addSuppressed(new Exception("expression: " + expression));
                failure.addSuppressed(new Exception("inputTypes: " + inputTypes));
                failure.addSuppressed(new Exception("CPU execution exception", cpuExecutionException));
                throw failure;
            }
            return;
        }
        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, expression, gpuExpression);
        assertSameDataInOrder(gpuResults, cpuResults, List.of(expression.type()));
    }

    private static Expression buildUnaryOperatorExpression(OperatorType operator, ResolvedFunction function, Expression argument)
    {
        if (operator == OperatorType.CAST) {
            return new Cast(argument, function.signature().getReturnType());
        }
        // Note: SATURATED_FLOOR_CAST is still expressed as a Call (operator function), not a Cast IR node.
        return new Call(function, ImmutableList.of(argument));
    }

    private record Pair<F, S>(F first, S second)
    {
        Pair
        {
            requireNonNull(first, "first is null");
            requireNonNull(second, "second is null");
        }
    }
}
