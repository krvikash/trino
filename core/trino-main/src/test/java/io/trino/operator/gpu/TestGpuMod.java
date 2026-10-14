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
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createRepeatingValuesBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuMod
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("modTestCases")
    void testMod(Type type, List<Object> dividends, List<Object> divisors, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 2 * dividends.size() * divisors.size();
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, dividends, 1),
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, divisors, dividends.size())));

        assertGpuMatchesCpu(inputPages, inputTypes, mod(type, field(channelA, type), field(channelB, type)), Set.of(channelA, channelB));
    }

    private static Stream<Arguments> modTestCases()
    {
        // Integer divisors exclude 0, on which the CPU and GPU fail differently, see testIntegerModByZero
        Map<Type, List<List<Object>>> valuesByType = ImmutableMap.of(
                TINYINT, List.of(
                        List.of(0L, 1L, 7L, -7L, 127L, -128L),
                        List.of(1L, -1L, 3L, -3L, 127L, -128L)),
                SMALLINT, List.of(
                        List.of(0L, 1L, 7L, -7L, 32767L, -32768L),
                        List.of(1L, -1L, 3L, -3L, 32767L, -32768L)),
                INTEGER, List.of(
                        List.of(0L, 1L, 7L, -7L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE),
                        List.of(1L, -1L, 3L, -3L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE)),
                BIGINT, List.of(
                        List.of(0L, 1L, 7L, -7L, Long.MAX_VALUE, Long.MIN_VALUE),
                        List.of(1L, -1L, 3L, -3L, Long.MAX_VALUE, Long.MIN_VALUE)),
                DOUBLE, List.of(
                        List.of(0.0, -0.0, 7.5, -7.5, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN),
                        List.of(0.0, -0.0, 3.0, -3.1, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN)),
                REAL, List.of(
                        realValues(0.0f, -0.0f, 7.5f, -7.5f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN),
                        realValues(0.0f, -0.0f, 3.0f, -3.1f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN)));
        return valuesByType.entrySet().stream()
                .flatMap(entry -> Stream.of(NullsProvider.values())
                        .map(nullsProvider -> arguments(entry.getKey(), entry.getValue().get(0), entry.getValue().get(1), nullsProvider)));
    }

    @Test
    void testIntegerModByZero()
    {
        // The CPU fails with a bare ArithmeticException, while the GPU reports DIVISION_BY_ZERO, like for the % operator
        for (Type type : List.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            Expression expression = mod(type, field(0, type), field(1, type));
            List<Type> inputTypes = List.of(type, type);
            CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
            List<Page> inputPages = List.of(new Page(writeNativeValue(type, 5L), writeNativeValue(type, 0L)));
            assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                    .hasErrorCode(DIVISION_BY_ZERO);
        }
    }

    @Test
    void testDecimalModFallsBack()
    {
        Type type = createDecimalType(10, 2);
        Expression expression = mod(type, field(0, type), field(1, type));
        assertThat(compileExpression(expression, layoutFor(List.of(type, type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private static Expression mod(Type type, Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("mod", fromTypes(type, type)),
                ImmutableList.of(left, right));
    }

    private static List<Object> realValues(float... values)
    {
        ImmutableList.Builder<Object> result = ImmutableList.builder();
        for (float value : values) {
            result.add((long) floatToRawIntBits(value));
        }
        return result.build();
    }
}
