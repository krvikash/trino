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
import io.airlift.slice.Slice;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.DoubleSupplier;

import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.gpu.expression.Fallibility.INFALLIBLE;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.DoubleOperators.castToVarchar;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuDoubleToVarcharCast
{
    private static final int RANDOM_POSITIONS = 100_000;
    // Below this magnitude the CPU prints the shortest digits
    private static final double SHORTEST_DIGITS_ON_CPU_LIMIT = 0x1p54;
    private static final double NOT_SHORTEST_DIGITS_ON_CPU_END = 0x1p85;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testSpecialValues()
    {
        List<Double> values = Arrays.asList(
                0.0,
                -0.0,
                Double.NaN,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                null,
                Math.nextDown(Double.MIN_NORMAL),
                Double.MIN_NORMAL,
                Double.MAX_VALUE,
                -Double.MAX_VALUE);
        for (Type targetType : List.of(VARCHAR, createVarcharType(24))) {
            assertGpuMatchesCpu(doublePages(values), List.of(DOUBLE), cast(targetType));
        }
    }

    @Test
    void testSmallestSubnormals()
    {
        // Where the shortest digits are a single one, the CPU prints two digits, e.g. 4.9E-323 for
        // 10 * Double.MIN_VALUE, and for 20 * Double.MIN_VALUE 1.0E-322 on JDK 25 but 9.9E-323 on JDK 26+
        ImmutableList.Builder<Double> values = ImmutableList.builder();
        for (int multiple = 1; multiple <= 64; multiple++) {
            values.add(multiple * Double.MIN_VALUE).add(-multiple * Double.MIN_VALUE);
        }
        assertGpuMatchesCpu(doublePages(values.build()), List.of(DOUBLE), cast(VARCHAR));
    }

    @Test
    void testDecimalValues()
    {
        List<Double> values = Arrays.asList(
                1.0,
                -1.0,
                0.1,
                -0.1,
                1.0 / 3,
                -2.0 / 3,
                4.35,
                0.3,
                0.01,
                12.34,
                -99999.99,
                123456789.123456789,
                0.001,
                0.00099999,
                9999999.0,
                1e7,
                1e-5,
                1e22,
                Math.PI,
                -Math.E,
                // 2^53 + 2, an integer that needs every digit
                9007199254740994.0,
                // Largest magnitude below 2^54
                Math.nextDown(SHORTEST_DIGITS_ON_CPU_LIMIT));
        for (Type targetType : List.of(VARCHAR, createVarcharType(24), createVarcharType(100))) {
            assertGpuMatchesCpu(doublePages(values), List.of(DOUBLE), cast(targetType));
        }
    }

    @Test
    void testPowersOfTwo()
    {
        // Every power of two and its neighbors, apart from the magnitudes where the CPU digits are not the shortest
        ImmutableList.Builder<Double> values = ImmutableList.builder();
        for (int power = -1074; power <= 1023; power++) {
            double powerOfTwo = Math.scalb(1.0, power);
            for (double value : List.of(Math.nextDown(powerOfTwo), powerOfTwo, Math.nextUp(powerOfTwo))) {
                if ((value > 0 && value < SHORTEST_DIGITS_ON_CPU_LIMIT) || (value >= NOT_SHORTEST_DIGITS_ON_CPU_END && Double.isFinite(value))) {
                    values.add(value).add(-value);
                }
            }
        }
        assertGpuMatchesCpu(doublePages(values.build()), List.of(DOUBLE), cast(VARCHAR));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCentsLikeValues(NullsProvider nullsProvider)
    {
        Random random = new Random(1);
        List<Page> pages = randomDoublePages(nullsProvider, () -> (random.nextInt(2_000_000_000) - 1_000_000_000) / 100.0);
        assertGpuMatchesCpu(pages, List.of(DOUBLE), cast(VARCHAR));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testRandomBitPatternsBelowShortestDigitsLimit(NullsProvider nullsProvider)
    {
        Random random = new Random(2);
        List<Page> pages = randomDoublePages(nullsProvider, () -> {
            double value = Double.longBitsToDouble(random.nextLong());
            return Math.abs(value) < SHORTEST_DIGITS_ON_CPU_LIMIT ? value : Math.scalb(value, -1100);
        });
        assertGpuMatchesCpu(pages, List.of(DOUBLE), cast(VARCHAR));
    }

    @Test
    void testRandomMagnitudesBelowShortestDigitsLimit()
    {
        Random random = new Random(3);
        List<Page> pages = randomDoublePages(
                NullsProvider.NO_NULLS,
                () -> Math.scalb(1 + random.nextDouble(), random.nextInt(1074 + 53) - 1074) * (random.nextBoolean() ? 1 : -1));
        assertGpuMatchesCpu(pages, List.of(DOUBLE), cast(VARCHAR));
    }

    @Test
    void testRandomMagnitudesAboveNotShortestDigitsRange()
    {
        Random random = new Random(6);
        List<Page> pages = randomDoublePages(
                NullsProvider.NO_NULLS,
                () -> Math.scalb(1 + random.nextDouble(), 85 + random.nextInt(1024 - 85)) * (random.nextBoolean() ? 1 : -1));
        assertGpuMatchesCpu(pages, List.of(DOUBLE), cast(VARCHAR));
    }

    @Test
    void testValuesWhereCpuDigitsAreNotShortest()
    {
        // In [2^54, 2^85) the CPU sometimes prints a digit more than needed, e.g. 1.9999999999999998E23 for 2.0E23
        Random random = new Random(4);
        double[] values = new double[RANDOM_POSITIONS];
        values[0] = 2e23;
        values[1] = -2e23;
        for (int i = 2; i < values.length; i++) {
            values[i] = Math.scalb(1 + random.nextDouble(), 54 + random.nextInt(85 - 54)) * (random.nextBoolean() ? 1 : -1);
        }
        List<String> results = castOnGpu(values);
        assertThat(results.get(0)).isEqualTo("2.0E23");
        assertThat(results.get(1)).isEqualTo("-2.0E23");
        for (int i = 0; i < values.length; i++) {
            assertWellFormedAndParsesBackTo(results.get(i), values[i]);
        }
    }

    @Test
    void testRandomBitPatternsParseBack()
    {
        Random random = new Random(5);
        double[] values = new double[RANDOM_POSITIONS];
        for (int i = 0; i < values.length; i++) {
            do {
                values[i] = Double.longBitsToDouble(random.nextLong());
            }
            while (!Double.isFinite(values[i]) || values[i] == 0);
        }
        List<String> results = castOnGpu(values);
        for (int i = 0; i < values.length; i++) {
            assertWellFormedAndParsesBackTo(results.get(i), values[i]);
        }
    }

    private static void assertWellFormedAndParsesBackTo(String actual, double value)
    {
        assertThat(actual)
                .describedAs("Cast of %s", value)
                .matches("-?[1-9]\\.[0-9]+E(0|-?[1-9][0-9]*)")
                .hasSizeLessThanOrEqualTo(24);
        assertThat(Double.parseDouble(actual))
                .describedAs("Cast of %s parsed back from %s", value, actual)
                .isEqualTo(value);
    }

    private static List<String> castOnGpu(double[] values)
    {
        Expression expression = cast(VARCHAR);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(DOUBLE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        List<Page> inputPages = List.of(new Page(createFloatingPointBlock(DOUBLE, Arrays.stream(values).boxed().toList())));
        ImmutableList.Builder<String> results = ImmutableList.builder();
        for (Page page : executeWithGpu(inputPages, List.of(DOUBLE), expression, gpuExpression)) {
            Block block = page.getBlock(0);
            for (int position = 0; position < block.getPositionCount(); position++) {
                Slice slice = VARCHAR.getSlice(block, position);
                results.add(slice.toStringUtf8());
            }
        }
        return results.build();
    }

    @Test
    void testBoundedVarchar()
    {
        List<Double> fitting = Arrays.asList(null, 0.0, -0.0, 1.0, -1.5, 0.1, 12.5, Double.NaN, Double.POSITIVE_INFINITY, null);
        for (int bound = 8; bound <= 23; bound++) {
            assertGpuMatchesCpu(doublePages(fitting), List.of(DOUBLE), cast(createVarcharType(bound)));
        }
        for (double value : List.of(Double.NEGATIVE_INFINITY, -1.5, 0.1, 1.0 / 3, -Double.MIN_VALUE, Double.MAX_VALUE, -Double.MIN_NORMAL)) {
            int length = castToVarchar(Integer.MAX_VALUE, value).length();
            assertGpuMatchesCpu(doublePages(List.of(value)), List.of(DOUBLE), cast(createVarcharType(length)));
            assertGpuMatchesCpu(doublePages(List.of(value)), List.of(DOUBLE), cast(createVarcharType(length - 1)));
        }
    }

    @Test
    void testBoundedVarcharFailureMessage()
    {
        // The second non-NULL row is the first one too long for varchar(5)
        List<Double> values = Arrays.asList(null, 1.0, 1.0 / 3, -2.5e-300, null);
        VarcharType targetType = createVarcharType(5);
        Expression expression = cast(targetType);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(DOUBLE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertThat(gpuExpression.expression().fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        TrinoException cpuFailure = catchThrowableOfType(TrinoException.class, () -> castToVarchar(5, 1.0 / 3));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(doublePages(values), List.of(DOUBLE), expression, gpuExpression))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage(cpuFailure.getMessage());
    }

    @Test
    void testUnboundedOrLongEnoughVarcharIsNotChecked()
    {
        for (Type targetType : List.of(VARCHAR, createVarcharType(24), createVarcharType(1000))) {
            CompiledExpression gpuExpression = compileExpression(cast(targetType), layoutFor(List.of(DOUBLE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
            assertThat(gpuExpression.expression().fallibility()).describedAs("cast to %s", targetType).isEqualTo(INFALLIBLE);
        }
        CompiledExpression bounded = compileExpression(cast(createVarcharType(23)), layoutFor(List.of(DOUBLE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertThat(bounded.expression().fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
    }

    private static Expression cast(Type targetType)
    {
        return new Cast(field(0, DOUBLE), targetType);
    }

    private static List<Page> doublePages(List<Double> values)
    {
        return List.of(new Page(createFloatingPointBlock(DOUBLE, values)));
    }

    private static List<Page> randomDoublePages(NullsProvider nullsProvider, DoubleSupplier values)
    {
        Optional<long[]> validity = nullsProvider.getValidityWords(RANDOM_POSITIONS);
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, RANDOM_POSITIONS);
        for (int position = 0; position < RANDOM_POSITIONS; position++) {
            double value = values.getAsDouble();
            if (validity.isPresent() && !Bitmap.isSet(validity.get(), 0, position)) {
                builder.appendNull();
            }
            else {
                DOUBLE.writeDouble(builder, value);
            }
        }
        return List.of(new Page(builder.build()));
    }
}
