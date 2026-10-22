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
import io.trino.operator.torin.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
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
import java.util.function.IntSupplier;

import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.torin.GpuTestUtils.executeWithGpu;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.torin.expression.Fallibility.INFALLIBLE;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.RealOperators.castToVarchar;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuRealToVarcharCast
{
    private static final int RANDOM_POSITIONS = 1_000_000;

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
                (double) Float.NaN,
                (double) Float.POSITIVE_INFINITY,
                (double) Float.NEGATIVE_INFINITY,
                null,
                (double) Float.MIN_VALUE,
                (double) -Float.MIN_VALUE,
                (double) Math.nextDown(Float.MIN_NORMAL),
                (double) Float.MIN_NORMAL,
                (double) Float.MAX_VALUE,
                (double) -Float.MAX_VALUE);
        for (Type targetType : List.of(VARCHAR, createVarcharType(13))) {
            assertGpuMatchesCpu(realPages(values), List.of(REAL), cast(targetType));
        }
    }

    @Test
    void testSubnormalsNeedingTwoDigits()
    {
        ImmutableList.Builder<Double> values = ImmutableList.builder();
        for (int bits = 1; bits < 128; bits++) {
            values.add((double) Float.intBitsToFloat(bits)).add((double) -Float.intBitsToFloat(bits));
        }
        assertGpuMatchesCpu(realPages(values.build()), List.of(REAL), cast(VARCHAR));
    }

    @Test
    void testDecimalValues()
    {
        List<Double> values = Arrays.asList(
                1.0,
                -1.0,
                (double) 0.1f,
                (double) -0.1f,
                (double) (1.0f / 3),
                (double) 4.35f,
                (double) 12.34f,
                (double) 1234567.0f,
                (double) 12345678.0f,
                (double) 123456789.0f,
                (double) 0.001f,
                (double) 1e7f,
                (double) 1e-5f,
                (double) 3.1415927f);
        for (Type targetType : List.of(VARCHAR, createVarcharType(13), createVarcharType(100))) {
            assertGpuMatchesCpu(realPages(values), List.of(REAL), cast(targetType));
        }
    }

    @Test
    void testRoundingToSevenDigits()
    {
        // Shortest digits ending in 5 at the 8th digit, where the widened double decides the rounding direction,
        // inside and around [2^23, 2^63), where the CPU rounds an exact tie up on JDK 25, e.g. 2.199029E12 for
        // 2.1990285E12, but half even on JDK 26+ (https://bugs.openjdk.org/browse/JDK-8362448), e.g. 2.199028E12
        List<Double> values = Arrays.asList(
                (double) 2.1990285e12f,
                (double) 1.2345675e7f,
                (double) 8388608.5f,
                (double) 3.8518645e-34f,
                (double) 2.2250735e8f,
                (double) 9.9999995e9f,
                (double) 1.0000005e19f,
                (double) 9.2233715e18f,
                (double) 9.223373e18f,
                (double) 1.8446743e19f,
                (double) 9999999.5f,
                (double) 0.99999995f);
        assertGpuMatchesCpu(realPages(values), List.of(REAL), cast(VARCHAR));
    }

    @Test
    void testPowersOfTwoAndNeighbors()
    {
        ImmutableList.Builder<Double> values = ImmutableList.builder();
        for (int power = -149; power <= 127; power++) {
            float powerOfTwo = Math.scalb(1.0f, power);
            for (float value : List.of(Math.nextDown(powerOfTwo), powerOfTwo, Math.nextUp(powerOfTwo))) {
                if (value > 0 && Float.isFinite(value)) {
                    values.add((double) value).add((double) -value);
                }
            }
        }
        assertGpuMatchesCpu(realPages(values.build()), List.of(REAL), cast(VARCHAR));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testRandomBitPatterns(NullsProvider nullsProvider)
    {
        Random random = new Random(1);
        List<Page> pages = randomRealPages(nullsProvider, random::nextInt);
        assertGpuMatchesCpu(pages, List.of(REAL), cast(VARCHAR));
    }

    @Test
    void testRandomMagnitudesAroundHalfUpRange()
    {
        Random random = new Random(2);
        List<Page> pages = randomRealPages(
                NullsProvider.NO_NULLS,
                () -> floatToRawIntBits(Math.scalb(1 + random.nextFloat(), 20 + random.nextInt(47)) * (random.nextBoolean() ? 1 : -1)));
        assertGpuMatchesCpu(pages, List.of(REAL), cast(VARCHAR));
    }

    @Test
    void testBoundedVarchar()
    {
        List<Double> fitting = Arrays.asList(null, 0.0, -0.0, 1.0, -1.5, (double) 0.1f, 12.5, (double) Float.NaN, (double) Float.POSITIVE_INFINITY, null);
        for (int bound = 8; bound <= 12; bound++) {
            assertGpuMatchesCpu(realPages(fitting), List.of(REAL), cast(createVarcharType(bound)));
        }
        for (float value : List.of(Float.NEGATIVE_INFINITY, -1.5f, 0.1f, 1.0f / 3, -Float.MIN_VALUE, Float.MAX_VALUE, -Float.MIN_NORMAL, 2.1990285e12f)) {
            int length = castToVarchar(Integer.MAX_VALUE, floatToRawIntBits(value)).length();
            assertGpuMatchesCpu(realPages(List.of((double) value)), List.of(REAL), cast(createVarcharType(length)));
            assertGpuMatchesCpu(realPages(List.of((double) value)), List.of(REAL), cast(createVarcharType(length - 1)));
        }
    }

    @Test
    void testBoundedVarcharFailureMessage()
    {
        // The second non-NULL row is the first one too long for varchar(5)
        List<Double> values = Arrays.asList(null, 1.0, (double) (1.0f / 3), -2.5e-30, null);
        Expression expression = cast(createVarcharType(5));
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(REAL)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertThat(gpuExpression.expression().fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        TrinoException cpuFailure = catchThrowableOfType(TrinoException.class, () -> castToVarchar(5, floatToRawIntBits(1.0f / 3)));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(realPages(values), List.of(REAL), expression, gpuExpression))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage(cpuFailure.getMessage());
    }

    @Test
    void testUnboundedOrLongEnoughVarcharIsNotChecked()
    {
        for (Type targetType : List.of(VARCHAR, createVarcharType(13), createVarcharType(1000))) {
            CompiledExpression gpuExpression = compileExpression(cast(targetType), layoutFor(List.of(REAL)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
            assertThat(gpuExpression.expression().fallibility()).describedAs("cast to %s", targetType).isEqualTo(INFALLIBLE);
        }
        CompiledExpression bounded = compileExpression(cast(createVarcharType(12)), layoutFor(List.of(REAL)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertThat(bounded.expression().fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
    }

    private static Expression cast(Type targetType)
    {
        return new Cast(field(0, REAL), targetType);
    }

    private static List<Page> realPages(List<Double> values)
    {
        return List.of(new Page(createFloatingPointBlock(REAL, values)));
    }

    private static List<Page> randomRealPages(NullsProvider nullsProvider, IntSupplier bits)
    {
        Optional<long[]> validity = nullsProvider.getValidityWords(RANDOM_POSITIONS);
        BlockBuilder builder = REAL.createBlockBuilder(null, RANDOM_POSITIONS);
        for (int position = 0; position < RANDOM_POSITIONS; position++) {
            int value = bits.getAsInt();
            if (validity.isPresent() && !Bitmap.isSet(validity.get(), 0, position)) {
                builder.appendNull();
            }
            else {
                REAL.writeFloat(builder, Float.intBitsToFloat(value));
            }
        }
        return List.of(new Page(builder.build()));
    }
}
