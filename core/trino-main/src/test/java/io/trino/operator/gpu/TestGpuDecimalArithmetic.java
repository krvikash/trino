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
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Verify.verify;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.decimalBlock;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuDecimalArithmetic
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testShortDecimalAdd(NullsProvider nullsProvider)
    {
        testShortDecimalAdd(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalAdd(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalAdd(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalAdd(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (integral + scale + 1 > 18)
        testShortDecimalAdd(createDecimalType(18, 0), createDecimalType(18, 18), nullsProvider);
        testShortDecimalAdd(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalAdd(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.ADD, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testShortDecimalSubtract(NullsProvider nullsProvider)
    {
        testShortDecimalSubtract(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalSubtract(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalSubtract(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalSubtract(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (integral + scale + 1 > 18)
        testShortDecimalSubtract(createDecimalType(18, 0), createDecimalType(18, 18), nullsProvider);
        testShortDecimalSubtract(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalSubtract(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.SUBTRACT, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testShortDecimalMultiply(NullsProvider nullsProvider)
    {
        testShortDecimalMultiply(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalMultiply(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalMultiply(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalMultiply(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (p1 + p2 > 18)
        testShortDecimalMultiply(createDecimalType(10, 2), createDecimalType(10, 2), nullsProvider);
        testShortDecimalMultiply(createDecimalType(13, 0), createDecimalType(13, 0), nullsProvider);
        testShortDecimalMultiply(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalMultiply(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.MULTIPLY, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLongDecimalAdd(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(19, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLongDecimalSubtract(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(19, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLongDecimalMultiply(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(20, 2), createDecimalType(18, 2), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(19, 0), createDecimalType(19, 0), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testShortDecimalDivide(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testDecimalDivide(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testDecimalDivide(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testDecimalDivide(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        testDecimalDivide(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalDivide(createDecimalType(18, 18), createDecimalType(18, 0), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLongDecimalDivide(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(38, 4), createDecimalType(38, 4), nullsProvider);
        testDecimalDivide(createDecimalType(38, 0), createDecimalType(38, 0), nullsProvider);
        testDecimalDivide(createDecimalType(38, 38), createDecimalType(38, 0), nullsProvider);
        testDecimalDivide(createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalDivide(createDecimalType(19, 0), createDecimalType(19, 0), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMixedWidthDecimalDivide(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
        testDecimalDivide(createDecimalType(13, 2), createDecimalType(25, 4), nullsProvider);
        testDecimalDivide(createDecimalType(38, 6), createDecimalType(5, 2), nullsProvider);
        testDecimalDivide(createDecimalType(5, 2), createDecimalType(38, 6), nullsProvider);
    }

    /// The largest rescale factor the long division supports is 12, where `3 * rescaleFactor <= 38` is
    /// tightest. These four sit at exactly that factor: the first still rescales the dividend up front;
    /// the second divides first but never reaches the estimating branch, because no divisor in
    /// `decimal(6,6)` reaches `10^24`; the third has `divisorPrecision + rescaleFactor` one above 38, the
    /// point at which a rescaled remainder stops fitting DECIMAL128 and the estimating branch becomes
    /// necessary; and the fourth is well past it.
    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDecimalDivideAtLargestRescaleFactor(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(26, 0), createDecimalType(6, 6), nullsProvider);
        testDecimalDivide(createDecimalType(27, 0), createDecimalType(6, 6), nullsProvider);
        testDecimalDivide(createDecimalType(27, 0), createDecimalType(27, 6), nullsProvider);
        testDecimalDivide(createDecimalType(27, 0), createDecimalType(30, 6), nullsProvider);
    }

    @Test
    void testDecimalDivideByZero()
    {
        DecimalType type = createDecimalType(38, 4);
        Page input = new Page(decimalBlock(type, List.of(BigInteger.valueOf(12345))), decimalBlock(type, List.of(BigInteger.ZERO)));
        assertGpuMatchesCpu(List.of(input), List.of(type, type), divide(type, type));

        DecimalType shortType = createDecimalType(9, 2);
        Page shortInput = new Page(decimalBlock(shortType, List.of(BigInteger.valueOf(12345))), decimalBlock(shortType, List.of(BigInteger.ZERO)));
        assertGpuMatchesCpu(List.of(shortInput), List.of(shortType, shortType), divide(shortType, shortType));
    }

    @Test
    void testDecimalDivideNullOperands()
    {
        // A null dividend yields null even for a zero divisor, so the division by zero must not be reported.
        // A null divisor yields null too, which relies on cuDF selecting the false branch of an ifElse
        // wherever the predicate is null: the null divisor's zero test is null, so nonZeroMagnitude keeps
        // the null magnitude instead of substituting one.
        DecimalType type = createDecimalType(38, 4);
        BlockBuilder dividends = type.createBlockBuilder(null, 6);
        dividends.appendNull();
        dividends.appendNull();
        dividends.appendNull();
        type.writeObject(dividends, Int128.valueOf(700));
        type.writeObject(dividends, Int128.valueOf(700));
        dividends.appendNull();
        BlockBuilder divisors = type.createBlockBuilder(null, 6);
        type.writeObject(divisors, Int128.valueOf(0));
        type.writeObject(divisors, Int128.valueOf(-3));
        type.writeObject(divisors, Int128.valueOf(1));
        type.writeObject(divisors, Int128.valueOf(4));
        divisors.appendNull();
        divisors.appendNull();
        Page input = new Page(dividends.build(), divisors.build());
        assertGpuMatchesCpu(List.of(input), List.of(type, type), divide(type, type));
    }

    @Test
    void testDecimalDivideOverflow()
    {
        DecimalType type = createDecimalType(38, 0);
        // decimal(38,0) / decimal(38,0) is decimal(38,6) and takes the long-division path, so dividing by
        // one leaves a quotient of 10^38 - 1 against a limit of 10^(38 - 6): this trips the guard on the
        // quotient, before it is rescaled, rather than any check on the finished result.
        Page input = new Page(
                decimalBlock(type, List.of(BigInteger.TEN.pow(38).subtract(BigInteger.ONE))),
                decimalBlock(type, List.of(BigInteger.ONE)));
        assertGpuMatchesCpu(List.of(input), List.of(type, type), divide(type, type));
    }

    @Test
    void testDecimalDivideUnsupportedRescale()
    {
        // decimal(30,0) / decimal(20,10) is decimal(38,6), rescaling the dividend by 10^16; neither the
        // rescaled dividend nor the long-division intermediates fit in DECIMAL128
        DecimalType dividendType = createDecimalType(30, 0);
        DecimalType divisorType = createDecimalType(20, 10);
        assertThat(compileExpression(divide(dividendType, divisorType), layoutFor(List.of(dividendType, divisorType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    private void testDecimalDivide(DecimalType dividendType, DecimalType divisorType, NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(dividendType, divisorType);
        Expression expression = divide(dividendType, divisorType);
        DecimalType resultType = (DecimalType) expression.type();
        int rescaleFactor = resultType.getScale() - dividendType.getScale() + divisorType.getScale();
        assertGpuMatchesCpu(divideInputs(dividendType, divisorType, rescaleFactor, nullsProvider), inputTypes, expression);
    }

    private static Expression divide(DecimalType dividendType, DecimalType divisorType)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(dividendType, divisorType)),
                ImmutableList.of(field(0, dividendType), field(1, divisorType)));
    }

    /// Dividend and divisor pairs covering both signs, remainders exactly on the HALF_UP boundary and
    /// their neighbours, and divisors on both sides of `10^(2 * rescaleFactor)`, where the GPU switches
    /// between rescaling the remainder directly and estimating the quotient. Dividends are capped so
    /// that no row overflows the result; overflow is covered separately.
    private static List<Page> divideInputs(DecimalType dividendType, DecimalType divisorType, int rescaleFactor, NullsProvider nullsProvider)
    {
        BigInteger dividendBound = BigInteger.TEN.pow(dividendType.getPrecision());
        BigInteger divisorBound = BigInteger.TEN.pow(divisorType.getPrecision());
        BigInteger rescale = BigInteger.TEN.pow(rescaleFactor);
        BigInteger resultBound = BigInteger.TEN.pow(38);
        List<BigInteger> dividends = new ArrayList<>();
        List<BigInteger> divisors = new ArrayList<>();

        // A divisor of 2^(rescaleFactor + 1) * m leaves the rescaled dividend m * 10^rescaleFactor at exactly
        // half of it: 10^rescaleFactor is 2^rescaleFactor * 5^rescaleFactor and 5^rescaleFactor is odd, so the
        // remainder is 2^rescaleFactor * m. The dividend's neighbours m - 1 and m + 1 are not ties.
        int ties = 0;
        for (BigInteger multiplier : tieMultipliers(rescaleFactor, dividendBound, divisorBound)) {
            BigInteger divisor = BigInteger.TWO.pow(rescaleFactor + 1).multiply(multiplier);
            verify(multiplier.multiply(rescale).mod(divisor).equals(divisor.divide(BigInteger.TWO)),
                    "Not a rounding tie: %s / %s",
                    multiplier,
                    divisor);
            ties++;
            for (long offset : new long[] {-1, 0, 1}) {
                for (int dividendSign : new int[] {1, -1}) {
                    for (int divisorSign : new int[] {1, -1}) {
                        dividends.add(multiplier.add(BigInteger.valueOf(offset)).multiply(BigInteger.valueOf(dividendSign)));
                        divisors.add(divisor.multiply(BigInteger.valueOf(divisorSign)));
                    }
                }
            }
        }
        verify(ties > 0, "No rounding tie fits %s / %s", dividendType, divisorType);

        Random random = new Random(29);
        BigInteger branchBoundary = rescale.multiply(rescale);
        // Just above the branch boundary, the divisor's low part is as large as it can be relative to its
        // high part, which is where the estimated quotient most often comes out one too high. Sample this
        // divisor repeatedly, since a single dividend would hit the correction only about half the time.
        BigInteger correctionProne = branchBoundary.add(rescale).subtract(BigInteger.ONE);
        if (correctionProne.compareTo(divisorBound) < 0) {
            for (int sample = 0; sample < 8; sample++) {
                addDividePair(dividends, divisors, correctionProne, dividendBound, resultBound, rescale, random);
            }
        }
        for (BigInteger branchPoint : List.of(branchBoundary, BigInteger.ONE, divisorBound.subtract(BigInteger.ONE))) {
            for (long offset : new long[] {-1, 0, 1}) {
                BigInteger divisor = branchPoint.add(BigInteger.valueOf(offset));
                if (divisor.signum() <= 0 || divisor.compareTo(divisorBound) >= 0) {
                    continue;
                }
                addDividePair(dividends, divisors, divisor, dividendBound, resultBound, rescale, random);
            }
        }
        while (dividends.size() < 256) {
            BigInteger divisor = randomBelow(random, divisorBound.subtract(BigInteger.ONE)).add(BigInteger.ONE);
            addDividePair(dividends, divisors, divisor, dividendBound, resultBound, rescale, random);
        }

        return List.of(new Page(
                decimalBlock(dividendType, dividends, nullsProvider),
                decimalBlock(divisorType, divisors, nullsProvider)));
    }

    /// Multipliers `m` whose tie divisor `2^(rescaleFactor + 1) * m` and tie dividend `m` both fit the
    /// tested precisions. The last candidate is the smallest `m` that lifts the divisor to
    /// `10^(2 * rescaleFactor)`, which is what puts a tie on the estimating branch of the long division.
    private static List<BigInteger> tieMultipliers(int rescaleFactor, BigInteger dividendBound, BigInteger divisorBound)
    {
        BigInteger step = BigInteger.TWO.pow(rescaleFactor + 1);
        BigInteger estimatingBranch = BigInteger.TEN.pow(2 * rescaleFactor).add(step).subtract(BigInteger.ONE).divide(step);
        ImmutableList.Builder<BigInteger> multipliers = ImmutableList.builder();
        for (BigInteger multiplier : List.of(BigInteger.ONE, BigInteger.valueOf(7), BigInteger.valueOf(9999), estimatingBranch)) {
            if (step.multiply(multiplier).compareTo(divisorBound) < 0 && multiplier.add(BigInteger.ONE).compareTo(dividendBound) < 0) {
                multipliers.add(multiplier);
            }
        }
        return multipliers.build();
    }

    private static void addDividePair(
            List<BigInteger> dividends,
            List<BigInteger> divisors,
            BigInteger divisor,
            BigInteger dividendBound,
            BigInteger resultBound,
            BigInteger rescale,
            Random random)
    {
        BigInteger cap = resultBound.subtract(BigInteger.ONE).multiply(divisor).divide(rescale).add(BigInteger.ONE).min(dividendBound);
        // What stresses the rescaling in the direct branch is a large remainder, not a large dividend: a
        // dividend one below the divisor leaves the largest remainder that divisor admits. The largest
        // permitted dividend is also worth a row, though against the largest divisor it divides exactly
        // and leaves nothing. Drawing only at random misses both: for decimal(27,0) / decimal(30,6) the
        // remainder has to reach 88 of its 90 available bits before the rescaled remainder stops fitting.
        BigInteger largest = cap.subtract(BigInteger.ONE).max(BigInteger.ZERO);
        for (BigInteger magnitude : List.of(randomBelow(random, cap), largest, divisor.subtract(BigInteger.ONE).min(largest).max(BigInteger.ZERO))) {
            for (int dividendSign : new int[] {1, -1}) {
                for (int divisorSign : new int[] {1, -1}) {
                    dividends.add(magnitude.multiply(BigInteger.valueOf(dividendSign)));
                    divisors.add(divisor.multiply(BigInteger.valueOf(divisorSign)));
                }
            }
        }
    }

    private static BigInteger randomBelow(Random random, BigInteger boundExclusive)
    {
        if (boundExclusive.compareTo(BigInteger.ONE) <= 0) {
            return BigInteger.ZERO;
        }
        // Draw the bit length first so that magnitudes spread over the whole range instead of clustering at the top
        return new BigInteger(1 + random.nextInt(boundExclusive.bitLength()), random).mod(boundExclusive);
    }

    private void testDecimalBinaryOp(OperatorType operatorType, DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(leftType, rightType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(leftType, positionsCount, nullsProvider),
                createBlock(rightType, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(operatorType, List.of(leftType, rightType)),
                ImmutableList.of(field(channelA, leftType), field(channelB, rightType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }
}
