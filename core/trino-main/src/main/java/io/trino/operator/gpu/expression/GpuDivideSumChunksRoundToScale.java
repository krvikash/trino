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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.DecimalType;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.DIV;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.MOD;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.UNSCALED_DECIMAL128;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.failOnNonPositiveDivisor;
import static io.trino.operator.gpu.expression.CudfUtils.halfUpCorrection;
import static io.trino.operator.gpu.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.util.Objects.requireNonNull;

/// Divides a sum by a positive INT64 divisor and rounds the quotient HALF_UP to an integer, which is
/// the unscaled value of the result type. The sum is given as the four 64-bit sums of the 32-bit chunks
/// [GpuExtractInt32Chunk] extracts from DECIMAL128 values, and equals
/// `chunk0 + chunk1 * 2^32 + chunk2 * 2^64 + chunk3 * 2^96`. Chunks 0..2 must not be negative.
///
/// Such a sum can exceed 128 bits even when the quotient is a valid decimal, e.g. for the average of
/// many 38-digit values. Unlike [GpuCombineDecimalStateSumsToDecimal128], which rejects such sums, this
/// expression divides without building the sum.
///
/// Raises an error for a divisor that is not positive, and for a quotient that does not fit the
/// precision of the result type.
public final class GpuDivideSumChunksRoundToScale
        extends GpuExpression
{
    private static final BigInteger TWO_TO_32 = BigInteger.ONE.shiftLeft(32);
    private static final BigInteger TWO_TO_64 = BigInteger.ONE.shiftLeft(64);

    private final List<GpuExpression> chunkSums;
    private final GpuExpression divisor;
    private final DecimalType resultType;

    public GpuDivideSumChunksRoundToScale(List<GpuExpression> chunkSums, GpuExpression divisor, DecimalType resultType)
    {
        checkArgument(chunkSums.size() == 4, "Expected 4 chunk sums, got %s", chunkSums.size());
        this.chunkSums = ImmutableList.copyOf(chunkSums);
        this.divisor = requireNonNull(divisor, "divisor is null");
        this.resultType = requireNonNull(resultType, "resultType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        // sum = high * 2^64 + low, where high = chunk3 * 2^32 + chunk2 and low = chunk1 * 2^32 + chunk0.
        // Both are below 2^96 in magnitude.
        try (ColumnVector chunk0 = chunkSums.get(0).evaluate(positionCount, inputColumns, bindings);
                ColumnVector chunk1 = chunkSums.get(1).evaluate(positionCount, inputColumns, bindings);
                ColumnVector chunk2 = chunkSums.get(2).evaluate(positionCount, inputColumns, bindings);
                ColumnVector chunk3 = chunkSums.get(3).evaluate(positionCount, inputColumns, bindings);
                ColumnVector divisorColumn = divisor.evaluate(positionCount, inputColumns, bindings);
                ColumnVector unscaledDivisor = divisorColumn.castTo(UNSCALED_DECIMAL128);
                Scalar twoTo64 = unscaled(TWO_TO_64);
                ClosingRef<ColumnVector> high = ClosingRef.own(combineChunkPair(chunk3, chunk2));
                ClosingRef<ColumnVector> low = ClosingRef.own(combineChunkPair(chunk1, chunk0))) {
            // Move any extra 2^64 from low into high, so low stays in [0, 2^64).
            try (ColumnVector carry = low.borrow().binaryOp(DIV, twoTo64, UNSCALED_DECIMAL128)) {
                high.replace(high.borrow().binaryOp(ADD, carry, UNSCALED_DECIMAL128));
            }
            low.replace(low.borrow().binaryOp(MOD, twoTo64, UNSCALED_DECIMAL128));
            failOnNonPositiveDivisor(high.borrow(), divisorColumn);

            // cuDF DIV truncates each part towards zero, which truncates the whole quotient only if every part has
            // the sign of the sum. low is never negative, so where high is negative, borrow 2^64 from high to give low the sign of the sum.
            try (Scalar zero = unscaled(BigInteger.ZERO);
                    Scalar one = unscaled(BigInteger.ONE);
                    ColumnVector negativeHigh = high.borrow().binaryOp(LESS, zero, DType.BOOL8)) {
                try (ColumnVector highAdjustment = negativeHigh.ifElse(one, zero)) {
                    high.replace(high.borrow().binaryOp(ADD, highAdjustment, UNSCALED_DECIMAL128));
                }
                try (ColumnVector lowAdjustment = negativeHigh.ifElse(twoTo64, zero)) {
                    low.replace(low.borrow().binaryOp(SUB, lowAdjustment, UNSCALED_DECIMAL128));
                }
            }

            // Divide in two steps so the intermediate values stay within DECIMAL128.
            // First divide high:
            //   high = highQuotient * c + highRemainder
            // Then divide the remaining part:
            //   highRemainder * 2^64 + low = lowQuotient * c + remainder
            // The full quotient is highQuotient * 2^64 + lowQuotient.
            try (ClosingRef<ColumnVector> highQuotient = ClosingRef.own(high.borrow().binaryOp(DIV, unscaledDivisor, UNSCALED_DECIMAL128));
                    ClosingRef<ColumnVector> highRemainder = ClosingRef.own(high.borrow().binaryOp(MOD, unscaledDivisor, UNSCALED_DECIMAL128))) {
                high.close();
                failOnHighQuotientOutOfRange(highQuotient.borrow());

                // |highRemainder| < unscaledDivisor and |low| <= 2^64, so |lowDividend| <= unscaledDivisor * 2^64.
                // With a 63-bit divisor, lowDividend fits DECIMAL128 and |lowQuotient| <= 2^64.
                try (ColumnVector lowDividend = shiftAdd(highRemainder.borrow(), twoTo64, low.borrow())) {
                    highRemainder.close();
                    low.close();
                    try (ColumnVector lowQuotient = lowDividend.binaryOp(DIV, unscaledDivisor, UNSCALED_DECIMAL128);
                            ColumnVector quotient = shiftAdd(highQuotient.borrow(), twoTo64, lowQuotient)) {
                        highQuotient.close();
                        return roundAndRescale(quotient, lowDividend, unscaledDivisor);
                    }
                }
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(ImmutableList.<GpuExpression>builder().addAll(chunkSums).add(divisor).build());
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return anyNullOnNullInputs(ImmutableList.<GpuExpression>builder().addAll(chunkSums).add(divisor).build());
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuDivideSumChunksRoundToScale other
                && chunkSums.equals(other.chunkSums)
                && divisor.equals(other.divisor)
                && resultType.equals(other.resultType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), chunkSums, divisor, resultType);
    }

    // higher * 2^32 + lower, below 2^96 in magnitude for any 64-bit chunk sums
    private static @Move ColumnVector combineChunkPair(@Borrow ColumnVector higher, @Borrow ColumnVector lower)
    {
        checkChunkSumType(higher);
        checkChunkSumType(lower);
        try (Scalar twoTo32 = unscaled(TWO_TO_32);
                ColumnVector unscaledHigher = higher.castTo(UNSCALED_DECIMAL128);
                ColumnVector unscaledLower = lower.castTo(UNSCALED_DECIMAL128)) {
            return shiftAdd(unscaledHigher, twoTo32, unscaledLower);
        }
    }

    private static void checkChunkSumType(@Borrow ColumnVector chunkSum)
    {
        // cuDF sums 32-bit chunks into 64 bits, as INT64 or, for the unsigned chunks, possibly UINT64
        DType.DTypeEnum typeId = chunkSum.getType().getTypeId();
        checkState(typeId == DType.DTypeEnum.INT64 || typeId == DType.DTypeEnum.UINT64, "Expected a 64-bit chunk sum, got %s", chunkSum.getType());
    }

    // value * multiplier + addend
    private static @Move ColumnVector shiftAdd(@Borrow ColumnView value, @Borrow Scalar multiplier, @Borrow ColumnView addend)
    {
        try (ColumnVector shifted = value.binaryOp(MUL, multiplier, UNSCALED_DECIMAL128)) {
            return shifted.binaryOp(ADD, addend, UNSCALED_DECIMAL128);
        }
    }

    // The remainder of the whole division is that of lowDividend, which also has the sign of the sum wherever
    // the remainder is not zero, so lowDividend stands in for the dividend when rounding.
    private @Move ColumnVector roundAndRescale(@Borrow ColumnVector truncatedQuotient, @Borrow ColumnView lowDividend, @Borrow ColumnVector divisor)
    {
        try (ColumnVector correction = halfUpCorrection(lowDividend, divisor);
                ColumnVector rounded = truncatedQuotient.binaryOp(ADD, correction, UNSCALED_DECIMAL128)) {
            failOnResultOutOfRange(rounded);
            try (ColumnView result = rounded.bitCastTo(DType.create(DType.DTypeEnum.DECIMAL128, -resultType.getScale()))) {
                return result.copyToColumnVector();
            }
        }
    }

    /// Bounds the high quotient before `highQuotient * 2^64` is computed, so that it cannot wrap. The low quotient
    /// shares the sign of the high quotient, so `|quotient| >= |highQuotient| * 2^64` and these bounds reject only
    /// quotients that are out of range anyway. Rounding moves the quotient away from zero, never back into range.
    private void failOnHighQuotientOutOfRange(@Borrow ColumnVector highQuotient)
    {
        BigInteger maxHighQuotient = maxUnscaled().shiftRight(64);
        try (Scalar min = unscaled(maxHighQuotient.negate());
                Scalar max = unscaled(maxHighQuotient)) {
            failOnOutOfRange(highQuotient, min, max);
        }
    }

    private void failOnResultOutOfRange(@Borrow ColumnVector result)
    {
        BigInteger maxUnscaled = maxUnscaled();
        try (Scalar min = unscaled(maxUnscaled.negate());
                Scalar max = unscaled(maxUnscaled)) {
            failOnOutOfRange(result, min, max);
        }
    }

    private static void failOnOutOfRange(@Borrow ColumnVector values, @Borrow Scalar min, @Borrow Scalar max)
    {
        try (ColumnVector belowMin = values.binaryOp(LESS, min, DType.BOOL8);
                ColumnVector aboveMax = values.binaryOp(GREATER, max, DType.BOOL8);
                ColumnVector outOfRange = belowMin.binaryOp(NULL_LOGICAL_OR, aboveMax, DType.BOOL8)) {
            if (anyTrue(outOfRange)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
        }
    }

    private BigInteger maxUnscaled()
    {
        return BigInteger.TEN.pow(resultType.getPrecision()).subtract(BigInteger.ONE);
    }

    private static @Move Scalar unscaled(BigInteger value)
    {
        return Scalar.fromDecimal(value, UNSCALED_DECIMAL128);
    }
}
