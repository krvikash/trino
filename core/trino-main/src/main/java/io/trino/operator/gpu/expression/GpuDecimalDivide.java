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
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.DIV;
import static ai.rapids.cudf.BinaryOp.EQUAL;
import static ai.rapids.cudf.BinaryOp.GREATER_EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.MOD;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.NOT_EQUAL;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static java.util.Objects.requireNonNull;

/// GPU counterpart of the decimal `/` operator ([io.trino.type.DecimalOperators]): divides the
/// dividend rescaled by `10^rescaleFactor` by the divisor, rounding HALF_UP away from zero.
public final class GpuDecimalDivide
        extends GpuExpression
{
    private static final DType UNSCALED = DType.create(DType.DTypeEnum.DECIMAL128, 0);

    private final GpuExpression dividend;
    private final GpuExpression divisor;
    private final int dividendPrecision;
    private final int divisorPrecision;
    private final int rescaleFactor;
    private final DType resultType;
    private final BigInteger rescale;
    private final BigInteger quotientLimit;
    private final boolean rescaledDividendFits;
    private final boolean estimatingBranchReachable;

    public GpuDecimalDivide(GpuExpression dividend, GpuExpression divisor, int dividendPrecision, int divisorPrecision, int rescaleFactor, DType resultType)
    {
        this.dividend = requireNonNull(dividend, "dividend is null");
        this.divisor = requireNonNull(divisor, "divisor is null");
        checkArgument(0 < dividendPrecision && dividendPrecision <= MAX_PRECISION, "Invalid dividendPrecision: %s", dividendPrecision);
        this.dividendPrecision = dividendPrecision;
        checkArgument(0 < divisorPrecision && divisorPrecision <= MAX_PRECISION, "Invalid divisorPrecision: %s", divisorPrecision);
        this.divisorPrecision = divisorPrecision;
        checkArgument(rescaleFactor >= 0, "Invalid rescaleFactor: %s", rescaleFactor);
        this.rescaledDividendFits = dividendPrecision + rescaleFactor <= MAX_PRECISION;
        // The long-division path rescales a remainder below 10^(2 * rescaleFactor), see narrowDivisorRemainder.
        // This gate, together with the quotient guard on that path, keeps every result below 10^38.
        checkArgument(rescaledDividendFits || 3 * rescaleFactor <= MAX_PRECISION, "Unsupported rescaleFactor: %s", rescaleFactor);
        this.rescaleFactor = rescaleFactor;
        // The estimating branch is only needed where a divisor both reaches 10^(2 * rescaleFactor) and
        // leaves a remainder too wide to rescale directly. No divisor of this precision reaches that
        // threshold unless it has more than 2 * rescaleFactor digits; and one that does is still harmless
        // while remainder * 10^rescaleFactor fits DECIMAL128, which it does below 10^38 because the
        // remainder is smaller than the divisor, and so below 10^divisorPrecision.
        this.estimatingBranchReachable = divisorPrecision > 2 * rescaleFactor
                && divisorPrecision + rescaleFactor > MAX_PRECISION;
        this.resultType = requireNonNull(resultType, "resultType is null");
        this.rescale = BigInteger.TEN.pow(rescaleFactor);
        this.quotientLimit = BigInteger.TEN.pow(MAX_PRECISION - rescaleFactor);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ClosingRef<ColumnVector> dividendMagnitude = ClosingRef.empty();
                ClosingRef<ColumnVector> divisorMagnitude = ClosingRef.empty();
                ClosingRef<ColumnVector> resultNegative = ClosingRef.empty()) {
            try (ColumnVector unscaledDividend = unscaledOperand(dividend, positionCount, inputColumns);
                    ColumnVector unscaledDivisor = unscaledOperand(divisor, positionCount, inputColumns)) {
                failOnZeroDivisor(unscaledDividend, unscaledDivisor);
                dividendMagnitude.set(unscaledDividend.abs());
                divisorMagnitude.set(nonZeroMagnitude(unscaledDivisor));
                resultNegative.set(negativeResult(unscaledDividend, unscaledDivisor));
            }
            // The operands are dead here: the division only needs the magnitudes, and the sign only the mask.
            try (ClosingOnce<ColumnVector> magnitude = ClosingOnce.own(roundedMagnitude(dividendMagnitude.take(), divisorMagnitude.borrow()))) {
                divisorMagnitude.close();
                return applySign(magnitude.borrow(), resultNegative.borrow());
            }
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuDecimalDivide other
                && dividend.equals(other.dividend)
                && divisor.equals(other.divisor)
                && dividendPrecision == other.dividendPrecision
                && divisorPrecision == other.divisorPrecision
                && rescaleFactor == other.rescaleFactor
                && resultType.equals(other.resultType);
        // derived: rescale
        // derived: quotientLimit
        // derived: rescaledDividendFits
        // derived: estimatingBranchReachable
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getClass(),
                dividend,
                divisor,
                dividendPrecision,
                divisorPrecision,
                rescaleFactor,
                resultType);
        // derived: rescale
        // derived: quotientLimit
        // derived: rescaledDividendFits
        // derived: estimatingBranchReachable
    }

    /// Evaluates an operand and reinterprets the decimal as its unscaled integer, widened to DECIMAL128.
    /// The operand's own column is released as soon as it has been reinterpreted.
    private static @Move ColumnVector unscaledOperand(GpuExpression operand, int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector column = operand.evaluate(positionCount, inputColumns);
                ColumnView unscaled = column.bitCastTo(DType.create(column.getType().getTypeId(), 0))) {
            if (unscaled.getType().equals(UNSCALED)) {
                return unscaled.copyToColumnVector();
            }
            return unscaled.castTo(UNSCALED);
        }
    }

    /// Whether the result is negative, that is whether the dividend and the divisor have different signs.
    private static @Move ColumnVector negativeResult(@Borrow ColumnVector unscaledDividend, @Borrow ColumnVector unscaledDivisor)
    {
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                ColumnVector dividendNegative = unscaledDividend.binaryOp(LESS, zero, DType.BOOL8);
                ColumnVector divisorNegative = unscaledDivisor.binaryOp(LESS, zero, DType.BOOL8)) {
            return dividendNegative.binaryOp(NOT_EQUAL, divisorNegative, DType.BOOL8);
        }
    }

    private static void failOnZeroDivisor(@Borrow ColumnVector unscaledDividend, @Borrow ColumnVector unscaledDivisor)
    {
        // A null dividend yields null regardless of the divisor, so a zero divisor is an error only
        // where the dividend is present.
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                ColumnVector zeroDivisor = unscaledDivisor.binaryOp(EQUAL, zero, DType.BOOL8);
                ColumnVector dividendPresent = unscaledDividend.isNotNull();
                ColumnVector zeroDivisorPresent = zeroDivisor.binaryOp(NULL_LOGICAL_AND, dividendPresent, DType.BOOL8)) {
            if (anyTrue(zeroDivisorPresent)) {
                throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
            }
        }
    }

    /// `round(|dividend| * 10^rescaleFactor / |divisor|)`. Consumes the dividend magnitude, which is dead
    /// as soon as it has been rescaled, or divided into a quotient and a remainder.
    private @Move ColumnVector roundedMagnitude(@Move ColumnVector dividendMagnitude, @Borrow ColumnVector divisorMagnitude)
    {
        try (ClosingOnce<ColumnVector> numerator = ClosingOnce.own(dividendMagnitude);
                ClosingRef<ColumnVector> quotient = ClosingRef.empty();
                ClosingRef<ColumnVector> remainder = ClosingRef.empty()) {
            if (rescaledDividendFits) {
                if (rescaleFactor == 0) {
                    return roundedQuotient(numerator.borrow(), divisorMagnitude);
                }
                try (Scalar rescaleScalar = unscaledScalar(rescale);
                        ClosingOnce<ColumnVector> rescaledDividend = ClosingOnce.own(numerator.borrow().binaryOp(MUL, rescaleScalar, UNSCALED))) {
                    numerator.close();
                    return roundedQuotient(rescaledDividend.borrow(), divisorMagnitude);
                }
            }

            // The rescaled dividend needs more than 38 digits, so divide first and rescale the remainder instead:
            //   |dividend| * 10^r / |divisor| = (|dividend| / |divisor|) * 10^r + (|dividend| mod |divisor|) * 10^r / |divisor|
            quotient.set(numerator.borrow().binaryOp(DIV, divisorMagnitude, UNSCALED));
            remainder.set(numerator.borrow().binaryOp(MOD, divisorMagnitude, UNSCALED));
            numerator.close();
            // Guard before the multiplication below, which would silently wrap around
            failOnOverflow(quotient.borrow(), quotientLimit);
            try (Scalar rescaleScalar = unscaledScalar(rescale);
                    ClosingOnce<ColumnVector> rescaledQuotient = ClosingOnce.own(quotient.borrow().binaryOp(MUL, rescaleScalar, UNSCALED))) {
                quotient.close();
                try (ClosingOnce<ColumnVector> fraction = ClosingOnce.own(roundedRescaledRemainder(remainder.borrow(), divisorMagnitude))) {
                    remainder.close();
                    return rescaledQuotient.borrow().binaryOp(ADD, fraction.borrow(), UNSCALED);
                }
            }
        }
    }

    /// `|divisor|`, with zero replaced by one. A zero divisor gets past [#failOnZeroDivisor] where
    /// the dividend is null, and the result for those rows is null no matter what is divided by.
    private static @Move ColumnVector nonZeroMagnitude(@Borrow ColumnVector unscaledDivisor)
    {
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                Scalar one = unscaledScalar(BigInteger.ONE);
                ColumnVector magnitude = unscaledDivisor.abs();
                ColumnVector isZero = magnitude.binaryOp(EQUAL, zero, DType.BOOL8)) {
            return isZero.ifElse(one, magnitude);
        }
    }

    private static @Move ColumnVector roundedQuotient(@Borrow ColumnVector dividendMagnitude, @Borrow ColumnVector divisorMagnitude)
    {
        try (ClosingOnce<ColumnVector> quotient = ClosingOnce.own(dividendMagnitude.binaryOp(DIV, divisorMagnitude, UNSCALED));
                ClosingOnce<ColumnVector> remainder = ClosingOnce.own(dividendMagnitude.binaryOp(MOD, divisorMagnitude, UNSCALED));
                ClosingOnce<ColumnVector> bump = ClosingOnce.own(roundingBump(remainder.borrow(), divisorMagnitude))) {
            remainder.close();
            return quotient.borrow().binaryOp(ADD, bump.borrow(), UNSCALED);
        }
    }

    /// `round(remainder * 10^rescaleFactor / divisor)` for `remainder < divisor`, which is what the
    /// long-division path needs and what forming `remainder * 10^rescaleFactor` cannot deliver: the
    /// remainder can be as wide as the divisor, so the product may need up to 38 + rescaleFactor digits.
    private @Move ColumnVector roundedRescaledRemainder(@Borrow ColumnVector remainder, @Borrow ColumnVector divisorMagnitude)
    {
        if (!estimatingBranchReachable) {
            return narrowDivisorRemainder(remainder, divisorMagnitude);
        }
        try (Scalar rescaleSquaredScalar = unscaledScalar(BigInteger.TEN.pow(2 * rescaleFactor));
                ColumnVector narrowDivisor = divisorMagnitude.binaryOp(LESS, rescaleSquaredScalar, DType.BOOL8);
                // The wide branch holds more intermediates than the narrow one, so evaluate it while
                // nothing else from this method is live yet
                ClosingOnce<ColumnVector> wide = ClosingOnce.own(wideDivisorRemainder(remainder, divisorMagnitude, narrowDivisor));
                ClosingOnce<ColumnVector> narrow = ClosingOnce.own(maskedNarrowDivisorRemainder(remainder, divisorMagnitude, narrowDivisor))) {
            return narrowDivisor.ifElse(narrow.borrow(), wide.borrow());
        }
    }

    /// [#roundedRescaledRemainder] for a divisor below `10^(2 * rescaleFactor)`. The remainder is
    /// smaller still, so rescaling it stays within 3 * rescaleFactor digits and the division is direct.
    private @Move ColumnVector narrowDivisorRemainder(@Borrow ColumnVector remainder, @Borrow ColumnVector divisorMagnitude)
    {
        try (Scalar rescaleScalar = unscaledScalar(rescale);
                ClosingOnce<ColumnVector> rescaled = ClosingOnce.own(remainder.binaryOp(MUL, rescaleScalar, UNSCALED))) {
            return roundedQuotient(rescaled.borrow(), divisorMagnitude);
        }
    }

    /// [#narrowDivisorRemainder] with the rows the estimating branch handles zeroed out first, so that
    /// rescaling them cannot wrap around.
    private @Move ColumnVector maskedNarrowDivisorRemainder(@Borrow ColumnVector remainder, @Borrow ColumnVector divisorMagnitude, @Borrow ColumnVector narrowDivisor)
    {
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                Scalar rescaleScalar = unscaledScalar(rescale);
                ClosingOnce<ColumnVector> selected = ClosingOnce.own(narrowDivisor.ifElse(remainder, zero));
                ClosingOnce<ColumnVector> rescaled = ClosingOnce.own(selected.borrow().binaryOp(MUL, rescaleScalar, UNSCALED))) {
            selected.close();
            return roundedQuotient(rescaled.borrow(), divisorMagnitude);
        }
    }

    /// [#roundedRescaledRemainder] for a divisor of at least `10^(2 * rescaleFactor)`.
    private @Move ColumnVector wideDivisorRemainder(@Borrow ColumnVector remainder, @Borrow ColumnVector divisorMagnitude, @Borrow ColumnVector narrowDivisor)
    {
        try (ClosingRef<ColumnVector> estimate = ClosingRef.empty();
                ClosingRef<ColumnVector> estimateRemainder = ClosingRef.empty()) {
            try (Scalar one = unscaledScalar(BigInteger.ONE);
                    Scalar rescaleScalar = unscaledScalar(rescale);
                    // divisor = divisorHigh * 10^rescaleFactor + divisorLow. Keep the division defined on
                    // rows handled by the other branch, where divisorHigh may be zero.
                    ClosingOnce<ColumnVector> divisorHigh = ClosingOnce.own(divisorMagnitude.binaryOp(DIV, rescaleScalar, UNSCALED));
                    ColumnVector safeDivisorHigh = narrowDivisor.ifElse(one, divisorHigh.borrow())) {
                divisorHigh.close();
                // Dropping divisorLow overestimates the quotient by less than divisorLow / divisorHigh.
                // Here divisorHigh is at least 10^rescaleFactor and divisorLow is below it, so the
                // estimate is either exact or one too high.
                estimate.set(remainder.binaryOp(DIV, safeDivisorHigh, UNSCALED));
                estimateRemainder.set(estimateRemainder(remainder, divisorMagnitude, safeDivisorHigh, estimate.borrow()));
            }
            try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                    // A negative remainder means the estimate was one too high
                    ColumnVector overestimated = estimateRemainder.borrow().binaryOp(LESS, zero, DType.BOOL8);
                    ClosingOnce<ColumnVector> bump = ClosingOnce.own(correctedRoundingBump(estimateRemainder.borrow(), divisorMagnitude, overestimated));
                    ClosingOnce<ColumnVector> correctedQuotient = ClosingOnce.own(correctedQuotient(estimate.borrow(), overestimated))) {
                estimate.close();
                estimateRemainder.close();
                return correctedQuotient.borrow().binaryOp(ADD, bump.borrow(), UNSCALED);
            }
        }
    }

    /// `remainder * 10^rescaleFactor - estimate * divisor`, regrouped as
    /// `(remainder mod divisorHigh) * 10^rescaleFactor - estimate * divisorLow` so that both products stay below 10^38.
    private @Move ColumnVector estimateRemainder(
            @Borrow ColumnVector remainder,
            @Borrow ColumnVector divisorMagnitude,
            @Borrow ColumnVector safeDivisorHigh,
            @Borrow ColumnVector estimate)
    {
        try (ClosingOnce<ColumnVector> restRescaled = ClosingOnce.own(restRescaled(remainder, safeDivisorHigh));
                ClosingOnce<ColumnVector> estimateTimesLow = ClosingOnce.own(estimateTimesLow(divisorMagnitude, estimate))) {
            return restRescaled.borrow().binaryOp(SUB, estimateTimesLow.borrow(), UNSCALED);
        }
    }

    private @Move ColumnVector restRescaled(@Borrow ColumnVector remainder, @Borrow ColumnVector safeDivisorHigh)
    {
        try (Scalar rescaleScalar = unscaledScalar(rescale);
                ColumnVector rest = remainder.binaryOp(MOD, safeDivisorHigh, UNSCALED)) {
            return rest.binaryOp(MUL, rescaleScalar, UNSCALED);
        }
    }

    private @Move ColumnVector estimateTimesLow(@Borrow ColumnVector divisorMagnitude, @Borrow ColumnVector estimate)
    {
        try (Scalar rescaleScalar = unscaledScalar(rescale);
                ColumnVector divisorLow = divisorMagnitude.binaryOp(MOD, rescaleScalar, UNSCALED)) {
            return estimate.binaryOp(MUL, divisorLow, UNSCALED);
        }
    }

    /// [#roundingBump] on the exact remainder, which is the estimate's remainder plus the divisor where
    /// the estimate was one too high.
    private static @Move ColumnVector correctedRoundingBump(@Borrow ColumnVector estimateRemainder, @Borrow ColumnVector divisorMagnitude, @Borrow ColumnVector overestimated)
    {
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                ClosingOnce<ColumnVector> correction = ClosingOnce.own(overestimated.ifElse(divisorMagnitude, zero));
                ClosingOnce<ColumnVector> correctedRemainder = ClosingOnce.own(estimateRemainder.binaryOp(ADD, correction.borrow(), UNSCALED))) {
            correction.close();
            return roundingBump(correctedRemainder.borrow(), divisorMagnitude);
        }
    }

    /// The estimate, less one where it was one too high.
    private static @Move ColumnVector correctedQuotient(@Borrow ColumnVector estimate, @Borrow ColumnVector overestimated)
    {
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                Scalar minusOne = unscaledScalar(BigInteger.ONE.negate());
                ColumnVector correction = overestimated.ifElse(minusOne, zero)) {
            return estimate.binaryOp(ADD, correction, UNSCALED);
        }
    }

    /// 1 where `2 * remainder >= divisor`, that is where HALF_UP rounds away from zero, and 0 elsewhere.
    private static @Move ColumnVector roundingBump(@Borrow ColumnVector remainder, @Borrow ColumnVector divisorMagnitude)
    {
        try (Scalar zero = unscaledScalar(BigInteger.ZERO);
                Scalar one = unscaledScalar(BigInteger.ONE);
                // Compared as remainder >= divisor - remainder, because 2 * remainder can exceed DECIMAL128
                ClosingOnce<ColumnVector> complement = ClosingOnce.own(divisorMagnitude.binaryOp(SUB, remainder, UNSCALED));
                ColumnVector roundUp = remainder.binaryOp(GREATER_EQUAL, complement.borrow(), DType.BOOL8)) {
            complement.close();
            return roundUp.ifElse(one, zero);
        }
    }

    private static void failOnOverflow(@Borrow ColumnVector magnitude, BigInteger limit)
    {
        try (Scalar limitScalar = unscaledScalar(limit);
                ColumnVector tooLarge = magnitude.binaryOp(GREATER_EQUAL, limitScalar, DType.BOOL8)) {
            if (anyTrue(tooLarge)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
        }
    }

    private @Move ColumnVector applySign(@Borrow ColumnVector magnitude, @Borrow ColumnVector resultNegative)
    {
        try (Scalar minusOne = unscaledScalar(BigInteger.ONE.negate());
                ClosingOnce<ColumnVector> negated = ClosingOnce.own(magnitude.binaryOp(MUL, minusOne, UNSCALED));
                ClosingOnce<ColumnVector> signed = ClosingOnce.own(resultNegative.ifElse(negated.borrow(), magnitude))) {
            negated.close();
            try (ColumnView result = signed.borrow().bitCastTo(DType.create(DType.DTypeEnum.DECIMAL128, resultType.getScale()))) {
                if (result.getType().equals(resultType)) {
                    return result.copyToColumnVector();
                }
                return result.castTo(resultType);
            }
        }
    }

    private static @Move Scalar unscaledScalar(BigInteger value)
    {
        return Scalar.fromDecimal(value, UNSCALED);
    }
}
