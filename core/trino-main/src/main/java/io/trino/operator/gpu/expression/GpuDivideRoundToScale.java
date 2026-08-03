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
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.DIV;
import static ai.rapids.cudf.BinaryOp.GREATER_EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.MOD;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/// GPU counterpart of the `$divide_round_to_scale` scalar
/// ([io.trino.operator.scalar.DivideRoundToScale]): divides a DECIMAL128 dividend by an INT64
/// divisor, rounding the quotient (HALF_UP) to the dividend's scale.
///
/// Only a positive divisor is currently accepted.
public final class GpuDivideRoundToScale
        extends GpuExpression
{
    private static final DType UNSCALED = DType.create(DType.DTypeEnum.DECIMAL128, 0);

    private final GpuExpression dividend;
    private final GpuExpression divisor;

    public GpuDivideRoundToScale(GpuExpression dividend, GpuExpression divisor)
    {
        this.dividend = requireNonNull(dividend, "dividend is null");
        this.divisor = requireNonNull(divisor, "divisor is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector dividendColumn = dividend.evaluate(positionCount, inputColumns);
                ColumnVector divisorColumn = divisor.evaluate(positionCount, inputColumns)) {
            failOnInvalidDivisor(dividendColumn, divisorColumn);

            // Work on the unscaled 128-bit integers. The rounding intermediates are confined to the
            // roundingBump helpers so they are freed as soon as they are consumed, keeping only a few
            // device columns live at once.
            DType decimalType = dividendColumn.getType();
            try (ColumnVector unscaledDivisor = divisorColumn.castTo(UNSCALED);
                    ColumnView unscaledDividend = dividendColumn.bitCastTo(UNSCALED);
                    ColumnVector quotient = unscaledDividend.binaryOp(DIV, unscaledDivisor, UNSCALED);
                    ColumnVector bump = roundingBump(unscaledDividend, unscaledDivisor);
                    ColumnVector rounded = quotient.binaryOp(ADD, bump, UNSCALED);
                    ColumnView result = rounded.bitCastTo(decimalType)) {
                return result.copyToColumnVector();
            }
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuDivideRoundToScale other
                && dividend.equals(other.dividend)
                && divisor.equals(other.divisor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), dividend, divisor);
    }

    private static void failOnInvalidDivisor(ColumnVector dividendColumn, ColumnVector divisorColumn)
    {
        // Reject a non-positive divisor, but only where the dividend is non-null: a null dividend
        // yields null regardless of the divisor.
        try (Scalar zero = Scalar.fromLong(0);
                ColumnVector dividendNotNull = dividendColumn.isNotNull()) {
            try (ColumnVector negativeDivisor = divisorColumn.binaryOp(LESS, zero, DType.BOOL8);
                    ColumnVector negativeAndPresent = negativeDivisor.binaryOp(NULL_LOGICAL_AND, dividendNotNull, DType.BOOL8)) {
                if (anyTrue(negativeAndPresent)) {
                    throw new TrinoException(NOT_SUPPORTED, "Negative divisor is not supported");
                }
            }
            try (ColumnVector zeroDivisor = divisorColumn.equalTo(zero);
                    ColumnVector zeroAndPresent = zeroDivisor.binaryOp(NULL_LOGICAL_AND, dividendNotNull, DType.BOOL8)) {
                if (anyTrue(zeroAndPresent)) {
                    throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
                }
            }
        }
    }

    // +1 / -1 following the dividend's sign, applied only where 2 * |remainder| >= divisor
    // (rounding the half away from zero == HALF_UP); 0 elsewhere.
    private static ColumnVector roundingBump(ColumnView unscaledDividend, ColumnVector unscaledDivisor)
    {
        try (Scalar zero = Scalar.fromDecimal(BigInteger.ZERO, UNSCALED);
                Scalar plusOne = Scalar.fromDecimal(BigInteger.ONE, UNSCALED);
                Scalar minusOne = Scalar.fromDecimal(BigInteger.valueOf(-1), UNSCALED);
                ColumnVector roundAwayFromZero = roundAwayFromZero(unscaledDividend, unscaledDivisor);
                ColumnVector dividendNonNegative = unscaledDividend.binaryOp(GREATER_EQUAL, zero, DType.BOOL8);
                ColumnVector signedStep = dividendNonNegative.ifElse(plusOne, minusOne)) {
            return roundAwayFromZero.ifElse(signedStep, zero);
        }
    }

    // Mask that is true where the remainder is at least half the divisor, i.e. where the quotient
    // must round away from zero (HALF_UP).
    private static ColumnVector roundAwayFromZero(ColumnView unscaledDividend, ColumnVector unscaledDivisor)
    {
        // Compare 2 * |remainder| against the divisor rather than |remainder| against divisor / 2,
        // which would truncate the fraction for an odd divisor and get the exact half wrong. The
        // divisor is a row count (INT64), so 2 * |remainder| < 2^64, far within DECIMAL128 and never
        // overflowing.
        try (ClosingRef<ColumnVector> remainder = ClosingRef.own(unscaledDividend.binaryOp(MOD, unscaledDivisor, UNSCALED));
                ColumnVector absoluteRemainder = remainder.borrow().abs()) {
            remainder.close();
            try (ColumnVector twiceAbsoluteRemainder = absoluteRemainder.binaryOp(ADD, absoluteRemainder, UNSCALED)) {
                return twiceAbsoluteRemainder.binaryOp(GREATER_EQUAL, unscaledDivisor, DType.BOOL8);
            }
        }
    }
}
