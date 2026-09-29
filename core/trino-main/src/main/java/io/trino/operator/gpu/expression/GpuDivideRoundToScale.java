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
import com.google.common.collect.ImmutableList;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.DIV;
import static io.trino.operator.gpu.expression.CudfUtils.UNSCALED_DECIMAL128;
import static io.trino.operator.gpu.expression.CudfUtils.failOnNonPositiveDivisor;
import static io.trino.operator.gpu.expression.CudfUtils.halfUpCorrection;
import static io.trino.operator.gpu.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static java.util.Objects.requireNonNull;

/// GPU counterpart of the `$divide_round_to_scale` scalar
/// ([io.trino.operator.scalar.DivideRoundToScale]): divides a DECIMAL128 dividend by an INT64
/// divisor, rounding the quotient (HALF_UP) to the dividend's scale.
///
/// Only a positive divisor is currently accepted.
public final class GpuDivideRoundToScale
        extends GpuExpression
{
    private final GpuExpression dividend;
    private final GpuExpression divisor;

    public GpuDivideRoundToScale(GpuExpression dividend, GpuExpression divisor)
    {
        this.dividend = requireNonNull(dividend, "dividend is null");
        this.divisor = requireNonNull(divisor, "divisor is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector dividendColumn = dividend.evaluate(positionCount, inputColumns, bindings);
                ColumnVector divisorColumn = divisor.evaluate(positionCount, inputColumns, bindings)) {
            failOnNonPositiveDivisor(dividendColumn, divisorColumn);

            // Work on the unscaled 128-bit integers. The rounding intermediates are confined to the
            // halfUpCorrection helpers so they are freed as soon as they are consumed, keeping only a few
            // device columns live at once.
            DType decimalType = dividendColumn.getType();
            try (ColumnVector unscaledDivisor = divisorColumn.castTo(UNSCALED_DECIMAL128);
                    ColumnView unscaledDividend = dividendColumn.bitCastTo(UNSCALED_DECIMAL128);
                    ColumnVector quotient = unscaledDividend.binaryOp(DIV, unscaledDivisor, UNSCALED_DECIMAL128);
                    ColumnVector correction = halfUpCorrection(unscaledDividend, unscaledDivisor);
                    ColumnVector rounded = quotient.binaryOp(ADD, correction, UNSCALED_DECIMAL128);
                    ColumnView result = rounded.bitCastTo(decimalType)) {
                return result.copyToColumnVector();
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(ImmutableList.of(dividend, divisor));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return anyNullOnNullInputs(ImmutableList.of(dividend, divisor));
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
}
