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
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Decimals;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.GREATER_EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.util.Objects.requireNonNull;

/// Casts a decimal to another decimal of the same scale but smaller precision.
public final class GpuNarrowingDecimalCast
        extends GpuExpression
{
    private final GpuExpression input;
    private final DType targetType;
    private final int targetPrecision;
    // Unscaled limits for the target precision
    private final BigInteger upperLimit;
    private final BigInteger lowerLimit;

    public GpuNarrowingDecimalCast(GpuExpression input, DType targetType, int targetPrecision)
    {
        this.input = requireNonNull(input, "input is null");
        this.targetType = requireNonNull(targetType, "targetType is null");
        checkArgument(0 <= targetPrecision && targetPrecision <= Decimals.MAX_PRECISION, "Invalid targetPrecision: %s", targetPrecision);
        this.targetPrecision = targetPrecision;
        this.upperLimit = BigInteger.TEN.pow(targetPrecision);
        this.lowerLimit = upperLimit.negate();
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector value = input.evaluate(positionCount, inputColumns)) {
            // Changing scale is currently unsupported
            verify(value.getType().getScale() == targetType.getScale(), "scale must be preserved: %s -> %s", value.getType(), targetType);
            // Input must be 128-bit. If the input was 64-bit but target was 128-bit, the bounds scalars would overflow.
            verify(value.getType().getTypeId() == DType.DTypeEnum.DECIMAL128, "input must be DECIMAL128: %s", value.getType());

            try (Scalar upperLimit = Scalar.fromDecimal(this.upperLimit, value.getType());
                    ColumnVector tooHigh = value.binaryOp(GREATER_EQUAL, upperLimit, DType.BOOL8)) {
                if (anyTrue(tooHigh)) {
                    throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
                }
            }

            try (Scalar lowerLimit = Scalar.fromDecimal(this.lowerLimit, value.getType());
                    ColumnVector tooLow = value.binaryOp(LESS_EQUAL, lowerLimit, DType.BOOL8)) {
                if (anyTrue(tooLow)) {
                    throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
                }
            }

            if (value.getType().equals(targetType)) {
                return value.incRefCount();
            }
            return value.castTo(targetType);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuNarrowingDecimalCast other
                && input.equals(other.input)
                && targetType.equals(other.targetType)
                && targetPrecision == other.targetPrecision;
        // derived: upperLimit
        // derived: lowerLimit
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getClass(),
                input,
                targetType,
                targetPrecision);
        // derived: upperLimit
        // derived: lowerLimit
    }
}
