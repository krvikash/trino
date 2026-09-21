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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static io.trino.operator.gpu.expression.CudfUtils.Ties.TOWARD_POSITIVE_INFINITY;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.roundToIntegral;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static java.util.Objects.requireNonNull;

/// `from_unixtime(double)`, which returns `timestamp(3) with time zone` in the session time zone.
/// Compilation admits it only in a UTC session, because that is the zone the GPU representation
/// holds.
public final class GpuFromUnixTime
        extends GpuExpression
{
    /// `packDateTimeWithZone` keeps the zone in the low 12 bits of the value, so a millisecond that
    /// does not survive the shift cannot be represented. The CPU implementation turns the resulting
    /// IllegalArgumentException into INVALID_FUNCTION_ARGUMENT, and so does this. Both bounds are
    /// below 2^53, so a double holds them exactly and the check can be made before the cast.
    public static final double MIN_EPOCH_MILLIS = -(double) (1L << 51);
    public static final double MAX_EPOCH_MILLIS = (1L << 51) - 1;

    private final GpuExpression argument;

    public GpuFromUnixTime(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ClosingOnce<ColumnVector> unixTime = ClosingOnce.own(argument.evaluate(positionCount, inputColumns, bindings));
                Scalar millisPerSecond = Scalar.fromDouble(MILLISECONDS_PER_SECOND);
                ClosingOnce<ColumnVector> scaled = ClosingOnce.own(unixTime.borrow().binaryOp(MUL, millisPerSecond, DType.FLOAT64))) {
            unixTime.close();
            try (ClosingOnce<ColumnVector> rounded = ClosingOnce.own(round(scaled.borrow()))) {
                scaled.close();
                verifyInRange(rounded.borrow());
                try (ColumnVector epochMillis = rounded.borrow().castTo(DType.INT64)) {
                    rounded.close();
                    return epochMillis.castTo(DType.TIMESTAMP_MILLISECONDS);
                }
            }
        }
    }

    /// Reproduces `Math.round`. NaN becomes zero, as `Math.round` does, and the CPU implementation
    /// then packs the epoch. An infinity stays infinite and is rejected by the range check.
    ///
    /// TODO: https://github.com/trinodb/trino/pull/31386 makes the CPU round like a cast to decimal
    /// and reject NaN; if it lands, this can be simplified to match it
    private static @Move ColumnVector round(@Borrow ColumnVector value)
    {
        try (ColumnVector rounded = roundToIntegral(value, TOWARD_POSITIVE_INFINITY);
                // is_nan is false for a null, so a null row keeps its null through the selection
                ColumnVector isNan = rounded.isNan();
                Scalar zero = Scalar.fromDouble(0.0)) {
            return isNan.ifElse(zero, rounded);
        }
    }

    private static void verifyInRange(@Borrow ColumnVector epochMillis)
    {
        try (Scalar minScalar = Scalar.fromDouble(MIN_EPOCH_MILLIS);
                Scalar maxScalar = Scalar.fromDouble(MAX_EPOCH_MILLIS);
                ClosingOnce<ColumnVector> belowMin = ClosingOnce.own(epochMillis.binaryOp(LESS, minScalar, DType.BOOL8));
                ClosingOnce<ColumnVector> aboveMax = ClosingOnce.own(epochMillis.binaryOp(GREATER, maxScalar, DType.BOOL8));
                ColumnVector outOfRange = belowMin.borrow().binaryOp(NULL_LOGICAL_OR, aboveMax.borrow(), DType.BOOL8)) {
            belowMin.close();
            aboveMax.close();
            if (anyTrue(outOfRange)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "from_unixtime() argument out of range: rounds to milliseconds outside [%s, %s]".formatted((long) MIN_EPOCH_MILLIS, (long) MAX_EPOCH_MILLIS));
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(ImmutableList.of(argument));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuFromUnixTime other
                && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
