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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.DType.DURATION_MICROSECONDS;
import static ai.rapids.cudf.DType.TIMESTAMP_MICROSECONDS;
import static ai.rapids.cudf.DType.TIMESTAMP_MILLISECONDS;
import static io.trino.operator.torin.expression.CudfUtils.anyTrue;
import static io.trino.operator.torin.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.util.Objects.requireNonNull;

/// Casts a timestamp with more than 3 fractional digits to `timestamp(3)`, rounding half up like
/// [io.trino.operator.scalar.timestamp.TimestampToTimestampCast]. The CPU rounds only the microseconds and ignores
/// any finer digits, and cuDF's cast to microseconds drops them the same way.
public final class GpuTimestampToMillisCast
        extends GpuExpression
{
    private final GpuExpression argument;

    public GpuTimestampToMillisCast(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        // cuDF casts timestamps to a coarser unit by truncating, so adding half a millisecond first rounds half up
        try (ColumnVector timestamps = argument.evaluate(positionCount, inputColumns, bindings);
                ColumnVector micros = timestamps.castTo(TIMESTAMP_MICROSECONDS)) {
            try (ColumnVector ticks = micros.castTo(DType.INT64);
                    Scalar minInput = Scalar.fromLong((Long.MIN_VALUE / 1000) * 1000 - 500);
                    Scalar maxInput = Scalar.fromLong(Long.MAX_VALUE - 500);
                    ColumnVector belowRange = ticks.binaryOp(LESS, minInput, DType.BOOL8);
                    ColumnVector aboveRange = ticks.binaryOp(GREATER, maxInput, DType.BOOL8)) {
                if (anyTrue(belowRange) || anyTrue(aboveRange)) {
                    throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp rounding argument out of range supported by GPU");
                }
            }
            try (Scalar halfMilli = Scalar.durationFromLong(DURATION_MICROSECONDS, 500);
                    ColumnVector shifted = micros.binaryOp(ADD, halfMilli, TIMESTAMP_MICROSECONDS)) {
                return shifted.castTo(TIMESTAMP_MILLISECONDS);
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
        return obj instanceof GpuTimestampToMillisCast other
                && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
