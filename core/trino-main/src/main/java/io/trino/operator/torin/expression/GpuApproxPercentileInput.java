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
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.torin.expression.CudfUtils.anyTrue;
import static io.trino.operator.torin.expression.CudfUtils.floatingScalar;
import static io.trino.operator.torin.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/// Passes through the values an `approx_percentile` aggregates, failing on a value the CPU
/// implementation rejects: a non-finite `real` or `double` (see `TDigestFunctions.verifyValue`), or
/// a `bigint` without an exact `double` representation (see
/// `ApproximateLongPercentileAggregations.toDoubleExact`). NULL values are skipped.
public final class GpuApproxPercentileInput
        extends GpuExpression
{
    private final GpuExpression source;
    private final DType dType;

    public GpuApproxPercentileInput(GpuExpression source, DType dType)
    {
        this.source = requireNonNull(source, "source is null");
        this.dType = requireNonNull(dType, "dType is null");
        checkArgument(dType.equals(DType.INT64) || dType.equals(DType.FLOAT32) || dType.equals(DType.FLOAT64), "Unsupported type: %s", dType);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ClosingRef<ColumnVector> values = ClosingRef.own(source.evaluate(positionCount, inputColumns, bindings))) {
            try (ColumnVector invalid = invalidValues(values.borrow())) {
                if (anyTrue(invalid)) {
                    throw invalidValueException(values.borrow(), invalid);
                }
            }
            return values.take();
        }
    }

    private @Move ColumnVector invalidValues(@Borrow ColumnVector values)
    {
        if (dType.equals(DType.INT64)) {
            return withoutExactDouble(values);
        }
        return nonFinite(values);
    }

    private static @Move ColumnVector nonFinite(@Borrow ColumnVector values)
    {
        DType type = values.getType();
        try (Scalar infinity = floatingScalar(type, Double.POSITIVE_INFINITY);
                ColumnVector nan = values.isNan();
                ColumnVector magnitude = values.abs();
                ColumnVector infinite = magnitude.binaryOp(EQUAL, infinity, DType.BOOL8)) {
            return nan.binaryOp(NULL_LOGICAL_OR, infinite, DType.BOOL8);
        }
    }

    private static @Move ColumnVector withoutExactDouble(@Borrow ColumnVector values)
    {
        // Mirrors `(long) (double) value == value`. A value rounding up to 2^63 is out of the long range, where the
        // Java cast saturates to Long.MAX_VALUE, so only Long.MAX_VALUE itself round-trips.
        try (Scalar twoToThe63 = Scalar.fromDouble(0x1p63);
                Scalar maxLong = Scalar.fromLong(Long.MAX_VALUE);
                ColumnVector asDouble = values.castTo(DType.FLOAT64);
                ColumnVector roundTripped = asDouble.castTo(DType.INT64);
                ColumnVector withinLongRange = asDouble.binaryOp(LESS, twoToThe63, DType.BOOL8);
                ColumnVector roundTripsExactly = roundTripped.binaryOp(EQUAL, values, DType.BOOL8);
                ColumnVector isMaxLong = values.binaryOp(EQUAL, maxLong, DType.BOOL8);
                ColumnVector exact = withinLongRange.ifElse(roundTripsExactly, isMaxLong)) {
            return exact.not();
        }
    }

    private TrinoException invalidValueException(@Borrow ColumnVector values, @Borrow ColumnVector invalid)
    {
        // Report the first invalid value in input order, like the CPU implementation
        try (Table table = new Table(values);
                Table invalidValues = table.filter(invalid);
                Scalar first = invalidValues.getColumn(0).getScalarElement(0)) {
            if (dType.equals(DType.INT64)) {
                return new TrinoException(INVALID_FUNCTION_ARGUMENT, format("no exact double representation for long: %s", first.getLong()));
            }
            double value = dType.equals(DType.FLOAT32) ? first.getFloat() : first.getDouble();
            return new TrinoException(INVALID_FUNCTION_ARGUMENT, format("value must be finite; was %s", value));
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(ImmutableList.of(source));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return source.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuApproxPercentileInput other
                && source.equals(other.source)
                && dType.equals(other.dType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, dType);
    }
}
