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
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/// `to_unixtime(timestamp(p) with time zone)`. The function returns the instant, which is the whole
/// of the GPU representation of its argument.
///
/// The CPU implementation rounds the exact instant, in seconds, to the nearest double once. Read as
/// a decimal with the scale of its unit, the instant converts to the same double: cuDF divides it by
/// the power of ten with 128 bits of headroom, which leaves no doubt about the side of a midpoint,
/// and rounds the quotient once.
public final class GpuToUnixTime
        extends GpuExpression
{
    private final GpuExpression argument;
    private final DType instantType;
    private final DType decimalSeconds;

    public GpuToUnixTime(GpuExpression argument, DType instantType)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.instantType = requireNonNull(instantType, "instantType is null");
        int fractionalDigits = switch (instantType.getTypeId()) {
            case TIMESTAMP_SECONDS -> 0;
            case TIMESTAMP_MILLISECONDS -> 3;
            case TIMESTAMP_MICROSECONDS -> 6;
            case TIMESTAMP_NANOSECONDS -> 9;
            default -> throw new IllegalArgumentException("Unexpected instant type: " + instantType);
        };
        this.decimalSeconds = DType.create(DType.DTypeEnum.DECIMAL64, -fractionalDigits);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector instant = argument.evaluate(positionCount, inputColumns, bindings);
                ColumnView seconds = instant.bitCastTo(decimalSeconds)) {
            return seconds.castTo(DType.FLOAT64);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return argument.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuToUnixTime other
                && argument.equals(other.argument)
                && instantType.equals(other.instantType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument, instantType);
    }
}
