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
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.DIV;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.PYMOD;
import static ai.rapids.cudf.BinaryOp.SUB;
import static ai.rapids.cudf.BinaryOp.TRUE_DIV;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static java.util.Objects.requireNonNull;

/// `to_unixtime(timestamp(p) with time zone)`. The function returns the instant, which is the whole
/// of the GPU representation of its argument.
///
/// The CPU implementation splits at the representation boundary and the split is observable, so it
/// is reproduced rather than simplified. Up to precision 3 the value is whole milliseconds and one
/// division gives the answer. From precision 4 the value is milliseconds plus a picosecond
/// remainder, and the two are converted and divided separately before being added, which rounds
/// twice: `to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345 UTC')` is `1589067296.1234498e0`, not
/// `1589067296.12345e0`, and dividing the whole instant once would produce the latter.
public final class GpuToUnixTime
        extends GpuExpression
{
    private final GpuExpression argument;
    private final DType instantType;

    public GpuToUnixTime(GpuExpression argument, DType instantType)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.instantType = requireNonNull(instantType, "instantType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector instant = argument.evaluate(positionCount, inputColumns, bindings);
                ColumnVector ticks = instant.castTo(DType.INT64)) {
            return switch (instantType.getTypeId()) {
                case TIMESTAMP_SECONDS -> {
                    try (Scalar millisPerSecond = Scalar.fromLong(MILLISECONDS_PER_SECOND);
                            ColumnVector epochMillis = ticks.binaryOp(MUL, millisPerSecond, DType.INT64)) {
                        yield epochSeconds(epochMillis);
                    }
                }
                case TIMESTAMP_MILLISECONDS -> epochSeconds(ticks);
                case TIMESTAMP_MICROSECONDS -> epochSecondsWithFraction(ticks, MICROSECONDS_PER_MILLISECOND, PICOSECONDS_PER_MICROSECOND);
                case TIMESTAMP_NANOSECONDS -> epochSecondsWithFraction(ticks, NANOSECONDS_PER_MILLISECOND, PICOSECONDS_PER_NANOSECOND);
                default -> throw new IllegalStateException("Unexpected instant type: " + instantType);
            };
        }
    }

    /// `epochMillis * 1.0 / MILLISECONDS_PER_SECOND`. TRUE_DIV converts both sides to double and
    /// divides, which is the same two steps in the same order.
    private static @Move ColumnVector epochSeconds(@Borrow ColumnVector epochMillis)
    {
        try (Scalar millisPerSecond = Scalar.fromLong(MILLISECONDS_PER_SECOND)) {
            return epochMillis.binaryOp(TRUE_DIV, millisPerSecond, DType.FLOAT64);
        }
    }

    /// `epochMillis * 1.0 / MILLISECONDS_PER_SECOND + picosOfMilli * 1.0 / PICOSECONDS_PER_SECOND`.
    ///
    /// TODO: https://github.com/trinodb/trino/pull/31386 makes the CPU round once; if it lands, this
    /// can be simplified to a single conversion of the whole instant
    private static @Move ColumnVector epochSecondsWithFraction(@Borrow ColumnVector ticks, long ticksPerMilli, long picosPerTick)
    {
        // cuDF's FLOOR_DIV goes through a double and is therefore lossy over the whole int64 range,
        // so the floor division is done as a truncating division of a value already made divisible
        try (Scalar ticksPerMilliScalar = Scalar.fromLong(ticksPerMilli);
                ColumnVector ticksOfMilli = ticks.binaryOp(PYMOD, ticksPerMilliScalar, DType.INT64);
                ColumnVector wholeMillisInTicks = ticks.binaryOp(SUB, ticksOfMilli, DType.INT64);
                ColumnVector epochMillis = wholeMillisInTicks.binaryOp(DIV, ticksPerMilliScalar, DType.INT64);
                ColumnVector seconds = epochSeconds(epochMillis);
                Scalar picosPerTickScalar = Scalar.fromLong(picosPerTick);
                ColumnVector picosOfMilli = ticksOfMilli.binaryOp(MUL, picosPerTickScalar, DType.INT64);
                Scalar picosPerSecond = Scalar.fromLong(PICOSECONDS_PER_SECOND);
                ColumnVector fraction = picosOfMilli.binaryOp(TRUE_DIV, picosPerSecond, DType.FLOAT64)) {
            return seconds.binaryOp(ADD, fraction, DType.FLOAT64);
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
