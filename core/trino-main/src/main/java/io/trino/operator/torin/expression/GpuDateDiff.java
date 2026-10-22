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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.torin.ClosingOnce;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.BinaryOp.DIV;
import static ai.rapids.cudf.BinaryOp.FLOOR_DIV;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.SUB;
import static io.trino.operator.torin.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.torin.expression.GpuExpressions.worstOf;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.util.Objects.requireNonNull;

/// `date_diff(unit, date_time1, date_time2)` with a constant fixed-length `unit`, for a `date` or a
/// `timestamp(p)`.
///
/// The CPU implementation takes Joda's difference of the two instants in epoch milliseconds, the
/// timestamp floored to the millisecond, divided by the unit's length and truncated toward zero.
/// The calendar units, month, quarter and year, are not a fixed number of milliseconds and are not
/// supported.
public final class GpuDateDiff
        extends GpuExpression
{
    /// The fixed-length units Trino accepts for `date_diff`.
    public enum Unit
    {
        MILLISECOND("millisecond", 1),
        SECOND("second", MILLISECONDS_PER_SECOND),
        MINUTE("minute", MILLISECONDS_PER_MINUTE),
        HOUR("hour", MILLISECONDS_PER_HOUR),
        DAY("day", MILLISECONDS_PER_DAY),
        WEEK("week", 7L * MILLISECONDS_PER_DAY),
        // month, quarter, year are not supported because they have variable length
        /**/;

        private static final Map<String, Unit> BY_UNIT;

        static {
            ImmutableMap.Builder<String, Unit> builder = ImmutableMap.builder();
            for (Unit unit : values()) {
                builder.put(unit.trinoDateDiffUnit, unit);
            }
            BY_UNIT = builder.buildOrThrow();
        }

        private final String trinoDateDiffUnit;
        private final long millisPerUnit;

        Unit(String trinoDateDiffUnit, long millisPerUnit)
        {
            this.trinoDateDiffUnit = requireNonNull(trinoDateDiffUnit, "trinoDateDiffUnit is null");
            this.millisPerUnit = millisPerUnit;
        }

        public String trinoDateDiffUnit()
        {
            return trinoDateDiffUnit;
        }

        long millisPerUnit()
        {
            return millisPerUnit;
        }

        public static Optional<Unit> forTrinoDateDiffUnit(String trinoDateDiffUnit)
        {
            // Trino matches the unit case-insensitively over ASCII letters only, so for example the
            // Kelvin sign, which String.toLowerCase folds to `k`, must not match
            return Optional.ofNullable(BY_UNIT.get(Ascii.toLowerCase(trinoDateDiffUnit)));
        }
    }

    private final GpuExpression start;
    private final GpuExpression end;
    private final DType columnType;
    private final Unit unit;

    /// Length of the unit in the ticks the difference is taken in: days for a date, milliseconds
    /// for a timestamp.
    private final long ticksPerUnit;

    /// Returns a `date_diff` expression, or empty when the CPU implementation rejects the unit for
    /// the carrier column.
    public static Optional<GpuExpression> create(GpuExpression start, GpuExpression end, DType columnType, Unit unit)
    {
        long ticksPerUnit;
        switch (columnType.getTypeId()) {
            case TIMESTAMP_DAYS -> {
                // The CPU implementation accepts only day and longer units for a date
                if (unit.millisPerUnit() % MILLISECONDS_PER_DAY != 0) {
                    return Optional.empty();
                }
                ticksPerUnit = unit.millisPerUnit() / MILLISECONDS_PER_DAY;
            }
            case TIMESTAMP_SECONDS, TIMESTAMP_MILLISECONDS, TIMESTAMP_MICROSECONDS, TIMESTAMP_NANOSECONDS -> ticksPerUnit = unit.millisPerUnit();
            default -> {
                return Optional.empty();
            }
        }
        return Optional.of(new GpuDateDiff(start, end, columnType, unit, ticksPerUnit));
    }

    private GpuDateDiff(GpuExpression start, GpuExpression end, DType columnType, Unit unit, long ticksPerUnit)
    {
        this.start = requireNonNull(start, "start is null");
        this.end = requireNonNull(end, "end is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.unit = requireNonNull(unit, "unit is null");
        this.ticksPerUnit = ticksPerUnit;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        // Infallible: a date is an INT32 day count, and a timestamp floored to milliseconds is within
        // Long.MAX_VALUE / 1000 on every carrier, so the difference cannot overflow the INT64.
        try (ClosingOnce<ColumnVector> startTicks = ClosingOnce.own(toTicks(start.evaluate(positionCount, inputColumns, bindings)));
                ClosingOnce<ColumnVector> endTicks = ClosingOnce.own(toTicks(end.evaluate(positionCount, inputColumns, bindings)));
                ClosingOnce<ColumnVector> difference = ClosingOnce.own(endTicks.borrow().binaryOp(SUB, startTicks.borrow(), DType.INT64))) {
            startTicks.close();
            endTicks.close();
            if (ticksPerUnit == 1) {
                return difference.borrow().incRefCount();
            }
            // DIV truncates toward zero, as the CPU implementation does
            try (Scalar divisor = Scalar.fromLong(ticksPerUnit)) {
                return difference.borrow().binaryOp(DIV, divisor, DType.INT64);
            }
        }
    }

    private @Move ColumnVector toTicks(@Move ColumnVector dateTime)
    {
        try (ClosingOnce<ColumnVector> dateTimeOwner = ClosingOnce.own(dateTime);
                ClosingOnce<ColumnVector> ticks = ClosingOnce.own(dateTimeOwner.borrow().castTo(DType.INT64))) {
            dateTimeOwner.close();
            return switch (columnType.getTypeId()) {
                case TIMESTAMP_DAYS, TIMESTAMP_MILLISECONDS -> ticks.borrow().incRefCount();
                case TIMESTAMP_SECONDS -> scaled(ticks.borrow(), MUL, MILLISECONDS_PER_SECOND);
                // The CPU implementation floors a finer timestamp to the millisecond
                case TIMESTAMP_MICROSECONDS -> scaled(ticks.borrow(), FLOOR_DIV, MICROSECONDS_PER_MILLISECOND);
                case TIMESTAMP_NANOSECONDS -> scaled(ticks.borrow(), FLOOR_DIV, NANOSECONDS_PER_MILLISECOND);
                default -> throw new IllegalStateException("Unexpected column type: " + columnType);
            };
        }
    }

    private static @Move ColumnVector scaled(@Borrow ColumnVector ticks, BinaryOp operation, long factor)
    {
        try (Scalar factorScalar = Scalar.fromLong(factor)) {
            return ticks.binaryOp(operation, factorScalar, DType.INT64);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return worstOf(ImmutableList.of(start, end));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return anyNullOnNullInputs(ImmutableList.of(start, end));
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuDateDiff other
                && start.equals(other.start)
                && end.equals(other.end)
                && columnType.equals(other.columnType)
                && unit == other.unit;
        // derived: ticksPerUnit
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), start, end, columnType, unit);
        // derived: ticksPerUnit
    }
}
