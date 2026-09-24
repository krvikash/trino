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
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static com.google.common.math.LongMath.saturatedSubtract;
import static io.trino.operator.gpu.expression.CudfUtils.MAX_CALENDAR_EPOCH_DAY;
import static io.trino.operator.gpu.expression.CudfUtils.MIN_CALENDAR_EPOCH_DAY;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.operator.gpu.expression.GpuExpressions.worstOf;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static java.lang.Math.abs;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.subtractExact;
import static java.util.Objects.requireNonNull;

/// `date_add(unit, value, date_time)` with a constant `unit` and a constant `value`.
public final class GpuDateAdd
        extends GpuExpression
{
    /// Longest possible calendar month, bounding how far a calendar unit shift can move a timestamp.
    private static final long MAX_DAYS_PER_MONTH = 31;

    private final GpuExpression argument;
    private final DType columnType;
    private final Unit unit;
    private final long value;

    /// Shift in the carrier's resolution, or a number of months for a calendar unit.
    private final long shift;
    /// Inclusive range of inputs for which this expression matches the CPU implementation.
    private final long minInput;
    private final long maxInput;
    /// Whether the input range excludes any value the argument can hold, so that it is checked.
    private final boolean needsInputInRangeCheck;

    /// Returns a `date_add` expression, or empty when the unit does not apply to the carrier
    /// column, or when the shift is too large to apply without leaving the representable range.
    public static Optional<GpuExpression> create(GpuExpression argument, DType columnType, Unit unit, long value)
    {
        Carrier carrier = Carrier.forColumnType(columnType).orElse(null);
        if (carrier == null) {
            return Optional.empty();
        }
        long minResult = carrier.minResult();
        long maxResult = carrier.maxResult();
        long resolutionNanos = carrier.resolutionNanos();

        long shift;
        long minInput;
        long maxInput;
        try {
            if (unit.isCalendarUnit()) {
                shift = multiplyExact(value, unit.monthsPerUnit());
                if (shift != (int) shift) {
                    // cuDF takes the month count as a 32-bit scalar
                    return Optional.empty();
                }
                long dayInCarrierResolution = NANOSECONDS_PER_DAY / resolutionNanos;
                // Both the input and the result have to stay in the calendar cuDF can represent.
                // Saturating, because a carrier finer than the calendar reaches past the long, in
                // which case the carrier's own bound is the tighter one anyway.
                minInput = max(minResult, saturatedMultiply(MIN_CALENDAR_EPOCH_DAY, dayInCarrierResolution));
                maxInput = min(maxResult, saturatedAdd(saturatedMultiply(MAX_CALENDAR_EPOCH_DAY, dayInCarrierResolution), dayInCarrierResolution - 1));
                // A calendar shift has no constant length, so the bound it moves toward reserves the
                // longest possible month, which also rejects the last few days before the largest
                // representable timestamp. Only that bound needs the reserve: taking it off the
                // other one as well would reject inputs whose result stays in range.
                long drift = multiplyExact(abs(shift), MAX_DAYS_PER_MONTH * dayInCarrierResolution);
                if (shift > 0) {
                    // Moving later, so only the upper bound can be overshot
                    maxInput = subtractExact(maxInput, drift);
                }
                else if (shift < 0) {
                    // Moving earlier, so only the lower bound can be undershot
                    minInput = addExact(minInput, drift);
                }
                if (minInput > maxInput) {
                    // The reserve is wider than the calendar, so no input at all can be shifted
                    return Optional.empty();
                }
            }
            else {
                long nanosPerUnit = multiplyExact(unit.millisPerUnit(), NANOSECONDS_PER_MILLISECOND);
                if (nanosPerUnit % resolutionNanos != 0) {
                    // The unit is finer than the carrier's resolution. The CPU implementation then
                    // also rounds the result, which a plain shift cannot reproduce.
                    return Optional.empty();
                }
                shift = multiplyExact(value, nanosPerUnit / resolutionNanos);
                // Saturating, because a bound past the carrier's own range only widens the check to
                // values the column cannot hold, which leaves it exact
                minInput = saturatedSubtract(minResult, shift);
                maxInput = saturatedSubtract(maxResult, shift);
            }
        }
        catch (ArithmeticException _) {
            // The shift is out of range for every input, so let the CPU implementation report the error
            return Optional.empty();
        }
        return Optional.of(new GpuDateAdd(argument, columnType, unit, value, shift, minInput, maxInput));
    }

    private GpuDateAdd(GpuExpression argument, DType columnType, Unit unit, long value, long shift, long minInput, long maxInput)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.unit = requireNonNull(unit, "unit is null");
        this.value = value;
        this.shift = shift;
        this.minInput = minInput;
        this.maxInput = maxInput;
        this.needsInputInRangeCheck = minInput != Long.MIN_VALUE || maxInput != Long.MAX_VALUE;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ClosingOnce<ColumnVector> dateTime = ClosingOnce.own(argument.evaluate(positionCount, inputColumns, bindings));
                ClosingOnce<ColumnVector> int64Timestamp = ClosingOnce.own(dateTime.borrow().castTo(DType.INT64))) {
            if (needsInputInRangeCheck) {
                verifyInputInRange(int64Timestamp.borrow());
            }
            if (unit.isCalendarUnit()) {
                int64Timestamp.close();
                // cuDF clamps the day of month to the last day of the resulting month, as Trino does
                try (Scalar months = Scalar.fromInt((int) shift)) {
                    return dateTime.borrow().addCalendricalMonths(months);
                }
            }
            dateTime.close();
            // The input range check rules out an overflow here, and for a date it also rules out a
            // result outside the INT32 the cuDF day count is carried in.
            try (Scalar shiftScalar = Scalar.fromLong(shift);
                    ClosingOnce<ColumnVector> shifted = ClosingOnce.own(int64Timestamp.borrow().binaryOp(ADD, shiftScalar, DType.INT64))) {
                int64Timestamp.close();
                if (columnType.equals(DType.TIMESTAMP_DAYS)) {
                    // cuDF only reinterprets an INT32 as a day count
                    try (ColumnVector days = shifted.borrow().castTo(DType.INT32)) {
                        shifted.close();
                        return days.castTo(columnType);
                    }
                }
                return shifted.borrow().castTo(columnType);
            }
        }
    }

    private void verifyInputInRange(@Borrow ColumnVector int64Timestamp)
    {
        // The bounds fold two limits together. The CPU implementation holds the result in
        // milliseconds and rescales it with an exact multiplication, so it rejects results the
        // carrier column could still hold; rejecting those here makes both engines fail on the same
        // rows. A calendar unit additionally reaches only as far as cuDF's calendar, which is
        // narrower, so there the GPU fails on inputs the CPU implementation answers.
        try (Scalar minScalar = Scalar.fromLong(minInput);
                Scalar maxScalar = Scalar.fromLong(maxInput);
                ClosingOnce<ColumnVector> belowMin = ClosingOnce.own(int64Timestamp.binaryOp(LESS, minScalar, DType.BOOL8));
                ClosingOnce<ColumnVector> aboveMax = ClosingOnce.own(int64Timestamp.binaryOp(GREATER, maxScalar, DType.BOOL8));
                ColumnVector outOfRange = belowMin.borrow().binaryOp(NULL_LOGICAL_OR, aboveMax.borrow(), DType.BOOL8)) {
            belowMin.close();
            aboveMax.close();
            if (anyTrue(outOfRange)) {
                // The bounds are in the carrier column's own ticks: days for a date, and the
                // carrier's resolution for a timestamp.
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "date_add('%s', %s, ...) argument out of range supported by GPU: must be in [%s, %s]".formatted(unit.trinoDateAddUnit(), value, minInput, maxInput));
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        if (needsInputInRangeCheck) {
            return nullSkippingCheck(ImmutableList.of(argument));
        }
        return worstOf(ImmutableList.of(argument));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuDateAdd other
                && argument.equals(other.argument)
                && columnType.equals(other.columnType)
                && unit == other.unit
                && value == other.value;
        // derived: shift, minInput, maxInput, needsInputInRangeCheck
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument, columnType, unit, value);
        // derived: shift, minInput, maxInput, needsInputInRangeCheck
    }

    /// The units Trino accepts for `date_add`.
    public enum Unit
    {
        MILLISECOND("millisecond"),
        SECOND("second"),
        MINUTE("minute"),
        HOUR("hour"),
        DAY("day"),
        WEEK("week"),
        MONTH("month"),
        QUARTER("quarter"),
        YEAR("year");

        private static final Map<String, Unit> BY_UNIT;

        static {
            ImmutableMap.Builder<String, Unit> builder = ImmutableMap.builder();
            for (Unit unit : values()) {
                builder.put(unit.trinoDateAddUnit, unit);
            }
            BY_UNIT = builder.buildOrThrow();
        }

        private final String trinoDateAddUnit;

        Unit(String trinoDateAddUnit)
        {
            this.trinoDateAddUnit = requireNonNull(trinoDateAddUnit, "trinoDateAddUnit is null");
        }

        public String trinoDateAddUnit()
        {
            return trinoDateAddUnit;
        }

        /// Whether the unit spans a variable number of days, so that it has to be added as months.
        boolean isCalendarUnit()
        {
            return switch (this) {
                case MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK -> false;
                case MONTH, QUARTER, YEAR -> true;
            };
        }

        /// Length of the unit in milliseconds. Throws for a calendar unit, which has no fixed length.
        long millisPerUnit()
        {
            return switch (this) {
                case MILLISECOND -> 1;
                case SECOND -> MILLISECONDS_PER_SECOND;
                case MINUTE -> MILLISECONDS_PER_MINUTE;
                case HOUR -> MILLISECONDS_PER_HOUR;
                case DAY -> MILLISECONDS_PER_DAY;
                case WEEK -> 7L * MILLISECONDS_PER_DAY;
                case MONTH, QUARTER, YEAR -> throw new IllegalStateException("Not a fixed-length unit: " + this);
            };
        }

        /// Length of the unit in months. Throws for a fixed-length unit, which is not a whole
        /// number of months.
        int monthsPerUnit()
        {
            return switch (this) {
                case MONTH -> 1;
                case QUARTER -> 3;
                case YEAR -> 12;
                case MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK -> throw new IllegalStateException("Not a calendar unit: " + this);
            };
        }

        public static Optional<Unit> forTrinoDateAddUnit(String trinoDateAddUnit)
        {
            // Trino matches the unit case-insensitively over ASCII letters
            return Optional.ofNullable(BY_UNIT.get(trinoDateAddUnit.toLowerCase(Locale.ENGLISH)));
        }
    }

    /// Properties of a cuDF timestamp column carrying a Trino `date` or `timestamp(p)`.
    /// `resolutionNanos` is how many nanoseconds one increment of the column's integer value is.
    ///
    /// `minResult` and `maxResult` bound the results the CPU implementation can produce. It holds
    /// the result in milliseconds and returns `addExact(multiplyExact(epochMillis, 1000), microsOfMilli)`,
    /// where `microsOfMilli` is the `floorMod(input, 1000)` it carried over from the input. So the
    /// multiplication is what fails first, at `MAX_EPOCH_MILLIS` milliseconds, and the
    /// sub-millisecond remainder is added afterwards, never negative.
    ///
    /// That is why the microsecond carrier is not symmetric: the remainder can only push a result
    /// up, so the largest result it reaches is `MAX_EPOCH_MILLIS * 1000 + 999`, past the end of the
    /// long, while the smallest is exactly `-MAX_EPOCH_MILLIS * 1000`. Tightening `maxResult` to
    /// `MAX_EPOCH_MILLIS * 1000` would reject the last 807 microseconds of the range, which the CPU
    /// implementation returns without complaint. Past the end of the long the final `addExact`
    /// throws, so both engines reject those inputs and `maxResult` can stay at `Long.MAX_VALUE`.
    private record Carrier(long resolutionNanos, long minResult, long maxResult)
    {
        /// Largest number of milliseconds Trino can rescale to microseconds.
        private static final long MAX_EPOCH_MILLIS = Long.MAX_VALUE / MICROSECONDS_PER_MILLISECOND;

        public static Optional<Carrier> forColumnType(DType columnType)
        {
            return switch (columnType.getTypeId()) {
                // A Trino date is carried as INT32 days
                case TIMESTAMP_DAYS -> Optional.of(new Carrier(NANOSECONDS_PER_DAY, Integer.MIN_VALUE, Integer.MAX_VALUE));
                // A coarser carrier holds no sub-millisecond remainder, so its bounds are symmetric
                case TIMESTAMP_SECONDS -> Optional.of(new Carrier(NANOSECONDS_PER_SECOND, -(MAX_EPOCH_MILLIS / MILLISECONDS_PER_SECOND), MAX_EPOCH_MILLIS / MILLISECONDS_PER_SECOND));
                case TIMESTAMP_MILLISECONDS -> Optional.of(new Carrier(NANOSECONDS_PER_MILLISECOND, -MAX_EPOCH_MILLIS, MAX_EPOCH_MILLIS));
                case TIMESTAMP_MICROSECONDS -> Optional.of(new Carrier(NANOSECONDS_PER_MICROSECOND, -MAX_EPOCH_MILLIS * MICROSECONDS_PER_MILLISECOND, Long.MAX_VALUE));
                // The nanosecond carrier holds timestamp(7) to timestamp(9). Its own range, roughly
                // 1677 to 2262, is far narrower than what the CPU implementation can rescale, so a
                // result only has to stay inside the long for the two to agree.
                case TIMESTAMP_NANOSECONDS -> Optional.of(new Carrier(1, Long.MIN_VALUE, Long.MAX_VALUE));
                default -> Optional.empty();
            };
        }
    }
}
