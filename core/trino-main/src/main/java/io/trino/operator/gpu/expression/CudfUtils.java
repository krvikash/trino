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
import ai.rapids.cudf.CudfException;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.airlift.log.Logger;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.function.Function;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.GREATER_EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.LESS_EQUAL;
import static ai.rapids.cudf.BinaryOp.MOD;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.SECONDS_PER_DAY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * Utilities helpful when working with rapids cudf library.
 */
final class CudfUtils
{
    private static final Logger log = Logger.get(CudfUtils.class);

    /// cuDF's datetime kernels go through `cuda::std::chrono::year_month_day`, whose year is a
    /// 16-bit value. `year::min()` is -32767; -32768 is the not-a-year sentinel. Outside this range
    /// cuDF silently returns a wrong result rather than reporting an error.
    static final int MIN_CALENDAR_YEAR = -32767;
    static final int MAX_CALENDAR_YEAR = 32767;
    static final long MIN_CALENDAR_EPOCH_DAY = LocalDate.of(MIN_CALENDAR_YEAR, 1, 1).toEpochDay();
    static final long MAX_CALENDAR_EPOCH_DAY = LocalDate.of(MAX_CALENDAR_YEAR, 12, 31).toEpochDay();
    static final DType UNSCALED_DECIMAL128 = DType.create(DType.DTypeEnum.DECIMAL128, 0);

    private CudfUtils() {}

    static @Move Scalar zero(DType operandType)
    {
        return integerScalar(operandType, 0);
    }

    static @Move Scalar integerScalar(DType integerDType, long value)
    {
        return switch (integerDType.getTypeId()) {
            case INT8 -> Scalar.fromByte((byte) value);
            case INT16 -> Scalar.fromShort((short) value);
            case INT32 -> Scalar.fromInt((int) value);
            case INT64 -> Scalar.fromLong(value);
            default -> throw new IllegalArgumentException("Unsupported integer DType: " + integerDType);
        };
    }

    static @Move Scalar floatingScalar(DType floatingDType, Double value)
    {
        return switch (floatingDType.getTypeId()) {
            case FLOAT32 -> Scalar.fromFloat(value != null ? value.floatValue() : null);
            case FLOAT64 -> Scalar.fromDouble(value);
            default -> throw new IllegalArgumentException("Unsupported floating DType: " + floatingDType);
        };
    }

    static long minValue(DType integerDType)
    {
        return switch (integerDType.getTypeId()) {
            case INT8 -> Byte.MIN_VALUE;
            case INT16 -> Short.MIN_VALUE;
            case INT32 -> Integer.MIN_VALUE;
            case INT64 -> Long.MIN_VALUE;
            default -> throw new IllegalArgumentException("Unsupported integer DType: " + integerDType);
        };
    }

    static long maxValue(DType integerDType)
    {
        return switch (integerDType.getTypeId()) {
            case INT8 -> Byte.MAX_VALUE;
            case INT16 -> Short.MAX_VALUE;
            case INT32 -> Integer.MAX_VALUE;
            case INT64 -> Long.MAX_VALUE;
            default -> throw new IllegalArgumentException("Unsupported integer DType: " + integerDType);
        };
    }

    enum Ties
    {
        /// `RoundingMode.HALF_UP`, as in `DoubleMath.roundToLong(value, HALF_UP)`
        AWAY_FROM_ZERO,
        /// `Math.round`
        TOWARD_POSITIVE_INFINITY,
    }

    /// Rounds a FLOAT64 column to the nearest integral value, breaking ties as `ties` says. NaN and
    /// the infinities come back unchanged.
    ///
    static @Move ColumnVector roundToIntegral(@Borrow ColumnVector value, Ties ties)
    {
        checkArgument(value.getType().equals(DType.FLOAT64), "Expected FLOAT64, got %s", value.getType());
        try (Scalar zero = Scalar.fromDouble(0.0);
                ColumnVector negative = value.binaryOp(LESS, zero, DType.BOOL8);
                ColumnVector magnitude = value.abs();
                ColumnVector floor = magnitude.floor();
                ColumnVector fraction = magnitude.binaryOp(SUB, floor, DType.FLOAT64);
                ColumnVector roundsUp = roundsUp(fraction, negative, ties);
                Scalar one = Scalar.fromDouble(1.0);
                ColumnVector floorPlusOne = floor.binaryOp(ADD, one, DType.FLOAT64);
                ColumnVector roundedMagnitude = roundsUp.ifElse(floorPlusOne, floor);
                Scalar minusOne = Scalar.fromDouble(-1.0);
                ColumnVector negated = roundedMagnitude.binaryOp(MUL, minusOne, DType.FLOAT64)) {
            return negative.ifElse(negated, roundedMagnitude);
        }
    }

    private static @Move ColumnVector roundsUp(@Borrow ColumnVector fraction, @Borrow ColumnVector negative, Ties ties)
    {
        try (Scalar half = Scalar.fromDouble(0.5)) {
            return switch (ties) {
                case AWAY_FROM_ZERO -> fraction.binaryOp(GREATER_EQUAL, half, DType.BOOL8);
                // Below zero, rounding the magnitude up moves away from positive infinity, so a tie keeps the floor
                case TOWARD_POSITIVE_INFINITY -> {
                    try (ColumnVector aboveHalf = fraction.binaryOp(GREATER, half, DType.BOOL8);
                            ColumnVector atLeastHalf = fraction.binaryOp(GREATER_EQUAL, half, DType.BOOL8)) {
                        yield negative.ifElse(aboveHalf, atLeastHalf);
                    }
                }
            };
        }
    }

    static boolean anyTrue(@Borrow ColumnVector boolColumn)
    {
        try (Scalar any = boolColumn.any()) {
            return any.isValid() && any.getBoolean();
        }
    }

    static boolean allTrue(@Borrow ColumnVector boolColumn)
    {
        try (Scalar all = boolColumn.all()) {
            return all.isValid() && all.getBoolean();
        }
    }

    static void checkYearWithinCudfCalendar(@Borrow ColumnVector input)
    {
        DType type = input.getType();
        if (type.equals(DType.TIMESTAMP_NANOSECONDS)) {
            return;
        }
        try (Scalar minValidValue = minTimestampInCudfCalendar(type);
                Scalar maxValidValue = maxTimestampInCudfCalendar(type);
                ClosingOnce<ColumnVector> belowMinValid = ClosingOnce.own(input.binaryOp(LESS, minValidValue, DType.BOOL8));
                ClosingOnce<ColumnVector> aboveMaxValid = ClosingOnce.own(input.binaryOp(GREATER, maxValidValue, DType.BOOL8));
                ColumnVector outOfRange = belowMinValid.borrow().binaryOp(NULL_LOGICAL_OR, aboveMaxValid.borrow(), DType.BOOL8)) {
            belowMinValid.close();
            aboveMaxValid.close();
            if (anyTrue(outOfRange)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Year out of range supported by GPU: must be in [%s, %s]", MIN_CALENDAR_YEAR, MAX_CALENDAR_YEAR));
            }
        }
    }

    private static @Move Scalar minTimestampInCudfCalendar(DType type)
    {
        return switch (type.getTypeId()) {
            case TIMESTAMP_DAYS -> Scalar.timestampDaysFromInt(toIntExact(MIN_CALENDAR_EPOCH_DAY));
            case TIMESTAMP_SECONDS -> Scalar.timestampFromLong(type, MIN_CALENDAR_EPOCH_DAY * SECONDS_PER_DAY);
            case TIMESTAMP_MILLISECONDS -> Scalar.timestampFromLong(type, MIN_CALENDAR_EPOCH_DAY * MILLISECONDS_PER_DAY);
            case TIMESTAMP_MICROSECONDS -> Scalar.timestampFromLong(type, MIN_CALENDAR_EPOCH_DAY * MICROSECONDS_PER_DAY);
            default -> throw new IllegalStateException("Unexpected timestamp type: " + type);
        };
    }

    private static @Move Scalar maxTimestampInCudfCalendar(DType type)
    {
        return switch (type.getTypeId()) {
            case TIMESTAMP_DAYS -> Scalar.timestampDaysFromInt(toIntExact(MAX_CALENDAR_EPOCH_DAY));
            case TIMESTAMP_SECONDS -> Scalar.timestampFromLong(type, MAX_CALENDAR_EPOCH_DAY * SECONDS_PER_DAY + SECONDS_PER_DAY - 1);
            case TIMESTAMP_MILLISECONDS -> Scalar.timestampFromLong(type, MAX_CALENDAR_EPOCH_DAY * MILLISECONDS_PER_DAY + MILLISECONDS_PER_DAY - 1);
            case TIMESTAMP_MICROSECONDS -> Scalar.timestampFromLong(type, MAX_CALENDAR_EPOCH_DAY * MICROSECONDS_PER_DAY + MICROSECONDS_PER_DAY - 1);
            default -> throw new IllegalStateException("Unexpected timestamp type: " + type);
        };
    }

    public static void failOnNonPositiveDivisor(@Borrow ColumnVector dividendColumn, @Borrow ColumnVector divisorColumn)
    {
        // Reject a non-positive divisor, but only where the dividend is non-null: a null dividend
        // yields null regardless of the divisor.
        try (Scalar zero = Scalar.fromLong(0);
                ColumnVector dividendNotNull = dividendColumn.isNotNull()) {
            try (ColumnVector nonPositiveDivisor = divisorColumn.binaryOp(LESS_EQUAL, zero, DType.BOOL8);
                    ColumnVector nonPositiveAndPresent = nonPositiveDivisor.binaryOp(NULL_LOGICAL_AND, dividendNotNull, DType.BOOL8)) {
                // A single device sync on the common path, where every divisor is positive
                if (!anyTrue(nonPositiveAndPresent)) {
                    return;
                }
            }
            try (ColumnVector negativeDivisor = divisorColumn.binaryOp(LESS, zero, DType.BOOL8);
                    ColumnVector negativeAndPresent = negativeDivisor.binaryOp(NULL_LOGICAL_AND, dividendNotNull, DType.BOOL8)) {
                if (anyTrue(negativeAndPresent)) {
                    throw new TrinoException(NOT_SUPPORTED, "Negative divisor is not supported");
                }
            }
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
    }

    /// Returns what to add to the truncated quotient dividend / divisor to round it HALF_UP: +1 or -1,
    /// following the dividend's sign, where the remainder is at least half the divisor, and 0 elsewhere.
    /// cuDF's DIV truncates toward zero, so moving one step away from zero rounds the half up. The
    /// divisor must be positive.
    public static @Move ColumnVector halfUpCorrection(@Borrow ColumnView unscaledDividend, @Borrow ColumnVector unscaledDivisor)
    {
        try (Scalar zero = Scalar.fromDecimal(BigInteger.ZERO, UNSCALED_DECIMAL128);
                Scalar plusOne = Scalar.fromDecimal(BigInteger.ONE, UNSCALED_DECIMAL128);
                Scalar minusOne = Scalar.fromDecimal(BigInteger.valueOf(-1), UNSCALED_DECIMAL128);
                ColumnVector roundAwayFromZero = roundAwayFromZero(unscaledDividend, unscaledDivisor);
                ColumnVector dividendNonNegative = unscaledDividend.binaryOp(GREATER_EQUAL, zero, DType.BOOL8);
                ColumnVector signedStep = dividendNonNegative.ifElse(plusOne, minusOne)) {
            return roundAwayFromZero.ifElse(signedStep, zero);
        }
    }

    // Mask that is true where the remainder is at least half the divisor, i.e. where the quotient
    // must round away from zero (HALF_UP).
    private static @Move ColumnVector roundAwayFromZero(@Borrow ColumnView unscaledDividend, @Borrow ColumnVector unscaledDivisor)
    {
        // Compare 2 * |remainder| against the divisor rather than |remainder| against divisor / 2,
        // which would truncate the fraction for an odd divisor and get the exact half wrong. The
        // divisor is a row count (INT64), so 2 * |remainder| < 2^64, far within DECIMAL128 and never
        // overflowing.
        try (ClosingRef<ColumnVector> remainder = ClosingRef.own(unscaledDividend.binaryOp(MOD, unscaledDivisor, UNSCALED_DECIMAL128));
                ColumnVector absoluteRemainder = remainder.borrow().abs()) {
            remainder.close();
            try (ColumnVector twiceAbsoluteRemainder = absoluteRemainder.binaryOp(ADD, absoluteRemainder, UNSCALED_DECIMAL128)) {
                return twiceAbsoluteRemainder.binaryOp(GREATER_EQUAL, unscaledDivisor, DType.BOOL8);
            }
        }
    }

    /// Whether cuDF can run `regexOperation`, a string operation applying the regex program `cudfPattern`.
    /// cuDF compiles the program inside each operation and rejects an unsupported pattern regardless of the
    /// strings, so a single row tells. Not an empty column, for which some operations skip their checks.
    static boolean cudfAcceptsRegex(String cudfPattern, Function<@Borrow ColumnVector, @Own ColumnVector> regexOperation)
    {
        try (ColumnVector probe = ColumnVector.fromStrings("");
                ColumnVector _ = regexOperation.apply(probe)) {
            return true;
        }
        catch (CudfException e) {
            // A CUDA error is a CudaException and running out of device memory an OutOfMemoryError, so neither is caught.
            // The message is not matched, as it is only cuDF's source location and free text.
            log.debug("Regex pattern %s rejected by cuDF, falling back to CPU: %s", cudfPattern, e.getMessage());
            return false;
        }
    }
}
