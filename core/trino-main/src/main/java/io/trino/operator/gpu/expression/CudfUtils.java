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
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.time.LocalDate;

import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
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
    private static final long LOWER_EPOCH_DAY = LocalDate.of(Short.MIN_VALUE, 1, 1).toEpochDay();
    private static final long UPPER_EPOCH_DAY = LocalDate.of(Short.MAX_VALUE, 12, 31).toEpochDay();

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

    static @Move Scalar floatingScalar(DType floatingDType, double value)
    {
        return switch (floatingDType.getTypeId()) {
            case FLOAT32 -> Scalar.fromFloat((float) value);
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

    static void checkYearWithinInt16Range(@Borrow ColumnVector input)
    {
        DType type = input.getType();
        if (type.equals(DType.TIMESTAMP_NANOSECONDS)) {
            return;
        }
        try (Scalar minValidValue = minTimestampForInt16Year(type);
                Scalar maxValidValue = maxTimestampForInt16Year(type);
                ClosingOnce<ColumnVector> belowMinValid = ClosingOnce.own(input.binaryOp(LESS, minValidValue, DType.BOOL8));
                ClosingOnce<ColumnVector> aboveMaxValid = ClosingOnce.own(input.binaryOp(GREATER, maxValidValue, DType.BOOL8));
                ColumnVector outOfRange = belowMinValid.borrow().binaryOp(NULL_LOGICAL_OR, aboveMaxValid.borrow(), DType.BOOL8)) {
            belowMinValid.close();
            aboveMaxValid.close();
            if (anyTrue(outOfRange)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Year out of range supported by GPU: must be in [%s, %s]", Short.MIN_VALUE, Short.MAX_VALUE));
            }
        }
    }

    private static @Move Scalar minTimestampForInt16Year(DType type)
    {
        return switch (type.getTypeId()) {
            case TIMESTAMP_DAYS -> Scalar.timestampDaysFromInt(toIntExact(LOWER_EPOCH_DAY));
            case TIMESTAMP_SECONDS -> Scalar.timestampFromLong(type, LOWER_EPOCH_DAY * SECONDS_PER_DAY);
            case TIMESTAMP_MILLISECONDS -> Scalar.timestampFromLong(type, LOWER_EPOCH_DAY * MILLISECONDS_PER_DAY);
            case TIMESTAMP_MICROSECONDS -> Scalar.timestampFromLong(type, LOWER_EPOCH_DAY * MICROSECONDS_PER_DAY);
            default -> throw new IllegalStateException("Unexpected timestamp type: " + type);
        };
    }

    private static @Move Scalar maxTimestampForInt16Year(DType type)
    {
        return switch (type.getTypeId()) {
            case TIMESTAMP_DAYS -> Scalar.timestampDaysFromInt(toIntExact(UPPER_EPOCH_DAY));
            case TIMESTAMP_SECONDS -> Scalar.timestampFromLong(type, UPPER_EPOCH_DAY * SECONDS_PER_DAY + SECONDS_PER_DAY - 1);
            case TIMESTAMP_MILLISECONDS -> Scalar.timestampFromLong(type, UPPER_EPOCH_DAY * MILLISECONDS_PER_DAY + MILLISECONDS_PER_DAY - 1);
            case TIMESTAMP_MICROSECONDS -> Scalar.timestampFromLong(type, UPPER_EPOCH_DAY * MICROSECONDS_PER_DAY + MICROSECONDS_PER_DAY - 1);
            default -> throw new IllegalStateException("Unexpected timestamp type: " + type);
        };
    }
}
