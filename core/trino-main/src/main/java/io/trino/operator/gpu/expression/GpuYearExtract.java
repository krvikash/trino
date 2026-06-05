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
import java.util.List;

import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.SECONDS_PER_DAY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Extracts the year of a date/timestamp.
 *
 * @see GpuDateTimeExtract
 */
public class GpuYearExtract
        implements GpuExpression
{
    private static final long LOWER_EPOCH_DAY = LocalDate.of(Short.MIN_VALUE, 1, 1).toEpochDay();
    private static final long UPPER_EPOCH_DAY = LocalDate.of(Short.MAX_VALUE, 12, 31).toEpochDay();

    private final GpuExpression argument;

    public GpuYearExtract(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector timestamp = argument.evaluate(positionCount, inputColumns)) {
            checkOverflow(timestamp);
            return timestamp.year();
        }
    }

    private static void checkOverflow(@Borrow ColumnVector input)
    {
        DType type = input.getType();
        if (type.equals(DType.TIMESTAMP_NANOSECONDS)) {
            // All valid 64-bit TIMESTAMP_NANOSECONDS values have year() fitting in 16-bit signed integer.
            return;
        }
        try (Scalar minValidValue = minValidValue(type);
                Scalar maxValidValue = maxValidValue(type);
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

    private static @Move Scalar minValidValue(DType type)
    {
        return switch (type.getTypeId()) {
            case TIMESTAMP_DAYS -> Scalar.timestampDaysFromInt(toIntExact(LOWER_EPOCH_DAY));
            case TIMESTAMP_SECONDS -> Scalar.timestampFromLong(type, LOWER_EPOCH_DAY * SECONDS_PER_DAY);
            case TIMESTAMP_MILLISECONDS -> Scalar.timestampFromLong(type, LOWER_EPOCH_DAY * MILLISECONDS_PER_DAY);
            case TIMESTAMP_MICROSECONDS -> Scalar.timestampFromLong(type, LOWER_EPOCH_DAY * MICROSECONDS_PER_DAY);
            default -> throw new IllegalStateException("Unexpected timestamp type: " + type);
        };
    }

    private static @Move Scalar maxValidValue(DType type)
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
