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
package io.trino.sql.analyzer;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;

import java.math.BigInteger;
import java.util.Optional;

import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.toIntExact;

/**
 * Declining a value yields empty rather than throwing: the caller falls back to the subquery
 * predicate, so a watermark with no exact literal form costs pushdown, never correctness.
 */
final class IncrementalColumnWatermarkLiteral
{
    private IncrementalColumnWatermarkLiteral() {}

    static Optional<Expression> toLiteral(NodeLocation location, Type type, Type watermarkType, Object nativeValue)
    {
        if (!hasSameNativeRepresentation(type, watermarkType)) {
            return Optional.empty();
        }
        return switch (type) {
            case IntegerType _, BigintType _ -> Optional.of(new LongLiteral(location, ((Long) nativeValue).toString()));
            case DateType _ -> Optional.of(new GenericLiteral(location, "DATE", new SqlDate(toIntExact((Long) nativeValue)).toString()));
            case DecimalType decimalType -> {
                BigInteger unscaledValue = decimalType.isShort()
                        ? BigInteger.valueOf((Long) nativeValue)
                        : ((Int128) nativeValue).toBigInteger();
                String typeName = "DECIMAL(%d,%d)".formatted(decimalType.getPrecision(), decimalType.getScale());
                yield Optional.of(new GenericLiteral(location, typeName, Decimals.toString(unscaledValue, decimalType.getScale())));
            }
            case TimeType timeType -> {
                long picos = (Long) nativeValue;
                yield timeFitsPrecision(picos, timeType.getPrecision())
                        ? Optional.of(new GenericLiteral(location, "TIME", SqlTime.newInstance(timeType.getPrecision(), picos).toString()))
                        : Optional.empty();
            }
            case TimestampType timestampType when timestampType.isShort() -> {
                long epochMicros = (Long) nativeValue;
                yield timestampFitsPrecision(epochMicros, timestampType.getPrecision())
                        ? Optional.of(new GenericLiteral(location, "TIMESTAMP", SqlTimestamp.newInstance(timestampType.getPrecision(), epochMicros, 0).toString()))
                        : Optional.empty();
            }
            default -> Optional.empty();
        };
    }

    /**
     * The storage table may have widened the column past the view type, e.g. by decimal precision
     * promotion, so the watermark is only rendered when its native value means the same thing in the
     * view type. TIME is always picoseconds and a short TIMESTAMP always epoch microseconds, whatever
     * the precision; that the value fits the view precision is checked when rendering.
     */
    private static boolean hasSameNativeRepresentation(Type type, Type watermarkType)
    {
        return switch (type) {
            case IntegerType _, BigintType _ -> watermarkType instanceof IntegerType || watermarkType instanceof BigintType;
            case TimeType _ -> watermarkType instanceof TimeType;
            case TimestampType timestampType when timestampType.isShort() -> watermarkType instanceof TimestampType watermarkTimestampType && watermarkTimestampType.isShort();
            default -> type.equals(watermarkType);
        };
    }

    /**
     * {@link ExpressionAnalyzer} infers a TIME literal's precision from the number of fractional
     * digits in its text, not from a declared type parameter, so text of the wrong length would
     * silently type the literal differently than the column. Rounding a finer value to fit would
     * re-append rows when rounding down and drop them when rounding up, so it is declined instead.
     */
    private static boolean timeFitsPrecision(long picos, int precision)
    {
        return round(picos, MAX_PRECISION - precision) == picos;
    }

    /**
     * TIMESTAMP literals infer their precision the same way TIME literals do; see {@link #timeFitsPrecision}.
     */
    private static boolean timestampFitsPrecision(long epochMicros, int precision)
    {
        return round(epochMicros, MAX_SHORT_PRECISION - precision) == epochMicros;
    }
}
