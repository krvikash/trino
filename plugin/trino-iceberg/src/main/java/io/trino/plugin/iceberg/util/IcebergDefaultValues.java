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
package io.trino.plugin.iceberg.util;

import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import jakarta.annotation.Nullable;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Locale.ENGLISH;

public final class IcebergDefaultValues
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();
    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(" ")
            .appendZoneId()
            .optionalEnd()
            .toFormatter();

    private IcebergDefaultValues() {}

    public static String toTrinoDefaultValue(Type type, Object value)
    {
        if (type instanceof Types.TimestampNanoType timestampNanoType) {
            // Avoid using the following Literal code because Iceberg library has an overflow issue https://github.com/apache/iceberg/issues/13160
            boolean shouldAdjustToUtc = timestampNanoType.shouldAdjustToUTC();
            LocalDateTime timestamp = DateTimeUtil.timestampFromNanos(Long.parseLong(String.valueOf(value)));
            if (shouldAdjustToUtc) {
                return "TIMESTAMP '%s UTC'".formatted(TIMESTAMP_TZ_FORMATTER.format(timestamp));
            }
            return "TIMESTAMP '%s'".formatted(TIMESTAMP_FORMATTER.format(timestamp));
        }

        Literal<Object> literal = Expressions.lit(value).to(type);
        return switch (type.typeId()) {
            case BOOLEAN, INTEGER, LONG -> String.valueOf(value);
            case FLOAT -> "REAL '%s'".formatted(String.valueOf(value));
            case DOUBLE -> "DOUBLE '%s'".formatted(String.valueOf(value));
            case DECIMAL -> "DECIMAL '%s'".formatted(String.valueOf(value));
            case STRING -> {
                String string = String.valueOf(value);
                string = string.replace("'", "''");
                yield "'%s'".formatted(string);
            }
            case BINARY -> literal.toString();
            case UUID -> "UUID '%s'".formatted(String.valueOf(value));
            case TIME -> {
                java.time.LocalTime time = java.time.LocalTime.ofNanoOfDay(Long.parseLong(String.valueOf(value)) * NANOSECONDS_PER_MICROSECOND);
                yield "TIME '%s'".formatted(TIME_FORMATTER.format(time));
            }
            case DATE -> {
                LocalDate date = LocalDate.ofEpochDay(Long.parseLong(String.valueOf(value)));
                yield "'%s'".formatted(DATE_FORMATTER.format(date));
            }
            case TIMESTAMP -> {
                boolean shouldAdjustToUtc = ((Types.TimestampType) type).shouldAdjustToUTC();
                LocalDateTime timestamp = DateTimeUtil.timestampFromMicros(Long.parseLong(String.valueOf(value)));
                if (shouldAdjustToUtc) {
                    yield "TIMESTAMP '%s UTC'".formatted(TIMESTAMP_TZ_FORMATTER.format(timestamp));
                }
                yield "TIMESTAMP '%s'".formatted(TIMESTAMP_FORMATTER.format(timestamp));
            }
            default -> throw new TrinoException(NOT_SUPPORTED, "Cannot convert Iceberg value to string literal %s %s".formatted(type, value));
        };
    }

    @Nullable
    public static Literal<?> toIcebergLiteral(Type type, String value)
    {
        Expression expression = SQL_PARSER.createExpression(value);
        if (!(expression instanceof io.trino.sql.tree.Literal)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported expression: " + value);
        }

        return switch (expression) {
            case NullLiteral _ -> null;
            case BooleanLiteral literal -> Literal.of(literal.getValue());
            case LongLiteral literal -> Literal.of(literal.getParsedValue());
            case DoubleLiteral literal -> Literal.of(literal.getValue());
            case DecimalLiteral literal -> Literal.of(literal.getValue());
            case StringLiteral literal -> Literal.of(literal.getValue());
            case BinaryLiteral literal -> Literal.of(literal.getValue());
            case GenericLiteral literal -> {
                String typeName = literal.getType().toLowerCase(ENGLISH);
                switch (typeName) {
                    case "date" -> {
                        LocalDate date = LocalDate.parse(literal.getValue(), DATE_FORMATTER);
                        yield Literal.of(date.toEpochDay());
                    }
                    case "real" -> {
                        yield Literal.of(Float.parseFloat(literal.getValue()));
                    }
                    case "double" -> {
                        yield Literal.of(Double.parseDouble(literal.getValue()));
                    }
                    case "time" -> {
                        yield Literal.of(literal.getValue());
                    }
                    case "timestamp" -> {
                        if (type instanceof Types.TimestampType timestampType) {
                            boolean shouldAdjustToUtc = timestampType.shouldAdjustToUTC();
                            if (shouldAdjustToUtc) {
                                ZonedDateTime timestamp = TIMESTAMP_TZ_FORMATTER.parse(literal.getValue(), ZonedDateTime::from);
                                ZonedDateTime zonedDateTime = timestamp.withZoneSameInstant(UTC);
                                yield Literal.of(zonedDateTime.toString());
                            }
                            LocalDateTime timestamp = TIMESTAMP_FORMATTER.parse(literal.getValue(), LocalDateTime::from);
                            yield Literal.of(timestamp.toString());
                        }
                        if (type instanceof Types.TimestampNanoType timestampNanoType) {
                            boolean shouldAdjustToUtc = timestampNanoType.shouldAdjustToUTC();
                            if (shouldAdjustToUtc) {
                                ZonedDateTime timestamp = TIMESTAMP_TZ_FORMATTER.parse(literal.getValue(), ZonedDateTime::from);
                                ZonedDateTime zonedDateTime = timestamp.withZoneSameInstant(UTC);
                                yield Literal.of(zonedDateTime.toString());
                            }
                            LocalDateTime timestamp = TIMESTAMP_FORMATTER.parse(literal.getValue(), LocalDateTime::from);
                            yield Literal.of(timestamp.toString());
                        }
                    }
                    case "json" -> {
                        throw new TrinoException(NOT_SUPPORTED, "Variant is not supported as default values");
                    }
                }
                if (typeName.equals("uuid")) {
                    yield Literal.of(literal.getValue());
                }
                throw new TrinoException(NOT_SUPPORTED, "Cannot convert Trino value to Iceberg literal: " + value);
            }
            default -> throw new TrinoException(NOT_SUPPORTED, "Cannot convert Trino value to Iceberg literal: " + value);
        };
    }
}
