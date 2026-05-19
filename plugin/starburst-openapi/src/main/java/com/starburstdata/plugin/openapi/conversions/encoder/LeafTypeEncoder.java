/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions.encoder;

import com.starburstdata.plugin.openapi.conversions.ir.BooleanIr;
import com.starburstdata.plugin.openapi.conversions.ir.LeafIr;
import com.starburstdata.plugin.openapi.conversions.ir.NumberIr;
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TrinoNumber.BigDecimalValue;
import io.trino.spi.type.TrinoNumber.Infinity;
import io.trino.spi.type.TrinoNumber.NotANumber;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSUPPORTED_PARAMETER_VALUE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class LeafTypeEncoder
        implements TypeEncoder
{
    private final String name;
    private final Type type;

    LeafTypeEncoder(String name, Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public SerializedValue serialize(Object value)
    {
        return new SerializedString(serializeToString(value));
    }

    public abstract String serializeToString(Object value);

    public static final LeafTypeEncoder LONG_ENCODER = new LeafTypeEncoder("LONG_ENCODER", BIGINT)
    {
        @Override
        public String serializeToString(Object value)
        {
            return Long.toString((Long) value);
        }
    };

    public static final LeafTypeEncoder INTEGER_ENCODER = new LeafTypeEncoder("INTEGER_ENCODER", INTEGER)
    {
        @Override
        public String serializeToString(Object value)
        {
            return Long.toString((Long) value);
        }
    };

    public static final LeafTypeEncoder DOUBLE_ENCODER = new LeafTypeEncoder("DOUBLE_ENCODER", DOUBLE)
    {
        @Override
        public String serializeToString(Object value)
        {
            // Expansion rules around floating point numbers is unclear.
            // See https://datatracker.ietf.org/doc/html/rfc6570#section-1.2 there are no numbers.
            return BigDecimal.valueOf((Double) value).toPlainString();
        }
    };

    public static final LeafTypeEncoder FLOAT_ENCODER = new LeafTypeEncoder("FLOAT_ENCODER", REAL)
    {
        @Override
        public String serializeToString(Object value)
        {
            return BigDecimal.valueOf(intBitsToFloat(toIntExact((Long) value))).toPlainString();
        }
    };

    public static final LeafTypeEncoder BOOLEAN_ENCODER = new LeafTypeEncoder("BOOLEAN_ENCODER", BOOLEAN)
    {
        @Override
        public String serializeToString(Object value)
        {
            return Boolean.toString((Boolean) value);
        }
    };

    public static final LeafTypeEncoder STRING_ENCODER = new LeafTypeEncoder("STRING_ENCODER", VARCHAR)
    {
        @Override
        public String serializeToString(Object value)
        {
            return ((Slice) value).toStringUtf8();
        }
    };

    public static final LeafTypeEncoder BYTE_ENCODER = new LeafTypeEncoder("BYTE_ENCODER", VARBINARY)
    {
        @Override
        public String serializeToString(Object value)
        {
            return Base64.getEncoder().encodeToString(((Slice) value).byteArray());
        }
    };

    public static final LeafTypeEncoder UUID_ENCODER = new LeafTypeEncoder("UUID_ENCODER", UUID)
    {
        @Override
        public String serializeToString(Object value)
        {
            return trinoUuidToJavaUuid((Slice) value).toString();
        }
    };

    public static final LeafTypeEncoder DATE_ENCODER = new LeafTypeEncoder("DATE_ENCODER", DATE)
    {
        @Override
        public String serializeToString(Object value)
        {
            LocalDate date = LocalDate.ofEpochDay((Long) value);
            int year = date.getYear();
            if (year < 0 || year > 9999) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Year %s must be greater than or equal to 0 and less than or equal to 9999".formatted(year));
            }
            return date.toString();
        }
    };

    public static final LeafTypeEncoder DATE_TIME_ENCODER = new LeafTypeEncoder("DATE_TIME_ENCODER", TIMESTAMP_TZ_PICOS)
    {
        // Used only for the sub-nanosecond path; ISO_OFFSET_DATE_TIME handles the common case.
        private static final DateTimeFormatter DATE_TIME_PREFIX = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");

        @Override
        public String serializeToString(Object value)
        {
            LongTimestampWithTimeZone lts = (LongTimestampWithTimeZone) value;
            int subMilliNanos = lts.getPicosOfMilli() / 1_000;
            int subNanoPicos = lts.getPicosOfMilli() % 1_000;
            Instant instant = Instant.ofEpochMilli(lts.getEpochMillis()).plusNanos(subMilliNanos);
            ZoneId zoneId = ZoneId.of(TimeZoneKey.getTimeZoneKey(lts.getTimeZoneKey()).getId());
            ZonedDateTime zonedDateTime = instant.atZone(zoneId);

            int year = zonedDateTime.getYear();
            if (year < 0 || year > 9999) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Year %s must be greater than or equal to 0 and less than or equal to 9999".formatted(year));
            }

            if (subNanoPicos == 0) {
                // ISO_OFFSET_DATE_TIME handles up to 9 fractional digits and trims trailing zeros
                return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime);
            }
            // Sub-nanosecond pico precision: format 12 fractional digits, trimming trailing zeros
            String fraction = String.format("%09d%03d", zonedDateTime.getNano(), subNanoPicos);
            return DATE_TIME_PREFIX.format(zonedDateTime) + '.' + fraction.substring(0, fraction.length()) + zonedDateTime.getOffset().getId();
        }
    };

    public static final LeafTypeEncoder NUMBER_ENCODER = new LeafTypeEncoder("NUMBER_ENCODER", NUMBER)
    {
        @Override
        public String serializeToString(Object value)
        {
            return asValidBigDecimal(value).toString();
        }
    };

    public static final LeafTypeEncoder INTEGER_NUMBER_ENCODER = new LeafTypeEncoder("INTEGER_NUMBER_ENCODER", NUMBER)
    {
        @Override
        public String serializeToString(Object value)
        {
            BigDecimal bigDecimal = asValidBigDecimal(value);
            if (bigDecimal.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                throw new TrinoException(
                        OPENAPI_UNSUPPORTED_PARAMETER_VALUE,
                        "Cannot use non-integer NUMBER value for an integer parameter");
            }
            return bigDecimal.toPlainString();
        }
    };

    private static BigDecimal asValidBigDecimal(Object value)
    {
        return switch (((TrinoNumber) value).toBigDecimal()) {
            case BigDecimalValue(BigDecimal bigDecimal) -> bigDecimal;
            case NotANumber _, Infinity _ -> throw new TrinoException(
                    OPENAPI_UNSUPPORTED_PARAMETER_VALUE,
                    "NaN and Infinity are not supported as NUMBER parameter values");
        };
    }

    public static LeafTypeEncoder from(LeafIr leafIr)
    {
        return switch (leafIr) {
            case BooleanIr _ -> BOOLEAN_ENCODER;
            case NumberIr(NumberIr.Format format) -> switch (format) {
                case NONE_INTEGER -> INTEGER_NUMBER_ENCODER;
                case NONE_NUMBER -> NUMBER_ENCODER;
                case INT64 -> LONG_ENCODER;
                case DOUBLE -> DOUBLE_ENCODER;
                case INT32 -> INTEGER_ENCODER;
                case FLOAT -> FLOAT_ENCODER;
            };
            case StringIr(StringIr.Format format) -> switch (format) {
                case NONE -> STRING_ENCODER;
                case UUID -> UUID_ENCODER;
                case BYTE -> BYTE_ENCODER;
                case DATE -> DATE_ENCODER;
                case DATE_TIME -> DATE_TIME_ENCODER;
            };
        };
    }
}
