/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.LocalDate;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public final class LiteralFormatter
{
    private LiteralFormatter() {}

    public static String formatLiteral(Type type, Object value)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            boolean typedValue = (boolean) value;
            return Boolean.toString(typedValue);
        }

        if (javaType == double.class) {
            double typedValue = (double) value;
            return formatStringLiteral("DOUBLE", Double.toString(typedValue));
        }

        if (javaType == long.class) {
            long typedValue = (long) value;

            if (type == TINYINT) {
                return formatStringLiteral("TINYINT", Byte.toString(SignedBytes.checkedCast(typedValue)));
            }

            if (type == REAL) {
                return formatStringLiteral("REAL", Float.toString(intBitsToFloat(toIntExact(typedValue))));
            }

            if (type == DATE) {
                return formatStringLiteral("DATE", LocalDate.ofEpochDay(typedValue).toString());
            }

            if (type == BIGINT) {
                return formatStringLiteral("BIGINT", Long.toString(typedValue));
            }

            if (type == INTEGER) {
                return formatStringLiteral("INTEGER", Integer.toString(toIntExact(typedValue)));
            }

            if (type instanceof DecimalType decimalType) {
                BigInteger unscaledValue = BigInteger.valueOf(typedValue);
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                return formatStringLiteral("DECIMAL", bigDecimal.toString());
            }

            if (type == SMALLINT) {
                return formatStringLiteral(
                        "SMALLINT",
                        Short.toString(Shorts.checkedCast(typedValue)));
            }

            if (type instanceof TimeType timeType) {
                return formatStringLiteral(
                        "TIME",
                        SqlTime.newInstance(timeType.getPrecision(), typedValue).toString());
            }

            if (type instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    return formatStringLiteral(
                            "TIMESTAMP",
                            SqlTimestamp.newInstance(timestampType.getPrecision(), typedValue, 0).toString());
                }
            }

            if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
                verify(timeWithTimeZoneType.isShort(), "Short TimeWithTimeZoneType was expected");
                return formatStringLiteral("TIME", SqlTimeWithTimeZone.newInstance(
                        timeWithTimeZoneType.getPrecision(),
                        unpackTimeNanos(typedValue) * PICOSECONDS_PER_NANOSECOND,
                        unpackOffsetMinutes(typedValue)).toString());
            }

            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                verify(timestampWithTimeZoneType.isShort(), "Short TimestampWithTimezone was expected");
                return formatStringLiteral(
                        "TIMESTAMP",
                        SqlTimestampWithTimeZone.newInstance(
                                timestampWithTimeZoneType.getPrecision(),
                                unpackMillisUtc(typedValue),
                                0,
                                unpackZoneKey(typedValue)).toString());
            }
        }

        if (javaType == Slice.class) {
            Slice typedValue = (Slice) value;
            return escapeStringLiteral(typedValue.toStringUtf8());
        }

        if (javaType == Int128.class) {
            if (type instanceof DecimalType decimalType) {
                Int128 typedValue = (Int128) value;

                BigInteger unscaledValue = typedValue.toBigInteger();
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                return formatStringLiteral("DECIMAL", bigDecimal.toString());
            }
        }

        switch (type) {
            case TimeWithTimeZoneType timeWithTimeZoneType when javaType == LongTimeWithTimeZone.class -> {
                LongTimeWithTimeZone typedValue = (LongTimeWithTimeZone) value;
                return formatStringLiteral("TIME",
                        SqlTimeWithTimeZone.newInstance(
                                timeWithTimeZoneType.getPrecision(),
                                typedValue.getPicoseconds(),
                                typedValue.getOffsetMinutes()).toString());
            }
            case TimestampType timestampType when javaType == LongTimestamp.class -> {
                LongTimestamp typedValue = (LongTimestamp) value;

                return formatStringLiteral(
                        "TIMESTAMP",
                        SqlTimestamp.newInstance(
                                timestampType.getPrecision(),
                                typedValue.getEpochMicros(),
                                typedValue.getPicosOfMicro()).toString());
            }
            case TimestampWithTimeZoneType timestampWithTimeZoneType when javaType == LongTimestampWithTimeZone.class -> {
                LongTimestampWithTimeZone typedValue = (LongTimestampWithTimeZone) value;
                return formatStringLiteral(
                        "TIMESTAMP",
                        SqlTimestampWithTimeZone.newInstance(
                                timestampWithTimeZoneType.getPrecision(),
                                typedValue.getEpochMillis(),
                                typedValue.getPicosOfMilli(),
                                getTimeZoneKey(typedValue.getTimeZoneKey())).toString());
            }
            default -> {}
        }

        throw new UnsupportedOperationException("Unsupported type " + type + " backed by java " + javaType);
    }

    private static String formatStringLiteral(String typeName, String value)
    {
        return typeName + " " + escapeStringLiteral(value);
    }

    private static String escapeStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }
}
