/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel.writer;

import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import net.snowflake.client.internal.core.arrow.ArrowVectorConverter;

import java.sql.Types;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_MAX_TIMESTAMP_PRECISION;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.lang.String.format;

public final class BlockWriterFactory
{
    private static final String MAX_TIMESTAMP_PRECISION_ERROR_MESSAGE = "The max timestamp precision in Snowflake is 9";

    private BlockWriterFactory()
    {
        // static utility class
    }

    public static BlockWriter createWriter(JdbcColumnHandle columnHandle, ArrowVectorConverter converter)
    {
        JdbcTypeHandle typeHandle = columnHandle.getJdbcTypeHandle();
        Type type = columnHandle.getColumnType();
        String typeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (type == BOOLEAN && typeHandle.jdbcType() == Types.BOOLEAN) {
            return new BooleanValueWriter(converter, type);
        }
        if (type == TINYINT && typeHandle.jdbcType() == Types.TINYINT) {
            return new LongValueWriter(converter, type);
        }
        if (type == SMALLINT && typeHandle.jdbcType() == Types.SMALLINT) {
            return new SmallIntValueWriter(converter, type);
        }
        if (type == INTEGER && typeHandle.jdbcType() == Types.INTEGER) {
            return new IntegerValueWriter(converter, type);
        }
        if (type == BIGINT && typeHandle.jdbcType() == Types.BIGINT) {
            return new LongValueWriter(converter, type);
        }
        if (type == REAL && typeHandle.jdbcType() == Types.REAL) {
            return new RealValueWriter(converter, type);
        }
        if (type == DoubleType.DOUBLE && (typeHandle.jdbcType() == Types.DOUBLE || typeHandle.jdbcType() == Types.FLOAT)) {
            return new DoubleValueWriter(converter, type);
        }
        if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.requiredDecimalDigits();
            int precision = typeHandle.requiredColumnSize() + max(-decimalDigits, 0);
            DecimalType decimalType = createDecimalType(precision, max(decimalDigits, 0));
            if (decimalType.isShort()) {
                return new ShortDecimalValueWriter(converter, decimalType, type);
            }
            return new LongDecimalValueWriter(converter, decimalDigits, type);
        }
        if (type == VarcharType.VARCHAR && typeName.equals("VARIANT")) {
            return new VariantValueWriter(converter, type);
        }
        // We intentionally map Snowflake `OBJECT` and `ARRAY` to Trino `Varchar` even though converter may be of `ArrayConverter` or `StructConverter` type,
        // because Snowflake connector maps `OBJECT` and `ARRAY` to `Varchar`.
        if (type == VarcharType.VARCHAR && (typeName.equals("OBJECT") || typeName.equals("ARRAY"))) {
            return new VarcharValueWriter(converter, type);
        }
        if (type == DateType.DATE && typeHandle.jdbcType() == Types.DATE) {
            return new LongValueWriter(converter, type);
        }
        if (typeHandle.jdbcType() == Types.TIME) {
            return new TimeValueWriter(converter, type);
        }
        if (typeHandle.jdbcType() == Types.TIMESTAMP_WITH_TIMEZONE || typeName.equals("TIMESTAMPLTZ")) {
            int precision = typeHandle.requiredDecimalDigits();
            checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION_ERROR_MESSAGE);
            if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION && type.getJavaType() == long.class) {
                return new ShortTimestampWithTimeZoneValueWriter(converter, type);
            }
            return new LongTimestampWithTimeZoneValueWriter(converter, type);
        }
        if (typeHandle.jdbcType() == Types.TIMESTAMP) {
            int precision = typeHandle.requiredDecimalDigits();
            checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION_ERROR_MESSAGE);
            if (precision <= TimestampType.MAX_SHORT_PRECISION && type.getJavaType() == long.class) {
                return new ShortTimestampValueWriter(converter, type, precision);
            }
            return new TimestampValueWriter(converter, type, precision);
        }
        if (typeHandle.jdbcType() == Types.CHAR) {
            int requiredColumnSize = typeHandle.requiredColumnSize();
            if (requiredColumnSize > CharType.MAX_LENGTH) {
                return new VarcharValueWriter(converter, type);
            }
            return new CharValueWriter(converter, type);
        }
        if (type.getJavaType() == Slice.class && typeHandle.jdbcType() == Types.VARCHAR) {
            return new VarcharValueWriter(converter, type);
        }
        if (type == VARBINARY && typeHandle.jdbcType() == Types.BINARY) {
            return new VarbinaryValueWriter(converter, type);
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", type.getJavaType().getSimpleName(), columnHandle));
    }
}
