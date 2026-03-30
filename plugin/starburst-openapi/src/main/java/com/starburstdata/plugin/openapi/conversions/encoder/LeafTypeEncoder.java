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
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.util.Base64;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
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

    public static LeafTypeEncoder from(LeafIr leafIr)
    {
        return switch (leafIr) {
            case BooleanIr _ -> BOOLEAN_ENCODER;
            case NumberIr(NumberIr.Format format) -> switch (format) {
                case NONE_INTEGER, INT64 -> LONG_ENCODER;
                case NONE_NUMBER, DOUBLE -> DOUBLE_ENCODER;
                case INT32 -> INTEGER_ENCODER;
                case FLOAT -> FLOAT_ENCODER;
            };
            case StringIr(StringIr.Format format) -> switch (format) {
                case NONE -> STRING_ENCODER;
                case UUID -> UUID_ENCODER;
                case BYTE -> BYTE_ENCODER;
            };
        };
    }
}
