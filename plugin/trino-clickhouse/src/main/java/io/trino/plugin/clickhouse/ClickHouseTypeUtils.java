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
package io.trino.plugin.clickhouse;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochSecondsAndFraction;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

final class ClickHouseTypeUtils
{
    private ClickHouseTypeUtils() {}

    static void writeScalarElement(BlockBuilder builder, Type type, Object value, RoundingMode roundingMode, TimeZone elementTimeZone)
    {
        Class<?> javaType = type.getJavaType();

        if (javaType == boolean.class) {
            type.writeBoolean(builder, (Boolean) value);
        }
        else if (javaType == long.class) {
            long element = switch (value) {
                case Date date -> date.toLocalDate().toEpochDay();
                case LocalDate localDate -> localDate.toEpochDay();
                // Tuple DateTime(tz) elements are ZonedDateTime; Array elements are java.sql.Timestamp
                case ZonedDateTime _, Timestamp _ -> toShortTimestampValue(type, value, elementTimeZone);
                case Float floatValue -> floatToRawIntBits(floatValue);
                case BigDecimal bigDecimal -> Decimals.encodeShortScaledValue(bigDecimal, ((DecimalType) type).getScale(), roundingMode);
                // ClickHouse JDBC returns signed primitive wrappers for unsigned integer array elements.
                // UInt8→Byte stored in SMALLINT, UInt16→Short stored in INTEGER, UInt32→Integer stored in BIGINT.
                case Byte byteValue when type instanceof SmallintType -> Byte.toUnsignedLong(byteValue);
                case Short shortValue when type instanceof IntegerType -> Short.toUnsignedLong(shortValue);
                case Integer intValue when type instanceof BigintType -> Integer.toUnsignedLong(intValue);
                default -> ((Number) value).longValue();
            };
            type.writeLong(builder, element);
        }
        else if (javaType == double.class) {
            type.writeDouble(builder, ((Number) value).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (value instanceof UUID uuid) {
                type.writeSlice(builder, javaUuidToTrinoUuid(uuid));
            }
            else if (type.getBaseName().equals(IPADDRESS) && value instanceof InetAddress inetAddress) {
                type.writeSlice(builder, castFromVarcharToIpAddress(inetAddress.getHostAddress()));
            }
            else if (value instanceof String stringValue) {
                // The v2 driver always decodes nested String/FixedString as Java String, so invalid UTF-8 bytes are already replaced with U+FFFD here
                type.writeSlice(builder, utf8Slice(stringValue));
            }
            else if (value instanceof BinaryStreamReader.EnumValue enumValue) {
                type.writeSlice(builder, utf8Slice(enumValue.getName()));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported value type for Slice: %s", value.getClass().getName()));
            }
        }
        else if (javaType == Int128.class) {
            DecimalType decimalType = (DecimalType) type;
            verify(!decimalType.isShort(), "The type should be long decimal");
            if (value instanceof BigDecimal bigDecimal) {
                type.writeObject(builder, Decimals.encodeScaledValue(bigDecimal, decimalType.getScale(), roundingMode));
            }
            else if (value instanceof BigInteger bigInteger) {
                type.writeObject(builder, Decimals.encodeScaledValue(new BigDecimal(bigInteger), decimalType.getScale(), roundingMode));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported value type for Int128: %s", value.getClass().getName()));
            }
        }
        else if (type instanceof TimestampType timestampType) {
            type.writeObject(builder, toLongTrinoTimestamp(timestampType, toLocalDateTime(value)));
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            type.writeObject(builder, longTimestampWithTimeZone(toZonedDateTime(value, elementTimeZone)));
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported tuple element type: %s (java type: %s)", type, javaType.getName()));
        }
    }

    private static long toShortTimestampValue(Type type, Object value, TimeZone elementTimeZone)
    {
        if (type instanceof TimestampType timestampType) {
            return toTrinoTimestamp(timestampType, toLocalDateTime(value));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return packShortTimestampWithTimeZone(toZonedDateTime(value, elementTimeZone));
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported type for timestamp value %s: %s", value.getClass().getName(), type));
    }

    static LocalDateTime toLocalDateTime(Object value)
    {
        return switch (value) {
            case Timestamp timestamp -> timestamp.toLocalDateTime();
            case Date date -> date.toLocalDate().atStartOfDay();
            case LocalDate localDate -> localDate.atStartOfDay();
            case LocalDateTime localDateTime -> localDateTime;
            case ZonedDateTime zonedDateTime -> zonedDateTime.toLocalDateTime();
            case null -> throw new TrinoException(JDBC_ERROR, "Unexpected timestamp value type: null");
            default -> throw new TrinoException(JDBC_ERROR, "Unexpected timestamp value type: " + value.getClass().getName());
        };
    }

    private static ZonedDateTime toZonedDateTime(Object value, TimeZone elementTimeZone)
    {
        return switch (value) {
            case ZonedDateTime zonedDateTime -> zonedDateTime;
            case Timestamp timestamp -> timestamp.toLocalDateTime().atZone(timeZoneId(elementTimeZone));
            default -> throw new TrinoException(NOT_SUPPORTED, format("Unsupported value type for timestamp with time zone: %s", value.getClass().getName()));
        };
    }

    static long packShortTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), toTimeZoneKey(zonedDateTime));
    }

    static LongTimestampWithTimeZone longTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return fromEpochSecondsAndFraction(
                zonedDateTime.toEpochSecond(),
                (long) zonedDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                toTimeZoneKey(zonedDateTime));
    }

    private static TimeZoneKey toTimeZoneKey(ZonedDateTime zonedDateTime)
    {
        // clickhouse-jdbc 0.10+ returns a named zone (e.g. Asia/Kathmandu); normalize to fixed offset (e.g. +05:45) to match Trino's representation
        int offsetSeconds = zonedDateTime.getOffset().getTotalSeconds();
        if (offsetSeconds % 60 == 0) {
            return getTimeZoneKeyForOffset(offsetSeconds / 60);
        }
        // Trino has no fixed-offset zone with seconds (e.g. -00:44:30 in Africa/Monrovia before 1972), so keep the named zone
        return getTimeZoneKey(zonedDateTime.getZone().getId());
    }

    private static ZoneId timeZoneId(TimeZone elementTimeZone)
    {
        return requireNonNull(elementTimeZone, "elementTimeZone is null").toZoneId();
    }

    private static Slice castFromVarcharToIpAddress(String ipAddress)
    {
        // copied from IpAddressOperators.castFromVarcharToIpAddress
        byte[] address = InetAddress.ofLiteral(ipAddress).getAddress();

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xFF;
            bytes[11] = (byte) 0xFF;
            arraycopy(address, 0, bytes, 12, 4);
        }
        else if (address.length == 16) {
            bytes = address;
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
        }

        return wrappedBuffer(bytes);
    }

    static Object[] toObjectArray(Object arrayObject)
    {
        if (arrayObject instanceof Object[] array) {
            return array;
        }
        throw new TrinoException(JDBC_ERROR, format("Unexpected type returned for Tuple column: %s", arrayObject.getClass().getName()));
    }

    static Object[] normalizeArrayObject(Object arrayObject)
    {
        if (arrayObject instanceof Object[] array) {
            return array;
        }
        if (arrayObject instanceof List<?> list) {
            return list.toArray();
        }
        if (arrayObject.getClass().isArray()) {
            int length = Array.getLength(arrayObject);
            Object[] result = new Object[length];
            for (int i = 0; i < length; i++) {
                result[i] = Array.get(arrayObject, i);
            }
            return result;
        }
        throw new IllegalArgumentException("Unsupported array object: " + arrayObject.getClass());
    }
}
