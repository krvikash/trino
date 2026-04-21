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

import com.clickhouse.data.value.UnsignedLong;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.clickhouse.ClickHouseClient.UINT64_TYPE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochSecondsAndFraction;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.lang.System.arraycopy;

final class ClickHouseTypeUtils
{
    private ClickHouseTypeUtils() {}

    static void writeScalarElement(BlockBuilder builder, Type type, Object value, RoundingMode roundingMode)
    {
        Class<?> javaType = type.getJavaType();

        if (javaType == boolean.class) {
            type.writeBoolean(builder, (Boolean) value);
        }
        else if (javaType == long.class) {
            long element = switch (value) {
                case LocalDate localDate -> localDate.toEpochDay();
                case LocalDateTime localDateTime -> toTrinoTimestamp((TimestampType) type, localDateTime);
                case OffsetDateTime offsetDateTime -> packDateTimeWithZone(
                        offsetDateTime.toInstant().toEpochMilli(),
                        TimeZoneKey.getTimeZoneKey(offsetDateTime.getOffset().getId()));
                case Float floatValue -> floatToRawIntBits(floatValue);
                case BigDecimal bigDecimal -> Decimals.encodeShortScaledValue(bigDecimal, ((DecimalType) type).getScale(), roundingMode);
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
                // Unlike top-level String/FixedString columns where raw bytes are preserved
                // (e.g. INSERT 0x80 → SELECT returns X'80'), ClickHouse JDBC TupleDeserializer
                // replaces invalid UTF-8 bytes in Tuple String/FixedString elements with U+FFFD
                // before this code runs (e.g. Tuple(value String) INSERT 0x80 → SELECT returns X'efbfbd').
                // Original bytes are unrecoverable.
                type.writeSlice(builder, utf8Slice(stringValue));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported value type for Slice: %s", value.getClass().getName()));
            }
        }
        else if (javaType == Int128.class) {
            DecimalType decimalType = (DecimalType) type;
            verify(!decimalType.isShort(), "The type should be long decimal");
            if (value instanceof UnsignedLong unsignedLong) {
                BigInteger unscaledValue = unsignedLong.bigIntegerValue();
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, UINT64_TYPE.getScale(), new MathContext(UINT64_TYPE.getPrecision()));
                type.writeObject(builder, Decimals.encodeScaledValue(bigDecimal, decimalType.getScale(), roundingMode));
            }
            else if (value instanceof BigDecimal bigDecimal) {
                type.writeObject(builder, Decimals.encodeScaledValue(bigDecimal, decimalType.getScale(), roundingMode));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported value type for Int128: %s", value.getClass().getName()));
            }
        }
        else if (value instanceof OffsetDateTime offsetDateTime && type instanceof TimestampWithTimeZoneType) {
            type.writeObject(builder, fromEpochSecondsAndFraction(
                    offsetDateTime.toEpochSecond(),
                    (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                    TimeZoneKey.getTimeZoneKey(offsetDateTime.getOffset().getId())));
        }
        else if (value instanceof LocalDateTime localDateTime && type instanceof TimestampType timestampType) {
            type.writeObject(builder, toLongTrinoTimestamp(timestampType, localDateTime));
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported tuple element type: %s (java type: %s)", type, javaType.getName()));
        }
    }

    private static Slice castFromVarcharToIpAddress(String ipAddress)
    {
        // copied from IpAddressOperators.castFromVarcharToIpAddress
        byte[] address = InetAddresses.forString(ipAddress).getAddress();

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
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
        if (arrayObject instanceof List<?> list) {
            return list.toArray();
        }
        throw new TrinoException(JDBC_ERROR, format("Unexpected type returned for Tuple column: %s", arrayObject.getClass().getName()));
    }
}
