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
package io.trino.plugin.hive.coercions;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.plugin.hive.parquet.ParquetTypeTranslator;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochSecondsAndFraction;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TypeUtils.blockToNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimestampCoercer
{
    private static final TimeZone UTC_TZ = TimeZone.getTimeZone(ZoneId.of("UTC"));
    private static final LocalDate GREGORIAN_START_DATE = LocalDate.of(1582, 10, 15);
    private static final LocalDate JULIAN_END_DATE = LocalDate.of(1582, 10, 4);
    private static final LocalDateTime GREGORIAN_START_DATETIME = LocalDateTime.of(GREGORIAN_START_DATE, LocalTime.MIDNIGHT);
    private static final LocalDateTime JULIAN_END_DATETIME = LocalDateTime.of(JULIAN_END_DATE, LocalTime.of(23, 59, 59, 999999999));
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter LONG_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS");

    @Test
    public void testTimestampToDate()
    {
        // before epoch
        assertTimestampToDateCoercion("1900-01-01T00:00:00.000", "1900-01-01");
        assertTimestampToDateCoercion("1958-01-01T13:18:03.123", "1958-01-01");
        // after epoch
        assertTimestampToDateCoercion("2019-03-18T10:01:17.987", "2019-03-18");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.000", "2021-12-31");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.999", "2021-12-31");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.999999", "2021-12-31");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.999999999", "2021-12-31");
        // time doubled in JVM zone
        assertTimestampToDateCoercion("2018-10-28T01:33:17.456", "2018-10-28");
        // epoch
        assertTimestampToDateCoercion("1970-01-01T00:00:00.000", "1970-01-01");
        // time gap in JVM zone
        assertTimestampToDateCoercion("1970-01-01T00:13:42.000", "1970-01-01");
        assertTimestampToDateCoercion("2018-04-01T02:13:55.123", "2018-04-01");
        // time gap in Vilnius
        assertTimestampToDateCoercion("2018-03-25T03:17:17.000", "2018-03-25");
        // time gap in Kathmandu
        assertTimestampToDateCoercion("1986-01-01T00:13:07.000", "1986-01-01");
        // before epoch with second fraction
        assertTimestampToDateCoercion("1969-12-31T23:59:59.123456", "1969-12-31");
    }

    @Test
    public void testHistoricalLongTimestampToDate()
    {
        assertThatThrownBy(() -> assertTimestampToDateCoercion("1899-12-31T23:59:59.999999999", "1899-12-31"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testTimestampToVarchar()
    {
        testTimestampToVarchar("1900-01-01T00:00:00.000", "1900-01-01 00:00:00");
        testTimestampToVarchar("1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123");
        // after epoch
        testTimestampToVarchar("2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987");
        // time doubled in JVM zone
        testTimestampToVarchar("2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456");
        // time doubled in JVM zone
        testTimestampToVarchar("2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333");
        // epoch
        testTimestampToVarchar("1970-01-01T00:00:00.000", "1970-01-01 00:00:00");
        // time gap in JVM zone
        testTimestampToVarchar("1970-01-01T00:13:42.000", "1970-01-01 00:13:42");
        testTimestampToVarchar("2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123");
        // time gap in Vilnius
        testTimestampToVarchar("2018-03-25T03:17:17.000", "2018-03-25 03:17:17");
        // time gap in Kathmandu
        testTimestampToVarchar("1986-01-01T00:13:07.000", "1986-01-01 00:13:07");
        // before epoch with second fraction
        testTimestampToVarchar("1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456");
    }

    private static void testTimestampToVarchar(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), createUnboundedVarcharType(), hiveTimestampValue);
    }

    @Test
    public void testVarcharToShortTimestamp()
    {
        testVarcharToShortTimestamp("1900-01-01T00:00:00.000", "1900-01-01 00:00:00");
        testVarcharToShortTimestamp("1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123");
        // after epoch
        testVarcharToShortTimestamp("2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987");
        // time doubled in JVM zone
        testVarcharToShortTimestamp("2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456");
        // time doubled in JVM zone
        testVarcharToShortTimestamp("2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333");
        // epoch
        testVarcharToShortTimestamp("1970-01-01T00:00:00.000", "1970-01-01 00:00:00");
        // time gap in JVM zone
        testVarcharToShortTimestamp("1970-01-01T00:13:42.000", "1970-01-01 00:13:42");
        testVarcharToShortTimestamp("2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123");
        // time gap in Vilnius
        testVarcharToShortTimestamp("2018-03-25T03:17:17.000", "2018-03-25 03:17:17");
        // time gap in Kathmandu
        testVarcharToShortTimestamp("1986-01-01T00:13:07.000", "1986-01-01 00:13:07");
        // before epoch with second fraction
        testVarcharToShortTimestamp("1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456");
    }

    private static void testVarcharToShortTimestamp(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_MICROS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertVarcharToShortTimestampCoercions(createUnboundedVarcharType(), utf8Slice(hiveTimestampValue), TIMESTAMP_MICROS, timestamp.getEpochMicros());
    }

    @Test
    public void testVarcharToLongTimestamp()
    {
        testVarcharToLongTimestamp("1900-01-01T00:00:00.000", "1900-01-01 00:00:00");
        testVarcharToLongTimestamp("1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123");
        // after epoch
        testVarcharToLongTimestamp("2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987");
        // time doubled in JVM zone
        testVarcharToLongTimestamp("2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456");
        // time doubled in JVM zone
        testVarcharToLongTimestamp("2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333");
        // epoch
        testVarcharToLongTimestamp("1970-01-01T00:00:00.000", "1970-01-01 00:00:00");
        // time gap in JVM zone
        testVarcharToLongTimestamp("1970-01-01T00:13:42.000", "1970-01-01 00:13:42");
        testVarcharToLongTimestamp("2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123");
        // time gap in Vilnius
        testVarcharToLongTimestamp("2018-03-25T03:17:17.000", "2018-03-25 03:17:17");
        // time gap in Kathmandu
        testVarcharToLongTimestamp("1986-01-01T00:13:07.000", "1986-01-01 00:13:07");
        // before epoch with second fraction
        testVarcharToLongTimestamp("1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456");
    }

    private static void testVarcharToLongTimestamp(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertVarcharToLongTimestampCoercions(createUnboundedVarcharType(), utf8Slice(hiveTimestampValue), TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()));
    }

    @Test
    public void testTimestampToSmallerVarchar()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("2023-04-11T05:16:12.345678876");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        LongTimestamp longTimestamp = new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros());
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(1), "2");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(2), "20");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(3), "202");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(4), "2023");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(5), "2023-");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(6), "2023-0");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(7), "2023-04");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(8), "2023-04-");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(9), "2023-04-1");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(10), "2023-04-11");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(11), "2023-04-11 ");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(12), "2023-04-11 0");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(13), "2023-04-11 05");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(14), "2023-04-11 05:");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(15), "2023-04-11 05:1");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(16), "2023-04-11 05:16");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(17), "2023-04-11 05:16:");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(18), "2023-04-11 05:16:1");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(19), "2023-04-11 05:16:12");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(20), "2023-04-11 05:16:12.");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(21), "2023-04-11 05:16:12.3");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(22), "2023-04-11 05:16:12.34");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(23), "2023-04-11 05:16:12.345");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(24), "2023-04-11 05:16:12.3456");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(25), "2023-04-11 05:16:12.34567");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(26), "2023-04-11 05:16:12.345678");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(27), "2023-04-11 05:16:12.3456788");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(28), "2023-04-11 05:16:12.34567887");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(29), "2023-04-11 05:16:12.345678876");
    }

    @Test
    public void testHistoricalLongTimestampToVarchar()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("1899-12-31T23:59:59.999999999");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertThatThrownBy(() ->
                assertLongTimestampToVarcharCoercions(
                        TIMESTAMP_PICOS,
                        new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()),
                        createUnboundedVarcharType(),
                        "1899-12-31 23:59:59.999999999"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testInvalidVarcharToShortTimestamp()
    {
        testInvalidVarcharToShortTimestamp("Invalid timestamp"); // Invalid string
        testInvalidVarcharToShortTimestamp("2022"); // Partial timestamp value
        testInvalidVarcharToShortTimestamp("2001-04-01T00:13:42.000"); // ISOFormat date
        testInvalidVarcharToShortTimestamp("2001-14-01 00:13:42.000"); // Invalid month
        testInvalidVarcharToShortTimestamp("2001-01-32 00:13:42.000"); // Invalid day
        testInvalidVarcharToShortTimestamp("2001-04-01 23:59:60.000"); // Invalid second
        testInvalidVarcharToShortTimestamp("2001-04-01 23:60:01.000"); // Invalid minute
        testInvalidVarcharToShortTimestamp("2001-04-01 27:01:01.000"); // Invalid hour
    }

    private static void testInvalidVarcharToShortTimestamp(String invalidValue)
    {
        assertVarcharToShortTimestampCoercions(createUnboundedVarcharType(), utf8Slice(invalidValue), TIMESTAMP_MICROS, null);
    }

    @Test
    public void testInvalidVarcharLongTimestamp()
    {
        testInvalidVarcharLongTimestamp("Invalid timestamp"); // Invalid string
        testInvalidVarcharLongTimestamp("2022"); // Partial timestamp value
        testInvalidVarcharLongTimestamp("2001-04-01T00:13:42.000"); // ISOFormat date
        testInvalidVarcharLongTimestamp("2001-14-01 00:13:42.000"); // Invalid month
        testInvalidVarcharLongTimestamp("2001-01-32 00:13:42.000"); // Invalid day
        testInvalidVarcharLongTimestamp("2001-04-01 23:59:60.000"); // Invalid second
        testInvalidVarcharLongTimestamp("2001-04-01 23:60:01.000"); // Invalid minute
        testInvalidVarcharLongTimestamp("2001-04-01 27:01:01.000"); // Invalid hour
    }

    private static void testInvalidVarcharLongTimestamp(String invalidValue)
    {
        assertVarcharToLongTimestampCoercions(createUnboundedVarcharType(), utf8Slice(invalidValue), TIMESTAMP_MICROS, null);
    }

    @Test
    public void testHistoricalVarcharToShortTimestamp()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("1899-12-31T23:59:59.999999");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_MICROS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertThatThrownBy(() ->
                assertVarcharToShortTimestampCoercions(
                        createUnboundedVarcharType(),
                        utf8Slice("1899-12-31 23:59:59.999999"),
                        TIMESTAMP_MICROS,
                        timestamp.getEpochMicros()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testHistoricalVarcharToLongTimestamp()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("1899-12-31T23:59:59.999999");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertThatThrownBy(() -> assertVarcharToShortTimestampCoercions(
                createUnboundedVarcharType(),
                utf8Slice("1899-12-31 23:59:59.999999"),
                TIMESTAMP_PICOS,
                timestamp.getEpochMicros()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testLegacyTimestampCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("-9999-12-31 23:59:59.999", "-9999-12-31 23:59:59.999");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("-5555-01-01 23:59:59.999", "-5555-01-01 23:59:59.999");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("-4713-01-01 00:00:00.000", "-4713-01-01 00:00:00.000");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("-1001-01-01 00:00:00.123", "-1001-01-01 00:00:00.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("-0001-01-01 00:00:00.123", "-0001-01-01 00:00:00.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("0001-01-01 00:00:00.000", "0001-01-01 00:00:00.000");  // 1st  year of era
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("0001-01-01 15:00:00.000", "0001-01-01 15:00:00.000");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("0001-01-01 15:15:00.000", "0001-01-01 15:15:00.000");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("0001-01-01 15:15:15.000", "0001-01-01 15:15:15.000");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("0001-01-01 15:15:15.123", "0001-01-01 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1000-01-01 15:15:15.123", "1000-01-01 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-03 23:59:59.999", "1582-10-03 23:59:59.999");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-04 00:00:00.000", "1582-10-04 00:00:00.000"); // just before the switch to Gregorian calendar
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-04 15:15:15.123", "1582-10-04 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-04 23:59:59.999", "1582-10-04 23:59:59.999");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-15 00:00:00.000", "1582-10-15 00:00:00.000"); // first day after the switch to Gregorian calendar
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-15 15:15:15.123", "1582-10-15 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-15 23:59:59.999", "1582-10-15 23:59:59.999");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1582-10-16 00:00:00.000", "1582-10-16 00:00:00.000");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1788-09-10 15:15:15.123", "1788-09-10 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1888-12-31 15:15:15.123", "1888-12-31 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1969-12-31 15:15:15.123", "1969-12-31 15:15:15.123"); // just before the epoch
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1970-01-01 00:00:00.001", "1970-01-01 00:00:00.001"); // epoch day
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1970-01-01 15:15:15.123", "1970-01-01 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("2024-03-30 15:15:15.123", "2024-03-30 15:15:15.123");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("5000-11-11 23:59:59.999", "5000-11-11 23:59:59.999");
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("9999-12-31 23:59:59.999", "9999-12-31 23:59:59.999");

        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1000-02-29 01:02:03.123", "1000-02-28 01:02:03.123"); // legacy leap year
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("1600-02-29 11:12:13.654", "1600-02-29 11:12:13.654"); // Gregorian leap year
        assertReadingWithCoercionHybridToProlepticLegacyTimestamp("2000-02-29 00:00:00.999", "2000-02-29 00:00:00.999"); // Gregorian leap year

        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("-9999-12-31 23:59:59.999", "-9999-10-15 23:59:59.999");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("-5555-01-01 23:59:59.999", "-5556-11-18 23:59:59.999");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("-4713-01-01 00:00:00.000", "-4714-11-24 00:00:00.000");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("-1001-01-01 00:00:00.123", "-1002-12-22 00:00:00.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("-0001-01-01 00:00:00.123", "-0002-12-30 00:00:00.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("0001-01-01 15:15:15.123", "0000-12-30 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1000-01-01 15:15:15.123", "1000-01-06 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1582-10-04 15:15:15.123", "1582-10-14 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1582-10-04 23:59:59.999", "1582-10-14 23:59:59.999");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1582-10-15 00:00:00.000", "1582-10-15 00:00:00.000");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1582-10-15 15:15:15.123", "1582-10-15 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1582-10-15 23:59:59.999", "1582-10-15 23:59:59.999");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1582-10-16 00:00:00.000", "1582-10-16 00:00:00.000");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1788-09-10 15:15:15.123", "1788-09-10 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1888-12-31 15:15:15.123", "1888-12-31 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1969-12-31 15:15:15.123", "1969-12-31 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1970-01-01 15:15:15.123", "1970-01-01 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("2024-03-30 15:15:15.123", "2024-03-30 15:15:15.123");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("5000-11-11 23:59:59.999", "5000-11-11 23:59:59.999");
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("9999-12-31 23:59:59.999", "9999-12-31 23:59:59.999");

        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1000-02-29 01:02:03.123", "1000-03-05 01:02:03.123"); // legacy leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("1600-02-29 11:12:13.654", "1600-02-29 11:12:13.654"); // Gregorian leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp("2000-02-29 00:00:00.999", "2000-02-29 00:00:00.999"); // Gregorian leap year
    }

    @Test
    public void testLegacyLongTimestampCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("-9999-12-31 23:59:59.999999999", "-9999-12-31T23:59:59.999999999");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("-5555-01-01 23:59:59.999999999", "-5555-01-01T23:59:59.999999999");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("-4713-01-01 00:00:00.000000000", "-4713-01-01T00:00:00.000000000");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("-1001-01-01 00:00:00.123456789", "-1001-01-01T00:00:00.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("-0001-01-01 00:00:00.123456789", "-0001-01-01T00:00:00.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("0001-01-01 15:15:15.123456789", "0001-01-01T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1000-01-01 15:15:15.123456789", "1000-01-01T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1582-10-04 15:15:15.123456789", "1582-10-04T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1582-10-04 23:59:59.999999999", "1582-10-04T23:59:59.999999999");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1582-10-15 00:00:00.000000000", "1582-10-15T00:00:00.000000000");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1582-10-15 15:15:15.123456789", "1582-10-15T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1582-10-15 23:59:59.999999999", "1582-10-15T23:59:59.999999999");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1582-10-16 00:00:00.000000000", "1582-10-16T00:00:00.000000000");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1788-09-10 15:15:15.123456789", "1788-09-10T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1888-12-31 15:15:15.123456789", "1888-12-31T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1969-12-31 15:15:15.123456789", "1969-12-31T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1970-01-01 15:15:15.123456789", "1970-01-01T15:15:15.123456789");
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("1970-01-01 00:00:00.000000001", "1970-01-01T00:00:00.000000001"); // epoch day
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp("2024-03-30 15:15:15.123456789", "2024-03-30T15:15:15.123456789");

        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("-9999-12-31 23:59:59.999999999", "-9999-10-15T23:59:59.999999999");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("-5555-01-01 23:59:59.999999999", "-5556-11-18T23:59:59.999999999");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("-4713-01-01 00:00:00.000000000", "-4714-11-24T00:00:00.000000000"); // Julian calendar start
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("-1001-01-01 00:00:00.123456789", "-1002-12-22T00:00:00.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("-0001-01-01 00:00:00.123456789", "-0002-12-30T00:00:00.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("0001-01-01 15:15:15.123456789", "0000-12-30T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1000-01-01 15:15:15.123456789", "1000-01-06T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1582-10-04 15:15:15.123456789", "1582-10-14T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1582-10-04 23:59:59.999999999", "1582-10-14T23:59:59.999999999");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1582-10-15 00:00:00.000000000", "1582-10-15T00:00:00.000000000");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1582-10-15 15:15:15.123456789", "1582-10-15T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1582-10-15 23:59:59.999999999", "1582-10-15T23:59:59.999999999");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1582-10-16 00:00:00.000000000", "1582-10-16T00:00:00.000000000");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1788-09-10 15:15:15.123456789", "1788-09-10T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1888-12-31 15:15:15.123456789", "1888-12-31T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1969-12-31 15:15:15.123456789", "1969-12-31T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("1970-01-01 15:15:15.123456789", "1970-01-01T15:15:15.123456789");
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp("2024-03-30 15:15:15.123456789", "2024-03-30T15:15:15.123456789");
    }

    @Test
    public void testLegacyTimestampWithTimeZoneCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        ImmutableSet.of(
                        "UTC",
                        "Europe/Warsaw",
                        "America/New_York",
                        "America/Los_Angeles",
                        "America/Bahia_Banderas",
                        "Asia/Kolkata",
                        "Asia/Hong_Kong",
                        "Africa/Dakar",
                        "Antarctica/Vostok",
                        "Asia/Kathmandu")
                .stream()
                .map(ZoneId::of)
                .forEach(this::testLegacyTimestampWithTimeZoneCoercionFromHybridCalendarToProlepticGregorianCalendar);
    }

    private void testLegacyTimestampWithTimeZoneCoercionFromHybridCalendarToProlepticGregorianCalendar(ZoneId zoneId)
    {
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("-9999-12-31 23:59:59.999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("-5555-01-01 23:59:59.999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("-4713-01-01 00:00:00.000", zoneId); // Julian day zero
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("-1001-01-01 00:00:00.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("-0001-01-01 00:00:00.123", zoneId);

        // Year 1 CE and early AD dates
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0001-01-01 00:00:00.000", zoneId);  // 1st  year of era
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0001-01-01 15:00:00.000", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0001-01-01 15:15:00.000", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0001-01-01 15:15:15.000", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0001-01-01 15:15:15.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0100-03-20 14:30:00.250", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("0500-08-10 08:15:45.678", zoneId);

        // Medieval dates before Gregorian calendar switch
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1000-01-01 15:15:15.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1200-12-25 00:00:00.001", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1400-09-15 16:45:30.999", zoneId);

        // Around the Julian-Gregorian calendar switch (October 1582)
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-01-01 00:00:00.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-03 23:59:59.999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-04 00:00:00.000", zoneId); // just before the switch to Gregorian calendar
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-04 15:15:15.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-04 11:59:59.999", zoneId); // last day of Julian calendar
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-15 12:00:00.000", zoneId); // first day after the switch to Gregorian calendar

        // Post-Gregorian dates (no rebasing needed but test coercer still works)
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-15 15:15:15.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-15 23:59:59.999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-16 00:00:00.000", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1788-09-10 15:15:15.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1888-12-31 15:15:15.123", zoneId);

        // Epoch and modern dates
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1969-12-31 15:15:15.123", zoneId); // just before the epoch
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1970-01-01 00:00:00.001", zoneId); // epoch day
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1970-01-01 15:15:15.123", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("2000-01-01 00:00:00.000", zoneId); // Y2K
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("2024-03-30 15:15:15.123", zoneId);

        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1000-02-29 01:02:03.123", zoneId); // legacy leap year
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("1600-02-29 11:12:13.654", zoneId); // Gregorian leap year
        assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone("2000-02-29 00:00:00.999", zoneId); // Gregorian leap year
    }

    @Test
    public void testLegacyTimestampWithTimeZoneNoCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        ZoneId zoneId = ZoneId.of("Europe/London");

        // BC dates show significant drift
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("-9999-12-31 23:59:59.999", "-9999-10-15 23:58:44.999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("-5555-01-01 23:59:59.999", "-5556-11-18 23:58:44.999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("-4713-01-01 00:00:00.000", "-4714-11-23 23:58:45.000", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("-1001-01-01 00:00:00.123", "-1002-12-21 23:58:45.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("-0001-01-01 00:00:00.123", "-0002-12-29 23:58:45.123", zoneId);

        // 1Y CE
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("0001-01-01 15:15:15.123", "0000-12-30 15:14:00.123", zoneId);

        // Medieval dates
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1000-01-01 15:15:15.123", "1000-01-06 15:14:00.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1000-06-20 10:20:30.456", "1000-06-26 10:19:15.456", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1400-09-15 16:45:30.999", "1400-09-24 16:44:15.999", zoneId);

        // Around calendar switch
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-04 15:15:15.123", "1582-10-14 15:14:00.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-04 23:59:59.999", "1582-10-14 23:58:44.999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-15 00:00:00.000", "1582-10-14 23:58:45.000", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-15 15:15:15.123", "1582-10-15 15:15:15.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-15 23:59:59.999", "1582-10-15 23:59:59.999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1582-10-16 00:00:00.000", "1582-10-16 00:00:00.000", zoneId);

        // Post-Gregorian dates unchanged
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1788-09-10 15:15:15.123", "1788-09-10 15:15:15.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1888-12-31 15:15:15.123", "1888-12-31 15:15:15.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1969-12-31 15:15:15.123", "1969-12-31 15:15:15.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1970-01-01 15:15:15.123", "1970-01-01 15:15:15.123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("2024-03-30 15:15:15.123", "2024-03-30 15:15:15.123", zoneId);

        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1000-02-29 01:02:03.123", "1000-03-05 01:00:48.123", zoneId); // legacy leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("1600-02-29 11:12:13.654", "1600-02-29 11:12:13.654", zoneId); // Gregorian leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone("2000-02-29 00:00:00.999", "2000-02-29 00:00:00.999", zoneId); // Gregorian leap year
    }

    @Test
    public void testLegacyLongTimestampWithTimeZoneCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        ImmutableSet.of(
                        "UTC",
                        "Europe/Warsaw",
                        "America/New_York",
                        "America/Los_Angeles",
                        "America/Bahia_Banderas",
                        "Asia/Kolkata",
                        "Asia/Hong_Kong",
                        "Africa/Dakar",
                        "Antarctica/Vostok",
                        "Asia/Kathmandu")
                .stream()
                .map(ZoneId::of)
                .forEach(this::testLegacyLongTimestampWithTimeZoneCoercionFromHybridCalendarToProlepticGregorianCalendar);
    }

    private void testLegacyLongTimestampWithTimeZoneCoercionFromHybridCalendarToProlepticGregorianCalendar(ZoneId zoneId)
    {
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-9999-12-31 23:59:59.999999999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-5555-01-01 23:59:59.999999999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-1001-01-01 00:00:00.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-0001-01-01 00:00:00.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("0001-01-01 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1000-01-01 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-04 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-04 11:59:59.999999999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-15 12:00:00.000000000", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-15 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-15 23:59:59.999999999", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-16 00:00:00.000000000", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1788-09-10 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1888-12-31 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1969-12-31 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1970-01-01 15:15:15.123456789", zoneId);
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1970-01-01 00:00:00.000000001", zoneId); // epoch day
        assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("2024-03-30 15:15:15.123456789", zoneId);
    }

    @Test
    public void testLegacyLongTimestampWithTimeZoneInHybridCalendarNoCoercion()
    {
        ZoneId zoneId = ZoneId.of("Europe/London");

        // BC dates show significant drift
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-9999-12-31 23:59:59.999999999", "-9999-10-15 23:58:44.999999999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-5555-01-01 23:59:59.999999999", "-5556-11-18 23:58:44.999999999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-4713-01-01 00:00:00.000000000", "-4714-11-23 23:58:45.000000000", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-1001-01-01 00:00:00.123456789", "-1002-12-21 23:58:45.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("-0001-01-01 00:00:00.123456789", "-0002-12-29 23:58:45.123456789", zoneId);

        // 1Y CE
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("0001-01-01 15:15:15.123456789", "0000-12-30 15:14:00.123456789", zoneId);

        // Medieval dates
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1000-01-01 15:15:15.123456789", "1000-01-06 15:14:00.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1000-06-20 10:20:30.456789123", "1000-06-26 10:19:15.456789123", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1400-09-15 16:45:30.999999999", "1400-09-24 16:44:15.999999999", zoneId);

        // Around calendar switch
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-04 15:15:15.123456789", "1582-10-14 15:14:00.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-04 23:59:59.999999999", "1582-10-14 23:58:44.999999999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-15 00:00:00.000000000", "1582-10-14 23:58:45.000000000", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-15 15:15:15.123456789", "1582-10-15 15:15:15.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-15 23:59:59.999999999", "1582-10-15 23:59:59.999999999", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1582-10-16 00:00:00.000000000", "1582-10-16 00:00:00.000000000", zoneId);

        // Post-Gregorian dates unchanged
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1788-09-10 15:15:15.123456789", "1788-09-10 15:15:15.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1888-12-31 15:15:15.123456789", "1888-12-31 15:15:15.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1969-12-31 15:15:15.123456789", "1969-12-31 15:15:15.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1970-01-01 15:15:15.123456789", "1970-01-01 15:15:15.123456789", zoneId);
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("2024-03-30 15:15:15.123456789", "2024-03-30 15:15:15.123456789", zoneId);

        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1000-02-29 01:02:03.123456789", "1000-03-05 01:00:48.123456789", zoneId); // legacy leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("1600-02-29 11:12:13.654123456", "1600-02-29 11:12:13.654123456", zoneId); // Gregorian leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone("2000-02-29 00:00:00.999999999", "2000-02-29 00:00:00.999999999", zoneId); // Gregorian leap year
    }

    private void assertReadingWithCoercionHybridToProlepticLegacyTimestamp(String writtenTimestamp, String actualReadDate)
    {
        assertReadingHybridToProlepticLegacyTimestamp(true, writtenTimestamp, actualReadDate);
    }

    private void assertReadingWithoutCoercionHybridToProlepticLegacyTimestamp(String writtenTimestamp, String actualReadDate)
    {
        assertReadingHybridToProlepticLegacyTimestamp(false, writtenTimestamp, actualReadDate);
    }

    private void assertReadingHybridToProlepticLegacyTimestamp(boolean convertTimestampToProleptic, String writtenTimestamp, String actualReadTimestamp)
    {
        ZonedDateTime insertZdt = fromDateTimeString(writtenTimestamp, UTC);
        long hybridMillis = toHybridMillis(insertZdt);
        Block writtenBlock = writeNativeValue(TIMESTAMP_MICROS, multiplyExact(hybridMillis, MICROSECONDS_PER_MILLISECOND));

        Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = ParquetTypeTranslator.createCoercer(INT96, null, TIMESTAMP_MICROS, new ParquetTypeTranslator.CoercionContext(false, false, convertTimestampToProleptic, UTC_TZ));
        Block readBlock = coercer.isPresent() ? coercer.get().apply(writtenBlock) : writtenBlock;

        long actualMillis = floorDiv((long) blockToNativeValue(TIMESTAMP_MICROS, readBlock), MICROSECONDS_PER_MILLISECOND);

        ZonedDateTime expectedZonedDateTime = fromDateTimeString(actualReadTimestamp, UTC);
        long expectedMillis = expectedZonedDateTime.toInstant().toEpochMilli();

        assertThat(actualMillis)
                .withFailMessage(() -> "Expected timestamp to be %s, but was %s".formatted(expectedZonedDateTime, fromProlepticMillis(actualMillis, UTC_TZ.toZoneId())))
                .isEqualTo(expectedMillis);
    }

    private void assertReadingWithCoercionHybridToProlepticLegacyTimestampWithTimeZone(String timestamp, ZoneId zoneId)
    {
        assertReadingHybridToProlepticLegacyTimestampWithTimeZone(true, timestamp, timestamp, zoneId);
    }

    private void assertReadingWithoutCoercionHybridToProlepticLegacyTimestampWithTimeZone(String writtenTimestamp, String actualReadTimestamp, ZoneId zoneId)
    {
        assertReadingHybridToProlepticLegacyTimestampWithTimeZone(false, writtenTimestamp, actualReadTimestamp, zoneId);
    }

    private void assertReadingHybridToProlepticLegacyTimestampWithTimeZone(boolean convertTimestampToProleptic, String timestamp, String actualReadTimestamp, ZoneId zoneId)
    {
        TimeZone timezone = TimeZone.getTimeZone(zoneId);
        ZonedDateTime zonedDateTime = fromDateTimeString(timestamp, timezone.toZoneId());
        long hybridMillis = toHybridMillis(zonedDateTime);
        long hybridMillisWithTimeZone = DateTimeEncoding.packDateTimeWithZone(hybridMillis, UTC_KEY);
        Block writtenBlock = writeNativeValue(TIMESTAMP_TZ_MILLIS, hybridMillisWithTimeZone);

        Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = ParquetTypeTranslator.createCoercer(
                INT96,
                null,
                TIMESTAMP_TZ_MILLIS,
                new ParquetTypeTranslator.CoercionContext(false, false, convertTimestampToProleptic, timezone));
        Block readBlock = coercer.isPresent() ? coercer.get().apply(writtenBlock) : writtenBlock;

        long actualMillisWithTimeZone = (long) blockToNativeValue(TIMESTAMP_TZ_MILLIS, readBlock);
        long actualMillis = unpackMillisUtc(actualMillisWithTimeZone);

        ZonedDateTime expectedZonedDateTime = fromDateTimeString(actualReadTimestamp, zoneId);
        long expectedMillis = expectedZonedDateTime.toInstant().toEpochMilli();

        assertThat(actualMillis)
                .withFailMessage(() -> "Expected timestamp to be %s, but was %s".formatted(expectedZonedDateTime, fromProlepticMillis(actualMillis, zoneId)))
                .isEqualTo(expectedMillis);
    }

    private void assertReadingWithCoercionHybridToProlepticLegacyLongTimestamp(String writtenTimestamp, String actualReadDate)
    {
        assertReadingHybridToProlepticLegacyLongTimestamp(true, writtenTimestamp, actualReadDate);
    }

    private void assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestamp(String writtenTimestamp, String actualReadDate)
    {
        assertReadingHybridToProlepticLegacyLongTimestamp(false, writtenTimestamp, actualReadDate);
    }

    private void assertReadingHybridToProlepticLegacyLongTimestamp(boolean convertTimestampToProleptic, String writtenTimestamp, String actualReadTimestamp)
    {
        LongTimestamp givenTimestamp = fromHybridTimestamp(writtenTimestamp);
        Block writtenBlock = writeNativeValue(TIMESTAMP_NANOS, givenTimestamp);

        Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = ParquetTypeTranslator.createCoercer(INT96, null, TIMESTAMP_NANOS, new ParquetTypeTranslator.CoercionContext(false, false, convertTimestampToProleptic, UTC_TZ));
        Block readBlock = coercer.isPresent() ? coercer.orElseThrow().apply(writtenBlock) : writtenBlock;
        LongTimestamp actualLongTimestamp = (LongTimestamp) blockToNativeValue(TIMESTAMP_NANOS, readBlock);

        LongTimestamp expectedLongTimestamp = fromProlepticGregorianTimestamp(actualReadTimestamp);

        assertThat(actualLongTimestamp).isEqualTo(expectedLongTimestamp);
    }

    private void assertReadingWithCoercionHybridToProlepticLegacyLongTimestampWithTimeZone(String writtenTimestamp, ZoneId zoneId)
    {
        assertReadingHybridToProlepticLegacyLongTimestampWithTimeZone(true, writtenTimestamp, writtenTimestamp, zoneId);
    }

    private void assertReadingWithoutCoercionHybridToProlepticLegacyLongTimestampWithTimeZone(String writtenTimestamp, String actualReadDate, ZoneId zoneId)
    {
        assertReadingHybridToProlepticLegacyLongTimestampWithTimeZone(false, writtenTimestamp, actualReadDate, zoneId);
    }

    private void assertReadingHybridToProlepticLegacyLongTimestampWithTimeZone(boolean convertTimestampToProleptic, String writtenTimestamp, String actualReadTimestamp, ZoneId zoneId)
    {
        TimeZone timeZone = TimeZone.getTimeZone(zoneId);
        TimeZoneKey givenTimeZoneKey = TimeZoneKey.getTimeZoneKey(zoneId.toString());

        LongTimestampWithTimeZone givenTimestamp = fromHybridTimestamp(writtenTimestamp, givenTimeZoneKey);
        Block writtenBlock = writeNativeValue(TIMESTAMP_TZ_NANOS, givenTimestamp);

        Optional<TypeCoercer<? extends Type, ? extends Type>> coercer =
                ParquetTypeTranslator.createCoercer(
                        INT96,
                        null,
                        TIMESTAMP_TZ_NANOS,
                        new ParquetTypeTranslator.CoercionContext(false, false, convertTimestampToProleptic, timeZone));
        Block readBlock = coercer.isPresent() ? coercer.orElseThrow().apply(writtenBlock) : writtenBlock;
        LongTimestampWithTimeZone actualLongTimestamp = (LongTimestampWithTimeZone) blockToNativeValue(TIMESTAMP_TZ_NANOS, readBlock);

        LongTimestampWithTimeZone expectedLongTimestamp = fromProlepticGregorianTimestamp(actualReadTimestamp, givenTimeZoneKey, givenTimeZoneKey.getZoneId());

        assertThat(actualLongTimestamp)
                .withFailMessage(() -> "Expected timestamp to be %s, but was %s".formatted(expectedLongTimestamp, actualLongTimestamp))
                .isEqualTo(expectedLongTimestamp);
    }

    private static LongTimestamp fromProlepticGregorianTimestamp(String actualReadTimestamp)
    {
        Instant expectedInstant = toInstantInProlepticGregorian(actualReadTimestamp);
        long epochMicros = multiplyExact(expectedInstant.getEpochSecond(), MICROSECONDS_PER_SECOND) + floorDiv(expectedInstant.getNano(), NANOSECONDS_PER_MICROSECOND);
        int picosOfMicros = multiplyExact(floorMod(expectedInstant.getNano(), NANOSECONDS_PER_MICROSECOND), PICOSECONDS_PER_NANOSECOND);
        return new LongTimestamp(epochMicros, picosOfMicros);
    }

    private static LongTimestampWithTimeZone fromProlepticGregorianTimestamp(String actualReadTimestamp, TimeZoneKey timeZoneKey, ZoneId zoneId)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(actualReadTimestamp, LONG_DATE_TIME_FORMATTER);
        Instant instant = localDateTime.atZone(zoneId).toInstant();
        return fromEpochSecondsAndFraction(instant.getEpochSecond(), (long) instant.getNano() * PICOSECONDS_PER_NANOSECOND, timeZoneKey);
    }

    private static LongTimestamp fromHybridTimestamp(String writtenTimestamp)
    {
        ZonedDateTime zonedDateTime = fromLongDateTimeString(writtenTimestamp, UTC);
        int nanosOfMilli = floorMod(zonedDateTime.getNano(), NANOSECONDS_PER_MILLISECOND);
        long julianMillis = toHybridMillis(zonedDateTime);
        long epochMicros = multiplyExact(julianMillis, MICROSECONDS_PER_MILLISECOND) + floorDiv(nanosOfMilli, NANOSECONDS_PER_MICROSECOND);
        int picosOfMicro = multiplyExact(floorMod(nanosOfMilli, NANOSECONDS_PER_MICROSECOND), PICOSECONDS_PER_NANOSECOND);
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    private static LongTimestampWithTimeZone fromHybridTimestamp(String writtenTimestamp, TimeZoneKey timeZoneKey)
    {
        ZonedDateTime zonedDateTime = fromLongDateTimeString(writtenTimestamp, timeZoneKey.getZoneId());
        long julianMillis = toHybridMillis(zonedDateTime);

        int picosOfMilli = (zonedDateTime.getNano() % NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND;
        return fromEpochMillisAndFraction(julianMillis, picosOfMilli, timeZoneKey);
    }

    private static ZonedDateTime fromDateTimeString(String timestamp, ZoneId zoneId)
    {
        return LocalDateTime.parse(timestamp, DATE_TIME_FORMATTER).atZone(zoneId);
    }

    private static ZonedDateTime fromLongDateTimeString(String timestamp, ZoneId zoneId)
    {
        return LocalDateTime.parse(timestamp, LONG_DATE_TIME_FORMATTER).atZone(zoneId);
    }

    private static Instant toInstantInProlepticGregorian(String timestamp)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestamp);
        ZonedDateTime zoned = localDateTime.atZone(UTC);
        return zoned.toInstant();
    }

    private static ZonedDateTime fromProlepticMillis(long millis, ZoneId zoneId)
    {
        Instant instant = Instant.ofEpochMilli(millis);
        return instant.atZone(zoneId);
    }

    public static void assertLongTimestampToVarcharCoercions(TimestampType fromType, LongTimestamp valueToBeCoerced, VarcharType toType, String expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, utf8Slice(expectedValue), NANOSECONDS);
    }

    public static void assertVarcharToShortTimestampCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, expectedValue, MICROSECONDS);
    }

    public static void assertVarcharToLongTimestampCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, expectedValue, NANOSECONDS);
    }

    public static void assertCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue, HiveTimestampPrecision timestampPrecision)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(timestampPrecision, PARQUET)).orElseThrow()
                .apply(writeNativeValue(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }

    private static void assertTimestampToDateCoercion(String timestampAsString, String expectedDate)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampAsString);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), DATE, LocalDate.parse(expectedDate).toEpochDay(), NANOSECONDS);
    }

    private static long toHybridMillis(ZonedDateTime zonedDateTime)
    {
        LocalDateTime ldt = zonedDateTime.toLocalDateTime();
        ZoneId zoneId = zonedDateTime.getZone();
        TimeZone tz = TimeZone.getTimeZone(zoneId);

        if (ldt.isAfter(GREGORIAN_START_DATETIME)) {
            return zonedDateTime.toInstant().toEpochMilli();
        }

        if (ldt.isAfter(JULIAN_END_DATETIME) && ldt.isBefore(GREGORIAN_START_DATETIME)) {
            ldt = LocalDateTime.of(GREGORIAN_START_DATE, ldt.toLocalTime());
        }

        Calendar cal = new Calendar.Builder()
                .setCalendarType("gregory")
                .setDate(ldt.getYear(), ldt.getMonthValue() - 1, ldt.getDayOfMonth())
                .setTimeOfDay(ldt.getHour(), ldt.getMinute(), ldt.getSecond())
                .setTimeZone(tz)
                .build();

        // Handle DST overlap transitions
        ZoneRules rules = zoneId.getRules();
        ZoneOffsetTransition trans = rules.getTransition(ldt);
        if (trans != null && trans.isOverlap()) {
            Calendar cloned = (Calendar) cal.clone();
            int shift = trans.getOffsetBefore().equals(zonedDateTime.getOffset()) ? -1 : 1;
            cloned.add(Calendar.DAY_OF_MONTH, shift);
            cal.set(Calendar.ZONE_OFFSET, cloned.get(Calendar.ZONE_OFFSET));
            cal.set(Calendar.DST_OFFSET, cloned.get(Calendar.DST_OFFSET));
        }

        return cal.getTimeInMillis() + ldt.get(ChronoField.MILLI_OF_SECOND);
    }
}
