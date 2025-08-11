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
package io.trino.plugin.hive.util;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import static io.trino.plugin.hive.util.CalendarUtils.convertHybridDaysToProlepticGregorian;
import static io.trino.plugin.hive.util.CalendarUtils.convertHybridMillisToProlepticGregorian;
import static java.lang.Math.floor;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCalendarUtils
{
    @Test
    void testReadDateWithProlepticGregorianFromDaysInHybridCalendar()
    {
        assertDateConversion("0001-01-01"); // first day of AD
        assertDateConversion("1000-01-01");
        assertDateConversion("1582-10-03");
        assertDateConversion("1582-10-04"); // 05-14 Oct 1582 - missing days in Julian calendar
        assertDateConversion("1582-10-15"); // Gregorian cutover day
        assertDateConversion("1582-10-16");
        assertDateConversion("1788-09-10");
        assertDateConversion("1888-12-31");
        assertDateConversion("1969-12-31"); // The epoch day -1
        assertDateConversion("1970-01-01"); // The epoch day
        assertDateConversion("2024-03-30");
    }

    @Test
    void testConvertingTimestampFromHybridToProlepticGregorianCalendar()
            throws ParseException
    {
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("UTC"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Europe/Amsterdam"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Europe/Warsaw"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("America/Los_Angeles"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("America/Bahia_Banderas"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Asia/Kolkata"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Asia/Hong_Kong"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Africa/Dakar"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Antarctica/Vostok"));
        testConvertGregorianTimestampToAndFromHybridDays(ZoneId.of("Asia/Kathmandu"));
    }

    private void testConvertGregorianTimestampToAndFromHybridDays(ZoneId zoneId)
            throws ParseException
    {
        assertTimestampConversion("0001-01-01T15:15:15.123", zoneId);
        assertTimestampConversion("1000-01-01T00:00:00.000", zoneId);
        assertTimestampConversion("1000-01-01T15:15:15.123", zoneId);
        assertTimestampConversion("1000-01-01T23:59:59.999", zoneId);
        assertTimestampConversion("1582-10-04T15:15:15.123", zoneId);
        assertTimestampConversion("1582-10-15T15:15:15.123", zoneId);
        assertTimestampConversion("1582-10-15T23:59:59.999", zoneId);
        assertTimestampConversion("1582-10-16T00:00:00.000", zoneId);
        assertTimestampConversion("1788-09-10T15:15:15.123", zoneId);
        assertTimestampConversion("1888-12-31T15:15:15.123", zoneId);
        assertTimestampConversion("1969-12-31T15:15:15.123", zoneId);
        assertTimestampConversion("1970-01-01T15:15:15.123", zoneId);
        assertTimestampConversion("2024-03-30T15:15:15.123", zoneId);
    }

    private static void assertDateConversion(String date)
    {
        int julianDays = toHybridDaysFromString(date);
        int gregorianDays = convertHybridDaysToProlepticGregorian(julianDays);
        assertThat(date).isEqualTo(LocalDate.ofEpochDay(gregorianDays).toString());
    }

    private static void assertTimestampConversion(String timestamp, ZoneId zoneId)
            throws ParseException
    {
        ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.parse(timestamp), zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS").withZone(UTC);
        long julianMillis = parseToMillisInJulian(formatter.format(zonedDateTime.toInstant()));

        long gregorianMillis = convertHybridMillisToProlepticGregorian(julianMillis);

        long expectedMillis = LocalDateTime.parse(timestamp).atZone(zoneId).toInstant().toEpochMilli();
        assertThat(gregorianMillis).isEqualTo(expectedMillis);
    }

    private static int toHybridDaysFromString(String date)
    {
        long millisUtc = Date.valueOf(date).getTime();
        return (int) floor((double) millisUtc / DAYS.toMillis(1));
    }

    private static long parseToMillisInJulian(String timestamp)
            throws ParseException
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        format.setTimeZone(TimeZone.getTimeZone(UTC));
        java.util.Date date = format.parse(timestamp);
        return date.getTime();
    }
}
