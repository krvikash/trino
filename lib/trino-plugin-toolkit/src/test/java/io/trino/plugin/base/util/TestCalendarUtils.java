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
package io.trino.plugin.base.util;

import org.junit.jupiter.api.Test;

import java.sql.Date;
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
import java.util.TimeZone;

import static io.trino.plugin.base.util.CalendarUtils.convertHybridDaysToProlepticGregorian;
import static io.trino.plugin.base.util.CalendarUtils.convertHybridMillisToProlepticGregorian;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCalendarUtils
{
    private static final LocalDate GREGORIAN_START_DATE = LocalDate.of(1582, 10, 15);
    private static final LocalDate JULIAN_END_DATE = LocalDate.of(1582, 10, 4);
    private static final LocalDateTime GREGORIAN_START_DATETIME = LocalDateTime.of(GREGORIAN_START_DATE, LocalTime.MIDNIGHT);
    private static final LocalDateTime JULIAN_END_DATETIME = LocalDateTime.of(JULIAN_END_DATE, LocalTime.of(23, 59, 59, 999999999));

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
    void testReadNegativeDateWithProlepticGregorianFromDaysInHybridCalendar()
    {
        assertDateConversion(-9999, 12, 31);
        assertDateConversion(-5555, 1, 1);
        assertDateConversion(-4713, 1, 1);
        assertDateConversion(-1000, 1, 1);
        assertDateConversion(-1, 1, 1);
    }

    @Test
    void testConvertingTimestampFromHybridToProlepticGregorianCalendar()
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
    {
        assertTimestampConversion("-9999-12-31 23:59:59.999", zoneId);
        assertTimestampConversion("-5555-01-01 23:59:59.999", zoneId);
        assertTimestampConversion("-4713-01-01 00:00:00.000", zoneId);
        assertTimestampConversion("-1000-01-01 00:00:00.000", zoneId);
        assertTimestampConversion("-0001-01-01 15:15:15.123", zoneId);
        assertTimestampConversion("0001-01-01 15:15:15.123", zoneId);
        assertTimestampConversion("1000-01-01 00:00:00.000", zoneId);
        assertTimestampConversion("1000-01-01 15:15:15.123", zoneId);
        assertTimestampConversion("1000-01-01 23:59:59.999", zoneId);
        assertTimestampConversion("1582-10-04 15:15:15.123", zoneId);
        assertTimestampConversion("1582-10-15 15:15:15.123", zoneId);
        assertTimestampConversion("1582-10-15 23:59:59.999", zoneId);
        assertTimestampConversion("1582-10-16 00:00:00.000", zoneId);
        assertTimestampConversion("1788-09-10 15:15:15.123", zoneId);
        assertTimestampConversion("1888-12-31 15:15:15.123", zoneId);
        assertTimestampConversion("1969-12-31 15:15:15.123", zoneId);
        assertTimestampConversion("1970-01-01 15:15:15.123", zoneId);
        assertTimestampConversion("2024-03-30 15:15:15.123", zoneId);
    }

    private static void assertDateConversion(String date)
    {
        int julianDays = toHybridDaysFromString(date);
        int gregorianDays = convertHybridDaysToProlepticGregorian(julianDays);
        assertThat(date).isEqualTo(LocalDate.ofEpochDay(gregorianDays).toString());
    }

    private static void assertDateConversion(int year, int month, int day)
    {
        int julianDays = toHybridDaysFromString(year, month, day);
        int gregorianDays = convertHybridDaysToProlepticGregorian(julianDays);
        LocalDate localDate = LocalDate.of(year, month, 1).plusDays(day - 1);
        LocalDate other = LocalDate.ofEpochDay(gregorianDays);
        assertThat(localDate.isEqual(other)).isTrue();
    }

    private static void assertTimestampConversion(String timestamp, ZoneId zoneId)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS").withZone(UTC);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.parse(timestamp, formatter), zoneId);
        long expectedMillis = zonedDateTime.toInstant().toEpochMilli();

        long julianMillis = toHybridMillis(zonedDateTime);

        long gregorianMillis = convertHybridMillisToProlepticGregorian(julianMillis, TimeZone.getTimeZone(zoneId));
        assertThat(gregorianMillis).isEqualTo(expectedMillis);
    }

    private static int toHybridDaysFromString(String dateStr)
    {
        Date date = Date.valueOf(dateStr);
        int tzMillis = TimeZone.getDefault().getOffset(date.getTime());
        return toIntExact((date.getTime() + tzMillis) / DAYS.toMillis(1));
    }

    private static int toHybridDaysFromString(int year, int month, int day)
    {
        Date date = new Date(year - 1900, month - 1, day);
        int tzMillis = TimeZone.getDefault().getOffset(date.getTime());
        return toIntExact((date.getTime() + tzMillis) / DAYS.toMillis(1));
    }

    @Test
    void testTimezone()
    {
        assertTimestampTimezone("-1001-01-01T15:15:15.123", ZoneId.of("UTC"));
        assertTimestampTimezone("0001-01-01T15:15:15.123", ZoneId.of("UTC"));
        assertTimestampTimezone("-1001-01-01T15:15:15.123", ZoneId.of("Europe/London"));
        assertTimestampTimezone("0001-01-01T15:15:15.123", ZoneId.of("Europe/London"));
        assertTimestampTimezone("-1001-01-01T15:15:15.123", ZoneId.of("America/New_York"));
        assertTimestampTimezone("0001-01-01T15:15:15.123", ZoneId.of("America/Los_Angeles"));
    }

    public void assertTimestampTimezone(String timestamp, ZoneId zoneId)
    {
        ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.parse(timestamp), zoneId);
        long julianMillis = toHybridMillis(zonedDateTime);

        long gregorianMillis = convertHybridMillisToProlepticGregorian(julianMillis, TimeZone.getTimeZone(zoneId));

        long expectedMillis = LocalDateTime.parse(timestamp).atZone(zoneId).toInstant().toEpochMilli();
        assertThat(gregorianMillis).isEqualTo(expectedMillis);
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
