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

import com.google.common.annotations.VisibleForTesting;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.zone.ZoneOffsetTransition;
import java.util.Calendar;
import java.util.TimeZone;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.DST_OFFSET;
import static java.util.Calendar.ERA;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;
import static java.util.Calendar.ZONE_OFFSET;

public final class CalendarUtils
{
    private static final TimeZone UTC_TZ = TimeZone.getTimeZone(ZoneId.of("UTC"));
    private static final LocalDate LAST_SWITCH_JULIAN_DAY = LocalDate.of(1582, 10, 15);

    private static final long LAST_SWITCH_JULIAN_LOCAL_MILLIS = LAST_SWITCH_JULIAN_DAY.toEpochDay() * MILLISECONDS_PER_DAY;
    // offsets are less than a day, so both calendars are the same from here in all time zones
    private static final long LAST_SWITCH_JULIAN_MILLIS_IN_ALL_TIME_ZONES = LAST_SWITCH_JULIAN_LOCAL_MILLIS + MILLISECONDS_PER_DAY;

    @VisibleForTesting
    static long lastSwitchJulianDayInMillis(TimeZone timeZone)
    {
        // first instant of 1582-10-15 in the time zone. Calendar resolves the local date time using the offsets at the wall time
        // (ZoneInfo) or at the time shifted by the raw offset (other TimeZone types, e.g. SimpleTimeZone). The two agree for region
        // zones, as none has a transition in 1582, and TestCalendarUtils checks this against Calendar
        return LAST_SWITCH_JULIAN_LOCAL_MILLIS - timeZone.getOffset(LAST_SWITCH_JULIAN_LOCAL_MILLIS - timeZone.getRawOffset());
    }

    private CalendarUtils() {}

    public static int convertHybridDaysToProlepticGregorian(int hybridDays)
    {
        if (hybridDays >= LAST_SWITCH_JULIAN_DAY.toEpochDay()) {
            return hybridDays;
        }
        Calendar calendar = new Calendar.Builder()
                .setCalendarType("gregory")
                .setTimeZone(UTC_TZ)
                .setInstant(multiplyExact(hybridDays, (long) MILLISECONDS_PER_DAY))
                .build();

        LocalDate localDate = LocalDate.of(calendar.get(YEAR), calendar.get(MONTH) + 1, 1)
                .with(ChronoField.ERA, calendar.get(ERA))
                .plusDays(calendar.get(DAY_OF_MONTH) - 1);

        return toIntExact(localDate.toEpochDay());
    }

    public static long convertHybridMicrosToProlepticGregorian(long hybridMicros)
    {
        long hybridMillis = floorDiv(hybridMicros, (long) MICROSECONDS_PER_MILLISECOND);
        long remainderMicros = floorMod(hybridMicros, MICROSECONDS_PER_MILLISECOND);
        long prolepticMillis = convertHybridMillisToProlepticGregorian(hybridMillis, UTC_TZ);
        return multiplyExact(prolepticMillis, MICROSECONDS_PER_MILLISECOND) + remainderMicros;
    }

    public static long convertHybridMillisToProlepticGregorian(long hybridMillis)
    {
        return convertHybridMillisToProlepticGregorian(hybridMillis, UTC_TZ);
    }

    public static long convertHybridMillisToProlepticGregorian(long hybridMillis, TimeZone timeZone)
    {
        // both calendars are the same from the switch day in the time zone. The first check avoids computing it for most values;
        // the switch day in UTC alone is not enough, as it starts later than in time zones east of UTC and earlier than west of it
        if (hybridMillis >= LAST_SWITCH_JULIAN_MILLIS_IN_ALL_TIME_ZONES || hybridMillis >= lastSwitchJulianDayInMillis(timeZone)) {
            return hybridMillis;
        }
        // based on Apache Spark's implementation
        Calendar calendar = new Calendar.Builder()
                .setCalendarType("gregory")
                .setTimeZone(timeZone)
                .setInstant(hybridMillis)
                .build();

        LocalDateTime localDateTime = LocalDateTime.of(
                        calendar.get(YEAR),
                        calendar.get(MONTH) + 1,
                        1, // number of the days will be added at the end to handle not-existing date in leap Julian year which is non-leap in Gregorian
                        calendar.get(HOUR_OF_DAY),
                        calendar.get(MINUTE),
                        calendar.get(SECOND),
                        Math.floorMod(hybridMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND)
                .with(ChronoField.ERA, calendar.get(ERA))
                .plusDays(calendar.get(DAY_OF_MONTH) - 1);
        ZoneId zoneId = timeZone.toZoneId();
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);

        ZoneOffsetTransition zoneOffsetTransition = zoneId.getRules().getTransition(localDateTime);
        if (zoneOffsetTransition != null && zoneOffsetTransition.isOverlap()) {
            int dstOffset = calendar.get(DST_OFFSET);
            int zoneOffset = calendar.get(ZONE_OFFSET);
            calendar.add(DAY_OF_MONTH, 1);

            if (zoneOffset == calendar.get(ZONE_OFFSET) && dstOffset == calendar.get(DST_OFFSET)) {
                zonedDateTime = zonedDateTime.withLaterOffsetAtOverlap();
            }
            else {
                zonedDateTime = zonedDateTime.withEarlierOffsetAtOverlap();
            }
        }

        return zonedDateTime.toInstant().toEpochMilli();
    }
}
