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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.trino.cache.EvictableCacheBuilder;

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

    private static long lastSwitchJulianDayInMillis(TimeZone timeZone)
    {
        // last day of Julian calendar is 1582-10-4, and the next day is 1582-10-15 in Gregorian calendar.
        // the switch day is 1582-10-15
        Calendar cal = new Calendar.Builder()
                .setCalendarType("gregory")
                .setDate(1582, Calendar.OCTOBER, 15)
                .setTimeZone(timeZone)
                .build();
        return cal.getTimeInMillis();
    }

    private static final LoadingCache<TimeZone, Long> switchDaysByTimeZone = EvictableCacheBuilder.newBuilder()
            .maximumSize(100)
            .build(CacheLoader.from(CalendarUtils::lastSwitchJulianDayInMillis));

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
        ZoneId zoneId = timeZone.toZoneId();
        if (hybridMillis >= switchDaysByTimeZone.getUnchecked(timeZone)) {
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
