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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.time.format.ResolverStyle.LENIENT;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CalendarUtils
{
    private static final TimeZone TZ_UTC = TimeZone.getTimeZone(UTC);
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String LAST_SWITCH_JULIAN_DAY_STR = "1582-10-15";
    private static final long LAST_SWITCH_JULIAN_DAY_MILLIS;
    private static final long LAST_SWITCH_JULIAN_DAY;

    private static final ThreadLocal<SimpleDateFormat> HYBRID_CALENDAR_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        format.setCalendar(new GregorianCalendar(TZ_UTC));
        return format;
    });

    private static final ThreadLocal<SimpleDateFormat> HYBRID_CALENDAR_DATE_TIME_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_TIME_FORMAT);
        format.setCalendar(new GregorianCalendar(TZ_UTC));
        return format;
    });

    private static final DateTimeFormatter PROLEPTIC_CALENDAR_DATE_FORMAT = DateTimeFormatter.ofPattern(DATE_FORMAT)
            .withResolverStyle(LENIENT);

    private static final DateTimeFormatter PROLEPTIC_CALENDAR_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT)
            .withResolverStyle(LENIENT);

    static {
        try {
            LAST_SWITCH_JULIAN_DAY_MILLIS = HYBRID_CALENDAR_DATE_FORMAT.get().parse(LAST_SWITCH_JULIAN_DAY_STR).getTime();
            LAST_SWITCH_JULIAN_DAY = MILLISECONDS.toDays(LAST_SWITCH_JULIAN_DAY_MILLIS);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private CalendarUtils() {}

    public static int convertHybridDaysToProlepticGregorian(int hybridDays)
    {
        if (hybridDays >= LAST_SWITCH_JULIAN_DAY) {
            return hybridDays;
        }
        long hybridMillis = DAYS.toMillis(hybridDays);
        String hybridDateInString = HYBRID_CALENDAR_DATE_FORMAT.get().format(new Date(hybridMillis));
        return toIntExact(LocalDate.from(PROLEPTIC_CALENDAR_DATE_FORMAT.parse(hybridDateInString)).toEpochDay());
    }

    public static long convertHybridMicrosToProlepticGregorian(long hybridMicros)
    {
        long hybridMillis = floorDiv(hybridMicros, (long) MICROSECONDS_PER_MILLISECOND);
        long remainderMicros = floorMod(hybridMicros, MICROSECONDS_PER_MILLISECOND);
        long prolepticMillis = convertHybridMillisToProlepticGregorian(hybridMillis);
        return multiplyExact(prolepticMillis, MICROSECONDS_PER_MILLISECOND) + remainderMicros;
    }

    public static long convertHybridMillisToProlepticGregorian(long hybridMillis)
    {
        if (hybridMillis < LAST_SWITCH_JULIAN_DAY_MILLIS) {
            String dateTimeInString = HYBRID_CALENDAR_DATE_TIME_FORMAT.get().format(new Date(hybridMillis));
            LocalDateTime localDateTime = LocalDateTime.parse(dateTimeInString, PROLEPTIC_CALENDAR_DATE_TIME_FORMAT);
            return multiplyExact(localDateTime.toEpochSecond(UTC), MILLISECONDS_PER_SECOND) + floorDiv(localDateTime.getNano(), NANOSECONDS_PER_MILLISECOND);
        }
        return hybridMillis;
    }
}
