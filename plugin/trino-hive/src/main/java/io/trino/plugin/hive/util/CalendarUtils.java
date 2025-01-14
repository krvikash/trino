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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CalendarUtils
{
    private static final TimeZone TZ_UTC = TimeZone.getTimeZone(UTC);
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String LAST_SWITCH_JULIAN_DAY_STR = "1582-10-15";
    private static final long LAST_SWITCH_JULIAN_DAY_MILLIS;
    private static final long LAST_SWITCH_JULIAN_DAY;

    static final ThreadLocal<SimpleDateFormat> HYBRID_CALENDAR_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        format.setCalendar(new GregorianCalendar(TZ_UTC));
        return format;
    });

    static final ThreadLocal<SimpleDateFormat> PROLEPTIC_CALENDAR_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        GregorianCalendar prolepticGregorianCalendar = new GregorianCalendar(TZ_UTC);
        prolepticGregorianCalendar.setGregorianChange(new Date(Long.MIN_VALUE));
        format.setCalendar(prolepticGregorianCalendar);
        return format;
    });

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
        try {
            return (int) MILLISECONDS.toDays(PROLEPTIC_CALENDAR_DATE_FORMAT.get().parse(hybridDateInString).getTime());
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
