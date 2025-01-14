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
import java.time.LocalDate;

import static io.trino.plugin.hive.util.CalendarUtils.convertHybridDaysToProlepticGregorian;
import static java.lang.Math.floor;
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

    private static void assertDateConversion(String date)
    {
        int julianDays = toHybridDaysFromString(date);
        int gregorianDays = convertHybridDaysToProlepticGregorian(julianDays);
        assertThat(date).isEqualTo(LocalDate.ofEpochDay(gregorianDays).toString());
    }

    private static int toHybridDaysFromString(String date)
    {
        long millisUtc = Date.valueOf(date).getTime();
        return (int) floor((double) millisUtc / DAYS.toMillis(1));
    }
}
