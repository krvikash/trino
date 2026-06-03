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

import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.plugin.hive.parquet.ParquetTypeTranslator;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TypeUtils.blockToNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateCoercer
{
    private static final TimeZone UTC = TimeZone.getTimeZone(ZoneId.of("UTC"));

    @Test
    public void testValidVarcharToDate()
    {
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "+10000-04-13");
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "1900-01-01");
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "2000-01-01");
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "2023-03-12");
    }

    @Test
    public void testThrowsExceptionWhenStringIsNotAValidDate()
    {
        // hive would return 2023-02-09
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "2023-01-40", null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");

        // hive would return 2024-03-13
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "2023-15-13", null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");

        // hive would return null
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "invalidDate", null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");
    }

    @Test
    public void testThrowsExceptionWhenDateIsTooOld()
    {
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "1899-12-31", null))
                .hasMessageMatching(".*Coercion on historical dates is not supported.*");
    }

    @Test
    public void testDateToVarchar()
    {
        assertDateToVarcharCoercion(createUnboundedVarcharType(), LocalDate.parse("2023-01-10"), "2023-01-10");
        assertDateToVarcharCoercion(createUnboundedVarcharType(), LocalDate.parse("+10000-04-25"), "+10000-04-25");
    }

    @Test
    public void testDateToLowerBoundedVarchar()
    {
        assertThatThrownBy(() -> assertDateToVarcharCoercion(createVarcharType(8), LocalDate.parse("2023-10-23"), "2023-10-23"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of '2023-10-23' exceeds varchar(8) bounds");
    }

    @Test
    public void testHistoricalDateToVarchar()
    {
        assertThatThrownBy(() -> assertDateToVarcharCoercion(createUnboundedVarcharType(), LocalDate.parse("1899-12-31"), null))
                .hasMessageMatching(".*Coercion on historical dates is not supported.*");
    }

    @Test
    public void testLegacyDateCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        assertReadingWithCoercionHybridToProlepticLegacyDate("0001-01-01", "0001-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0100-01-01", "0100-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0200-01-01", "0200-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0300-01-01", "0300-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0400-01-01", "0400-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0500-01-01", "0500-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0600-01-01", "0600-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0700-01-01", "0700-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0800-01-01", "0800-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("0900-01-01", "0900-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1000-01-01", "1000-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1101-01-01", "1101-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1201-01-01", "1201-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1301-01-01", "1301-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1401-01-01", "1401-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1501-01-01", "1501-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1582-01-01", "1582-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1582-10-03", "1582-10-03");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1582-10-04", "1582-10-04");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1582-10-15", "1582-10-15");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1582-10-16", "1582-10-16");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1788-09-10", "1788-09-10");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1888-12-31", "1888-12-31");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1969-12-31", "1969-12-31");
        assertReadingWithCoercionHybridToProlepticLegacyDate("1970-01-01", "1970-01-01");
        assertReadingWithCoercionHybridToProlepticLegacyDate("2024-03-30", "2024-03-30");

        assertReadingWithCoercionHybridToProlepticLegacyDate("1000-02-29", "1000-03-01"); // legacy leap year
        assertReadingWithCoercionHybridToProlepticLegacyDate("1600-02-29", "1600-02-29"); // Gregorian leap year
        assertReadingWithCoercionHybridToProlepticLegacyDate("2000-02-29", "2000-02-29"); // Gregorian leap year

        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0001-01-01", "0000-12-30");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0100-01-01", "0099-12-30");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0200-01-01", "0199-12-31");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0300-01-01", "0300-01-01");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0400-01-01", "0400-01-02");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0500-01-01", "0500-01-02");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0600-01-01", "0600-01-03");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0700-01-01", "0700-01-04");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0800-01-01", "0800-01-05");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("0900-01-01", "0900-01-05");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1000-01-01", "1000-01-06");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1101-01-01", "1101-01-08");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1201-01-01", "1201-01-08");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1301-01-01", "1301-01-09");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1401-01-01", "1401-01-10");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1501-01-01", "1501-01-11");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1582-01-01", "1582-01-11");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1582-10-03", "1582-10-13");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1582-10-04", "1582-10-14");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1582-10-15", "1582-10-15");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1582-10-16", "1582-10-16");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1788-09-10", "1788-09-10");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1888-12-31", "1888-12-31");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1969-12-31", "1969-12-31");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1970-01-01", "1970-01-01");
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("2024-03-30", "2024-03-30");

        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1000-02-29", "1000-03-06"); // legacy leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("1600-02-29", "1600-02-29"); // Gregorian leap year
        assertReadingWithoutCoercionHybridToProlepticLegacyDate("2000-02-29", "2000-02-29"); // Gregorian leap year
    }

    @Test
    public void testLegacyNegativeDateCoercionFromHybridCalendarToProlepticGregorianCalendar()
    {
        assertReadingWithCoercionHybridToProlepticLegacyDate(-9999, 12, 31);
        assertReadingWithCoercionHybridToProlepticLegacyDate(-5555, 1, 1);
        assertReadingWithCoercionHybridToProlepticLegacyDate(-4713, 1, 1);
        assertReadingWithCoercionHybridToProlepticLegacyDate(-1001, 1, 1);
        assertReadingWithCoercionHybridToProlepticLegacyDate(-1, 1, 1);

        assertReadingWithoutCoercionHybridToProlepticLegacyDate(-9999, 12, 31, -9999, 10, 15);
        assertReadingWithoutCoercionHybridToProlepticLegacyDate(-5555, 1, 1, -5556, 11, 18);
        assertReadingWithoutCoercionHybridToProlepticLegacyDate(-4713, 1, 1, -4714, 11, 24);
        assertReadingWithoutCoercionHybridToProlepticLegacyDate(-1001, 1, 1, -1002, 12, 22);
        assertReadingWithoutCoercionHybridToProlepticLegacyDate(-1, 1, 1, -2, 12, 30);
    }

    private void assertReadingWithCoercionHybridToProlepticLegacyDate(String date, String actualReadDate)
    {
        assertReadingHybridToProlepticLegacyDate(true, toHybridDays(date), toEpochDaysInProlepticGregorian(actualReadDate));
    }

    private void assertReadingWithoutCoercionHybridToProlepticLegacyDate(String writtenDate, String actualReadDate)
    {
        assertReadingHybridToProlepticLegacyDate(false, toHybridDays(writtenDate), toEpochDaysInProlepticGregorian(actualReadDate));
    }

    private void assertReadingWithCoercionHybridToProlepticLegacyDate(int year, int month, int day)
    {
        assertReadingHybridToProlepticLegacyDate(true, toHybridDays(year, month, day), toEpochDaysInProlepticGregorian(year, month, day));
    }

    private void assertReadingWithoutCoercionHybridToProlepticLegacyDate(int jYear, int jMonth, int jDay, int gYear, int gMonth, int gDay)
    {
        assertReadingHybridToProlepticLegacyDate(false, toHybridDays(jYear, jMonth, jDay), toEpochDaysInProlepticGregorian(gYear, gMonth, gDay));
    }

    private void assertReadingHybridToProlepticLegacyDate(boolean convertDateToProleptic, long hybridDays, long gregorianDays)
    {
        Block writtenBlock = writeNativeValue(DATE, hybridDays);
        Optional<TypeCoercer<? extends Type, ? extends Type>> coercer =
                ParquetTypeTranslator.createCoercer(
                        INT32,
                        LogicalTypeAnnotation.dateType(),
                        DATE,
                        new ParquetTypeTranslator.CoercionContext(convertDateToProleptic, false, false, UTC));
        Block readBlock = coercer.isPresent() ? coercer.get().apply(writtenBlock) : writtenBlock;

        long actualDays = (Long) blockToNativeValue(DATE, readBlock);
        assertThat(actualDays)
                .withFailMessage("Expected to read %s days but was %s days", LocalDate.ofEpochDay(gregorianDays).toString(), LocalDate.ofEpochDay(actualDays).toString())
                .isEqualTo(gregorianDays);
    }

    private static long toEpochDaysInProlepticGregorian(String date)
    {
        return LocalDate.parse(date).toEpochDay();
    }

    private static long toEpochDaysInProlepticGregorian(int year, int month, int day)
    {
        return LocalDate.of(year, month, 1).plusDays(day - 1).toEpochDay();
    }

    private long toHybridDays(String dateStr)
    {
        Date date = Date.valueOf(dateStr);
        long millis = date.getTime();
        return TimeUnit.MILLISECONDS.toDays(millis + TimeZone.getDefault().getOffset(millis));
    }

    private long toHybridDays(int year, int month, int day)
    {
        Date date = new Date(year - 1900, month - 1, day);
        long millis = date.getTime();
        return TimeUnit.MILLISECONDS.toDays(millis + TimeZone.getDefault().getOffset(millis));
    }

    private void assertVarcharToDateCoercion(Type fromType, String date)
    {
        assertVarcharToDateCoercion(fromType, date, fromDateToEpochDate(date));
    }

    private void assertVarcharToDateCoercion(Type fromType, String date, Long expected)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(DATE), new CoercionContext(DEFAULT_PRECISION, PARQUET)).orElseThrow()
                .apply(writeNativeValue(fromType, utf8Slice(date)));
        assertThat(blockToNativeValue(DATE, coercedValue))
                .isEqualTo(expected);
    }

    private void assertDateToVarcharCoercion(Type toType, LocalDate date, String expected)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(DATE), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, PARQUET)).orElseThrow()
                .apply(writeNativeValue(DATE, date.toEpochDay()));
        assertThat(blockToNativeValue(VARCHAR, coercedValue))
                .isEqualTo(utf8Slice(expected));
    }

    private long fromDateToEpochDate(String dateString)
    {
        LocalDate date = LocalDate.parse(dateString);
        return date.toEpochDay();
    }
}
