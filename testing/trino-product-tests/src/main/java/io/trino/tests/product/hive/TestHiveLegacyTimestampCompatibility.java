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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE4;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveLegacyTimestampCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "hive";
    private static final String SCHEMA = "default";
    private static final Set<String> TIMESTAMPS = ImmutableSet.of(
            "0001-01-01 15:30:12",
            "1001-01-01 15:30:12",
            "1234-01-01 15:30:12",
            "1582-10-04 15:30:12",
            "1582-10-04 23:59:59",
            "1582-10-15 00:00:00",
            "1582-10-15 15:30:12",
            "1883-11-10 15:30:12",
            "1883-11-20 15:30:12",
            "1969-12-31 15:30:12",
            "1970-01-01 15:30:12",
            "2022-04-13 15:30:12");

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyTimestampCompatibility()
    {
        testHiveParquetLegacyTimestampCompatibility(MILLISECONDS, timestampsWithPrecision(MILLISECONDS));

        testHiveParquetLegacyTimestampCompatibility(MICROSECONDS, timestampsWithPrecision(MICROSECONDS));

        testHiveParquetLegacyTimestampCompatibility(NANOSECONDS, timestampsWithPrecision(NANOSECONDS));
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testConvertingNonExistingTimestampInHybridCalendar()
    {
        // the no-existing dates in hybrid calendar (5-14 Oct 1582) are mapped to the same dates in proleptic Gregorian calendar by Hive before saving to Parquet
        // so when we read them in Trino or Hive we expect mapped values to Gregorian calendar
        List<String> timestamps = ImmutableList.<String>builder()
                .add("1582-10-04 23:59:59.999")
                .add("1582-10-05 00:00:00.123")
                .add("1582-10-06 00:00:00.001")
                .add("1582-10-07 01:02:03.004")
                .add("1582-10-08 00:00:00.999")
                .add("1582-10-09 00:00:00.111")
                .add("1582-10-10 15:15:15.123")
                .add("1582-10-11 01:01:12.111")
                .add("1582-10-12 15:15:15.999")
                .add("1582-10-13 00:00:00.789")
                .add("1582-10-14 23:59:59.999")
                .add("1582-10-15 00:00:00.009")
                .build();

        List<String> expectedTimestamps = ImmutableList.<String>builder()
                .add("1582-10-04 23:59:59.999")
                .add("1582-10-15 00:00:00.123")
                .add("1582-10-16 00:00:00.001")
                .add("1582-10-17 01:02:03.004")
                .add("1582-10-18 00:00:00.999")
                .add("1582-10-19 00:00:00.111")
                .add("1582-10-20 15:15:15.123")
                .add("1582-10-21 01:01:12.111")
                .add("1582-10-22 15:15:15.999")
                .add("1582-10-23 00:00:00.789")
                .add("1582-10-24 23:59:59.999")
                .add("1582-10-15 00:00:00.009")
                .build();

        testHiveParquetLegacyTimestampCompatibility(MILLISECONDS, timestamps, expectedTimestamps);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyLeapYearTimestampCompatibility()
    {
        String hiveTableName = "test_hive_parquet_legacy_leap_year_tmst_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, hiveTableName);

        try {
            onTrino().executeQuery("SET SESSION hive.timestamp_precision = '%s'".formatted(MILLISECONDS));
            onHive().executeQuery("CREATE TABLE %s.%s (tmst timestamp) STORED AS PARQUET ".formatted(SCHEMA, hiveTableName));

            assertQueryFailure(() -> onHive().executeQuery("INSERT INTO %s.%s VALUES (TIMESTAMP '1000-02-29 01:02:03.123')".formatted(SCHEMA, hiveTableName)))
                    .hasMessageContaining("Unable to convert time literal '1000-02-29 01:02:03.123' to time value");

            onHive().executeQuery("INSERT INTO %s.%s VALUES (TIMESTAMP '1600-02-29 11:12:13.654')".formatted(SCHEMA, hiveTableName));

            assertQueryFailure(() -> onHive().executeQuery("INSERT INTO %s.%s VALUES (TIMESTAMP '1700-02-29 21:22:23.001')".formatted(SCHEMA, hiveTableName)))
                    .hasMessageContaining("Unable to convert time literal '1700-02-29 21:22:23.001' to time value");

            onHive().executeQuery("INSERT INTO %s.%s VALUES (TIMESTAMP '2000-02-29 00:00:00.999')".formatted(SCHEMA, hiveTableName));

            // Hive cannot accept Julian leap year dates
            QueryAssert.Row[] expectedRows = {
                    row(Timestamp.valueOf("1600-02-29 11:12:13.654")),
                    row(Timestamp.valueOf("2000-02-29 00:00:00.999"))};

            assertThat(onHive().executeQuery("SELECT tmst FROM " + hiveTableName)).containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName)).containsOnly(expectedRows);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    private void testHiveParquetLegacyTimestampCompatibility(
            HiveTimestampPrecision trinoTimestampPrecision,
            List<String> timestamps)
    {
        testHiveParquetLegacyTimestampCompatibility(trinoTimestampPrecision, timestamps, timestamps);
    }

    private void testHiveParquetLegacyTimestampCompatibility(
            HiveTimestampPrecision trinoTimestampPrecision,
            List<String> timestamps,
            List<String> expectedTimestamps)
    {
        String hiveTableName = "test_hive_parquet_legacy_tmst_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, hiveTableName);

        try {
            onTrino().executeQuery("SET SESSION hive.timestamp_precision = '%s'".formatted(trinoTimestampPrecision.toString()));

            onHive().executeQuery("CREATE TABLE %s.%s (tmst timestamp) STORED AS PARQUET ".formatted(SCHEMA, hiveTableName));
            onHive().executeQuery("INSERT INTO %s VALUES %s".formatted(hiveTableName, toValues(timestamps)));

            List<QueryAssert.Row> expectedRows = toExpectedRows(expectedTimestamps);
            assertThat(onHive().executeQuery("SELECT tmst FROM " + hiveTableName)).containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName)).containsOnly(expectedRows);

            String ctasTable = hiveTableName + "_ctas";
            onTrino().executeQuery("CREATE TABLE %s AS SELECT * FROM %s".formatted(ctasTable, trinoTableName));
            assertThat(onTrino().executeQuery("SELECT * from %s".formatted(ctasTable))).containsOnly(expectedRows);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    private static ImmutableList<String> fractions(HiveTimestampPrecision precision)
    {
        return switch (precision) {
            case MILLISECONDS -> ImmutableList.of("123", "999");
            case MICROSECONDS -> ImmutableList.of("123456", "999999");
            case NANOSECONDS -> ImmutableList.of("123456789", "999999999");
        };
    }

    private static List<String> timestampsWithPrecision(HiveTimestampPrecision precision)
    {
        return TIMESTAMPS.stream()
                .map(timestamp -> generateTimestampsWithPrecision(timestamp, fractions(precision)))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private static List<String> generateTimestampsWithPrecision(String timestamp, List<String> fractions)
    {
        return fractions.stream().map(fraction -> "%s.%s".formatted(timestamp, fraction)).collect(toImmutableList());
    }

    private static String toValues(List<String> timestamps)
    {
        return timestamps.stream()
                .map("(TIMESTAMP '%s')"::formatted)
                .collect(joining(","));
    }

    private static ImmutableList<QueryAssert.Row> toExpectedRows(List<String> timestamps)
    {
        return timestamps.stream()
                .map(value -> row(Timestamp.valueOf(value)))
                .collect(toImmutableList());
    }
}
