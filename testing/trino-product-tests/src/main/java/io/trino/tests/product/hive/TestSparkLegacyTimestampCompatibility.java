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
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_SPARK;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSparkLegacyTimestampCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "hive";
    private static final String SCHEMA = "default";
//    Apache Spark versions (< 3.0.0.) used Hybrid Calendar for all dates and INT64 timestamp (e.g. Julian calendar before October 15th 1582, and the Gregorian calendar after that)
//    Apache Spark versions (< 3.1.0.) used Hybrid Calendar for INT96 timestamp (e.g. Julian calendar before October 15th 1582, and the Gregorian calendar after that)
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

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testSparkParquetLegacyTimestampCompatibilityWithHybridCalendar()
    {
        testSparkParquetLegacyTimestampCompatibility("LEGACY");
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testSparkParquetLegacyTimestampCompatibilityWithProlepticCalendar()
    {
        testSparkParquetLegacyTimestampCompatibility("CORRECTED");
    }

    private void testSparkParquetLegacyTimestampCompatibility(String rebaseMode)
    {
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "INT96", MILLISECONDS, timestampsWithPrecision(MILLISECONDS));
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "INT96", MICROSECONDS, timestampsWithPrecision(MICROSECONDS));
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "INT96", NANOSECONDS, timestampsWithPrecision(MICROSECONDS));

        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "TIMESTAMP_MICROS", MILLISECONDS, timestampsWithPrecision(MILLISECONDS));
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "TIMESTAMP_MICROS", MICROSECONDS, timestampsWithPrecision(MICROSECONDS));
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "TIMESTAMP_MICROS", NANOSECONDS, timestampsWithPrecision(MICROSECONDS));

        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "TIMESTAMP_MILLIS", MILLISECONDS, timestampsWithPrecision(MILLISECONDS), timestampsWithPrecision(MILLISECONDS));
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "TIMESTAMP_MILLIS", MICROSECONDS, timestampsWithPrecision(MICROSECONDS), timestampsWithPrecision(MILLISECONDS));
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, "TIMESTAMP_MILLIS", NANOSECONDS, timestampsWithPrecision(MILLISECONDS), timestampsWithPrecision(MILLISECONDS));
    }

    private static void testSparkParquetLegacyTimestampCompatibility(
            String rebaseMode,
            String sparkOutputTimestampType,
            HiveTimestampPrecision trinoTimestampPrecision,
            List<String> timestamps)
    {
        testSparkParquetLegacyTimestampCompatibility(rebaseMode, sparkOutputTimestampType, trinoTimestampPrecision, timestamps, timestamps);
    }

    private static void testSparkParquetLegacyTimestampCompatibility(
            String rebaseMode,
            String sparkOutputTimestampType,
            HiveTimestampPrecision trinoTimestampPrecision,
            List<String> timestamps,
            List<String> expectedTimestamps)
    {
        String sparkTableName = "test_spark_parquet_legacy_timestamp_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(TRINO_CATALOG, SCHEMA, sparkTableName);
        try {
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=%s".formatted(rebaseMode));
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            }
            onSpark().executeQuery("CREATE TABLE %s.%s (tmst timestamp) USING PARQUET".formatted(SCHEMA, sparkTableName));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, sparkTableName, toValues(timestamps)));

            onTrino().executeQuery("SET SESSION hive.timestamp_precision = '" + trinoTimestampPrecision + "'");
            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTimestamps));
            assertThat(onSpark().executeQuery("SELECT tmst FROM " + sparkTableName)).containsOnly(toExpectedRows(expectedTimestamps));
        }
        finally {
            onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
        }
    }

    private static String toValues(List<String> timestamps)
    {
        return timestamps.stream()
                .map("(TIMESTAMP '%s')"::formatted)
                .collect(joining(","));
    }

    private static List<String> timestampsWithPrecision(HiveTimestampPrecision hiveTimestampPrecision)
    {
        return TIMESTAMPS.stream()
                .map(timestamp -> generateTimestampsWithPrecision(timestamp, switch (hiveTimestampPrecision) {
                    case MILLISECONDS -> ImmutableList.of("123", "999");
                    case MICROSECONDS -> ImmutableList.of("123456", "999999");
                    case NANOSECONDS -> ImmutableList.of("123456789", "999999999");
                }))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private static List<String> generateTimestampsWithPrecision(String timestamp, List<String> fractions)
    {
        return fractions.stream().map(fraction -> "%s.%s".formatted(timestamp, fraction)).collect(toImmutableList());
    }

    private static List<QueryAssert.Row> toExpectedRows(List<String> timestamps)
    {
        return timestamps.stream()
                .map(value -> row(Timestamp.valueOf(value)))
                .collect(toImmutableList());
    }
}
