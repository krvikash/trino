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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_143;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_154;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.TestDeltaLakeLegacyDateTimeCompatibility.Precision.MICROSECONDS;
import static io.trino.tests.product.deltalake.TestDeltaLakeLegacyDateTimeCompatibility.Precision.MILLISECONDS;
import static io.trino.tests.product.deltalake.TestDeltaLakeLegacyDateTimeCompatibility.Precision.NANOSECONDS;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeLegacyDateTimeCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private static final String DELTA_CATALOG = "delta";
    private static final String SCHEMA = "default";
    private static final Map<Integer, String> ID_TO_DATE = ImmutableMap.of(
            1, "0001-01-01",
            2, "1001-01-01",
            3, "1234-01-01",
            4, "1582-10-04",
            5, "1582-10-15",
            6, "1883-11-10",
            7, "1883-11-20",
            8, "1969-12-31",
            9, "1970-01-01",
            10, "2022-04-13");

    private static final String TABLE_VALUES = ID_TO_DATE.entrySet()
            .stream()
            .map(entry -> "(%d, DATE '%s')".formatted(entry.getKey(), entry.getValue()))
            .collect(joining(","));

    private static final List<QueryAssert.Row> EXPECTED_ROWS = ID_TO_DATE.entrySet()
            .stream()
            .map(entry -> row(entry.getKey(), Date.valueOf(entry.getValue())))
            .collect(toImmutableList());

    private static final Set<String> TIMESTAMPS = ImmutableSet.of(
            "0001-01-01 15:30:12",
            "1001-01-01 15:30:12",
            "1234-01-01 15:30:12",
            "1582-10-04 15:30:12",
            "1582-10-04 22:59:59",
            "1582-10-15 00:00:00",
            "1582-10-15 15:30:12",
            "1883-11-10 15:30:12",
            "1883-11-20 15:30:12",
            "1969-12-31 15:30:12",
            "1970-01-01 15:30:12",
            "2022-04-13 15:30:12");

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_143, DELTA_LAKE_DATABRICKS_154, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaParquetLegacyDateCompatibilityWithHybridCalendar()
    {
        testDeltaParquetLegacyDateCompatibility("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_143, DELTA_LAKE_DATABRICKS_154, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaParquetLegacyDateCompatibilityWithProlepticCalendar()
    {
        testDeltaParquetLegacyDateCompatibility("CORRECTED");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaAndSparkParquetLegacyDateCompatibilityWithHybridCalendar()
    {
        testDeltaAndSparkParquetLegacyDateCompatibility("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaAndSparkParquetLegacyDateCompatibilityWithProlepticCalendar()
    {
        testDeltaAndSparkParquetLegacyDateCompatibility("CORRECTED");
    }

    private void testDeltaParquetLegacyDateCompatibility(String rebaseMode)
    {
        String deltaTableName = "test_deltalake_parquet_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onDelta().executeQuery("CREATE TABLE %s.%s (id integer, date_col date) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onDelta().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            onDelta().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, TABLE_VALUES));

            assertThat(onDelta().executeQuery("SELECT id, date_col FROM " + deltaTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    private void testDeltaAndSparkParquetLegacyDateCompatibility(String rebaseMode)
    {
        String deltaTableName = "test_spark_on_delta_on_parquet_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onDelta().executeQuery("CREATE TABLE %s.%s (id integer, date_col date) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, TABLE_VALUES));

            assertThat(onDelta().executeQuery("SELECT id, date_col FROM " + deltaTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onSpark().executeQuery("SELECT id, date_col FROM " + deltaTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtzHybridCalendar()
    {
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtzProlepticCalendar()
    {
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz("CORRECTED");
    }

    private void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(String rebaseMode)
    {
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "INT96", timestampsWithPrecision(MILLISECONDS), timestampsWithPrecision(MILLISECONDS));
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "INT96", timestampsWithPrecision(MICROSECONDS), timestampsWithPrecision(MICROSECONDS));
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "INT96", timestampsWithPrecision(NANOSECONDS), timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "TIMESTAMP_MICROS", timestampsWithPrecision(MILLISECONDS), timestampsWithPrecision(MILLISECONDS));
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "TIMESTAMP_MICROS", timestampsWithPrecision(MICROSECONDS), timestampsWithPrecision(MICROSECONDS));
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "TIMESTAMP_MICROS", timestampsWithPrecision(NANOSECONDS), timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "TIMESTAMP_MILLIS", timestampsWithPrecision(MILLISECONDS), timestampsWithPrecision(MILLISECONDS));
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "TIMESTAMP_MILLIS", timestampsWithPrecision(MICROSECONDS), timestampsWithPrecision(MICROSECONDS));
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(rebaseMode, "TIMESTAMP_MILLIS", timestampsWithPrecision(NANOSECONDS), timestampsWithPrecision(MICROSECONDS));
    }

    private void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampNtz(
            String rebaseMode,
            String sparkOutputTimestampType,
            List<String> timestamps,
            List<String> expectedTimestamps)
    {
        String deltaTableName = "test_spark_table_in_delta_on_parquet_legacy_timestamp_ntz_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onSpark().executeQuery("SET spark.sql.timestampType=TIMESTAMP_NTZ");
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=%s".formatted(rebaseMode));
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            }

            onSpark().executeQuery("CREATE TABLE %s.%s (tmst timestamp) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, toValues(timestamps)));

            assertThat(onSpark().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedTimestamps));
            assertThat(onDelta().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedTimestamps));
            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTimestamps));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampHybridCalendar()
    {
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestampProlepticCalendar()
    {
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp("CORRECTED");
    }

    private void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(String rebaseMode)
    {
        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(MICROSECONDS),
                roundToMillis(timestampsWithPrecision(MICROSECONDS)),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(NANOSECONDS),
                roundToMillis(timestampsWithPrecision(NANOSECONDS)),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));
    }

    private void testWriteInSparkToSparkTableOnDeltaWithLegacyTimestamp(
            String rebaseMode,
            String sparkOutputTimestampType,
            List<String> timestamps,
            List<String> expectedTrinoTimestamps,
            List<String> expectedSparkTimestamps,
            List<String> expectedDeltaTimestamps)
    {
        String deltaTableName = "test_spark_table_delta_on_parquet_legacy_timestamp_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=%s".formatted(rebaseMode));
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            }

            onSpark().executeQuery("CREATE TABLE %s.%s (tmst timestamp) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, toValues(timestamps)));

            assertThat(onSpark().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedSparkTimestamps));
            assertThat(onDelta().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedDeltaTimestamps));
            assertThat(onTrino().executeQuery("SELECT cast (tmst AS TIMESTAMP) FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTrinoTimestamps));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToDeltaTableWithLegacyTimestampNtzWithHybridCalendar()
    {
        testWriteInSparkToDeltaTableWithLegacyTimestampNtz("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToDeltaTableWithLegacyTimestampNtzWithProlepticCalendar()
    {
        testWriteInSparkToDeltaTableWithLegacyTimestampNtz("CORRECTED");
    }

    private void testWriteInSparkToDeltaTableWithLegacyTimestampNtz(String rebaseMode)
    {
        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));
    }

    private void testWriteInSparkToDeltaTableWithLegacyTimestampNtz(
            String rebaseMode,
            String sparkOutputTimestampType,
            List<String> timestamps,
            List<String> expectedTrinoTimestamps,
            List<String> expectedSparkTimestamps,
            List<String> expectedDeltaTimestamps)
    {
        String deltaTableName = "test_delta_on_parquet_legacy_timestamp_ntz_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=%s".formatted(rebaseMode));
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            }

            onDelta().executeQuery("CREATE TABLE %s.%s (tmst TIMESTAMP_NTZ) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, toValues(timestamps)));

            assertThat(onSpark().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedSparkTimestamps));
            assertThat(onDelta().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedDeltaTimestamps));
            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTrinoTimestamps));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToDeltaTableWithLegacyTimestampWithHybridCalendar()
    {
        testWriteInSparkToDeltaTableWithLegacyTimestamp("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testWriteInSparkToDeltaTableWithLegacyTimestampWithProlepticCalendar()
    {
        testWriteInSparkToDeltaTableWithLegacyTimestamp("CORRECTED");
    }

    private void testWriteInSparkToDeltaTableWithLegacyTimestamp(String rebaseMode)
    {
        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "INT96",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)),
                millisToMicros(timestampsWithPrecision(MILLISECONDS)));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MICROS",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testWriteInSparkToDeltaTableWithLegacyTimestamp(
                rebaseMode,
                "TIMESTAMP_MILLIS",
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));
    }

    private void testWriteInSparkToDeltaTableWithLegacyTimestamp(
            String rebaseMode,
            String sparkOutputTimestampType,
            List<String> timestamps,
            List<String> expectedTrinoTimestamps,
            List<String> expectedSparkTimestamps,
            List<String> expectedDeltaTimestamps)
    {
        String deltaTableName = "test_spark_to_delta_on_parquet_legacy_timestamp_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=%s".formatted(rebaseMode));
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            }

            onDelta().executeQuery("CREATE TABLE %s.%s (tmst TIMESTAMP) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onDelta().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, toValues(timestamps)));

            assertThat(onSpark().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedSparkTimestamps));
            assertThat(onDelta().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedDeltaTimestamps));
            assertThat(onTrino().executeQuery("SELECT cast(tmst AS TIMESTAMP) FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTrinoTimestamps));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_143, DELTA_LAKE_DATABRICKS_154, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testRegressionWriteInDeltaToDeltaTableWithLegacyTimestamp()
    {
        testRegressionWriteInDeltaToDeltaTableWithLegacyTimestamp(
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));

        testRegressionWriteInDeltaToDeltaTableWithLegacyTimestamp(
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testRegressionWriteInDeltaToDeltaTableWithLegacyTimestamp(
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MICROSECONDS));
    }

    private void testRegressionWriteInDeltaToDeltaTableWithLegacyTimestamp(
            List<String> timestamps,
            List<String> expectedTrinoTimestamps,
            List<String> expectedDeltaTimestamps)
    {
        String deltaTableName = "test_spark_to_delta_on_parquet_legacy_timestamp_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onDelta().executeQuery("CREATE TABLE %s.%s (tmst TIMESTAMP) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onDelta().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, toValues(timestamps)));

            assertThat(onDelta().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedDeltaTimestamps));
            assertThat(onTrino().executeQuery("SELECT cast(tmst AS TIMESTAMP) FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTrinoTimestamps));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS_154, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testRegressionWriteInDeltaToDeltaTableWithLegacyTimestampNtz()
    {
        testRegressionWriteInDeltaToDeltaTableWithLegacyTimestampNtz(
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS),
                timestampsWithPrecision(MILLISECONDS));

        testRegressionWriteInDeltaToDeltaTableWithLegacyTimestampNtz(
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));

        testRegressionWriteInDeltaToDeltaTableWithLegacyTimestampNtz(
                timestampsWithPrecision(NANOSECONDS),
                timestampsWithPrecision(MICROSECONDS),
                timestampsWithPrecision(MICROSECONDS));
    }

    private void testRegressionWriteInDeltaToDeltaTableWithLegacyTimestampNtz(
            List<String> timestamps,
            List<String> expectedTrinoTimestamps,
            List<String> expectedDeltaTimestamps)
    {
        String deltaTableName = "test_delta_on_parquet_legacy_timestamp_ntz_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onDelta().executeQuery("CREATE TABLE %s.%s (tmst TIMESTAMP_NTZ) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onDelta().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, toValues(timestamps)));

            assertThat(onDelta().executeQuery("SELECT tmst FROM " + deltaTableName)).containsOnly(toExpectedRows(expectedDeltaTimestamps));
            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName)).containsOnly(toExpectedRows(expectedTrinoTimestamps));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHandlePushdownWhenLegacyTimestampNtz()
    {
        testHandlePushdownWhenLegacyTimestampNtz("INT96");
        testHandlePushdownWhenLegacyTimestampNtz("TIMESTAMP_MICROS");
        testHandlePushdownWhenLegacyTimestampNtz("TIMESTAMP_MILLIS");
    }

    private void testHandlePushdownWhenLegacyTimestampNtz(String sparkOutputTimestampType)
    {
        String deltaTableName = "test_spark_on_delta_handle_pushdown_when_legacy_timestamp_ntz_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onSpark().executeQuery("SET spark.sql.timestampType=TIMESTAMP_NTZ");
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=LEGACY");
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY");
            }
            onDelta().executeQuery("CREATE TABLE %s.%s (tmst TIMESTAMP_NTZ) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES (TIMESTAMP '0001-01-01 11:12:13.456')".formatted(SCHEMA, deltaTableName));

            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE tmst = TIMESTAMP '0001-01-01 11:12:13.456'"))
                    .containsOnly(row(1));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHandlePushdownWhenLegacyTimestamp()
    {
        testHandlePushdownWhenLegacyTimestamp("INT96");
        testHandlePushdownWhenLegacyTimestamp("TIMESTAMP_MICROS");
        testHandlePushdownWhenLegacyTimestamp("TIMESTAMP_MILLIS");
    }

    private void testHandlePushdownWhenLegacyTimestamp(String sparkOutputTimestampType)
    {
        String deltaTableName = "test_spark_on_delta_handle_pushdown_when_legacy_timestamp_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType=%s".formatted(sparkOutputTimestampType));
            if (sparkOutputTimestampType.equals("INT96")) {
                onSpark().executeQuery("SET spark.sql.parquet.int96RebaseModeInWrite=LEGACY");
            }
            else {
                onSpark().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY");
            }
            onDelta().executeQuery("CREATE TABLE %s.%s (tmst TIMESTAMP) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onSpark().executeQuery("INSERT INTO %s.%s VALUES (TIMESTAMP '0001-01-01 11:12:13.456')".formatted(SCHEMA, deltaTableName));

            assertThat(onSpark().executeQuery("SELECT 1 FROM " + deltaTableName + " WHERE tmst = TIMESTAMP '0001-01-01 11:12:13.456'"))
                    .containsOnly(row(1));

            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE tmst = TIMESTAMP '0001-01-01 11:12:13.456 UTC'"))
                    .containsOnly(row(1));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }

    private static List<String> timestampsWithPrecision(Precision precision)
    {
        return TIMESTAMPS.stream()
                .map(timestamp -> generateTimestampsWithPrecision(timestamp, fractions(precision)))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private static List<String> millisToMicros(List<String> millis)
    {
        return millis.stream()
                .map(timestamp -> timestamp + "000")
                .collect(toImmutableList());
    }

    enum Precision
    {
        MILLISECONDS,
        MICROSECONDS,
        NANOSECONDS
    }

    private static ImmutableList<String> fractions(Precision precision)
    {
        return switch (precision) {
            case MILLISECONDS -> ImmutableList.of("123", "999");
            case MICROSECONDS -> ImmutableList.of("123456", "999999");
            case NANOSECONDS -> ImmutableList.of("123456789", "999999999");
        };
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

    private static List<String> roundToMillis(List<String> timestamps)
    {
        DateTimeFormatter inputFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME)
                .toFormatter();
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS");

        return timestamps.stream()
                .map(value -> {
                    LocalDateTime dateTime = LocalDateTime.parse(value, inputFormatter);
                    LocalDateTime rounded = dateTime.truncatedTo(MILLIS);
                    if ((dateTime.getNano() / 1000) % 1000 >= 500) {
                        rounded = rounded.plus(1, MILLIS);
                    }
                    return rounded.format(outputFormatter);
                })
                .collect(toImmutableList());
    }
}
