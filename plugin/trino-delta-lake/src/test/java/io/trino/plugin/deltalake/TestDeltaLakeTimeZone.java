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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeTimeZone
        extends AbstractTestQueryFramework
{
    private static final String DELTA_WITHOUT_TIME_ZONE_CATALOG_NAME = "delta_lake_without_time_zone_" + randomNameSuffix();
    private static final Session WITHOUT_TIME_ZONE_SESSION = testSessionBuilder()
            .setCatalog(DELTA_WITHOUT_TIME_ZONE_CATALOG_NAME)
            .setSchema(TPCH_SCHEMA)
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path metastoreDirectory = Files.createTempDirectory("deltalake-time-zone-test");
        closeAfterClass(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .put("fs.hadoop.enabled", "true")
                .put("delta.enable-non-concurrent-writes", "true")
                .put("delta.register-table-procedure.enabled", "true")
                .buildOrThrow();
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addDeltaProperties(ImmutableMap.<String, String>builder()
                        .putAll(properties)
                        .put("delta.time-zone", "Asia/Kolkata")
                        .buildOrThrow())
                .setInitialTables(ImmutableList.of(REGION))
                .build();

        queryRunner.createCatalog(
                DELTA_WITHOUT_TIME_ZONE_CATALOG_NAME,
                "delta_lake",
                properties);
        return queryRunner;
    }

    @Test
    void testSelectTimestampTz()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00 UTC'") // basic utc
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.123 UTC'") // basic utc with milliseconds
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.123 America/New_York'") // EDT
                        .add("5, TIMESTAMP '2023-03-12 01:59:59.123 America/Los_Angeles'") // Just before DST starts
                        .add("6, TIMESTAMP '2023-03-12 03:00:00.123 America/Los_Angeles'") // Just after DST starts (skipped hour)
                        .add("7, TIMESTAMP '2023-11-05 01:59:59.123 America/Los_Angeles'") // Just before DST ends
                        .add("8, TIMESTAMP '2023-11-05 02:00:00.123 America/Los_Angeles'") // Just after DST ends
                        .add("9, TIMESTAMP '1947-08-15 00:00:00.123 UTC'") // Pre-epoch
                        .add("10, TIMESTAMP '1970-01-01 00:00:00.123 UTC'") // Unix epoch
                        .add("11, TIMESTAMP '2038-01-19 03:14:07.123 UTC'") // 32-bit int overflow boundary
                        .add("12, TIMESTAMP '1900-01-01 00:00:00.123 UTC'") // Pre-epoch
                        .add("13, TIMESTAMP '2024-02-29 23:59:59.999 UTC'") // Leap year
                        .add("14, TIMESTAMP '2023-10-01 00:00:00.999 Australia/Sydney'") // Southern hemisphere DST
                        .add("15, TIMESTAMP '2023-08-01 12:00:00.999 UTC'")
                        .add("16, TIMESTAMP '1970-01-01 05:30:00.999 Asia/Kathmandu'")
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.000 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 17:30:00.123 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 21:30:00.123 Asia/Kolkata')," +
                            "(5, TIMESTAMP '2023-03-12 15:29:59.123 Asia/Kolkata')," +
                            "(6, TIMESTAMP '2023-03-12 15:30:00.123 Asia/Kolkata')," +
                            "(7, TIMESTAMP '2023-11-05 14:29:59.123 Asia/Kolkata')," +
                            "(8, TIMESTAMP '2023-11-05 15:30:00.123 Asia/Kolkata')," +
                            "(9, TIMESTAMP '1947-08-15 05:30:00.123 Asia/Kolkata')," +
                            "(10, TIMESTAMP '1970-01-01 05:30:00.123 Asia/Kolkata')," +
                            "(11, TIMESTAMP '2038-01-19 08:44:07.123 Asia/Kolkata')," +
                            "(12, TIMESTAMP '1900-01-01 05:21:10.123 Asia/Kolkata')," + // +05:21:10 (https://www.timeanddate.com/time/zone/india/kolkata?year=1900)
                            "(13, TIMESTAMP '2024-03-01 05:29:59.999 Asia/Kolkata')," +
                            "(14, TIMESTAMP '2023-09-30 19:30:00.999 Asia/Kolkata')," +
                            "(15, TIMESTAMP '2023-08-01 17:30:00.999 Asia/Kolkata')," +
                            "(16, TIMESTAMP '1970-01-01 05:30:00.999 Asia/Kolkata')");
        }
    }

    @Test
    void testSelectTimestampTzWithRowType()
    {
        String instantUtc = "TIMESTAMP '1970-01-01 00:00:00.123 UTC'";
        String instantNewYork = "TIMESTAMP '1969-12-31 19:00:00.123 America/New_York'"; // 1970-01-01 00:00:00.123 UTC
        String instantKathmandu = "TIMESTAMP '1970-01-01 05:30:00.123 Asia/Kathmandu'"; // 1970-01-01 00:00:00.123 UTC
        String instantLondon = "TIMESTAMP '1970-01-01 01:00:00.123 Europe/London'"; // 1970-01-01 00:00:00.123 UTC
        String instantTokyo = "TIMESTAMP '1970-01-01 09:00:00.123 Asia/Tokyo'"; // 1970-01-01 00:00:00.123 UTC
        String instantKolkata = "TIMESTAMP '1970-01-01 05:30:00.123000 Asia/Kolkata'"; // 1970-01-01 00:00:00.123 UTC

        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_row_",
                "(" +
                        "id int, " +
                        "row1 ROW(field timestamp with time zone), " +
                        "row2 ROW(field1 timestamp with time zone, field2 timestamp with time zone), " +
                        "row3 ROW(row31 ROW(field timestamp with time zone)), " +
                        "row4 ROW(row41 ROW(field1 timestamp with time zone, field2 timestamp with time zone))" +
                        ")",
                ImmutableList.<String>builder()
                        .add("1, null, null, null, null")
                        .add("2, row(null), row(null, null), row(null), row(null)")
                        .add("3, row(%1$s), row(null, %1$s), row(row(null)), row(row(null, null))".formatted(instantUtc))
                        .add("4, row(%1$s), row(%2$s, null), row(row(%3$s)), row(row(null, %1$s))".formatted(instantNewYork, instantUtc, instantKathmandu))
                        .add("5, row(%1$s), row(null, %2$s), row(row(%3$s)), row(row(%1$s, null))".formatted(instantLondon, instantUtc, instantTokyo))
                        .add("6, row(%1$s), row(%2$s, %2$s), row(row(%2$s)), row(row(%2$s, %3$s))".formatted(instantTokyo, instantUtc, instantKolkata))
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(1, null, null, null, null)," +
                            "(2, row(null), row(null, null), row(null), row(null))," +
                            "(3, row(%1$s), row(null, %1$s), row(row(null)), row(row(null, null))),".formatted(instantKolkata) +
                            "(4, row(%1$s), row(%1$s, null), row(row(%1$s)), row(row(null, %1$s))),".formatted(instantKolkata) +
                            "(5, row(%1$s), row(null, %1$s), row(row(%1$s)), row(row(%1$s, null))),".formatted(instantKolkata) +
                            "(6, row(%1$s), row(%1$s, %1$s), row(row(%1$s)), row(row(%1$s, %1$s)))".formatted(instantKolkata));
            assertThat(query("SELECT id, row2.field2, row3.row31, row4.row41.field1 FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(1, null, null, null)," +
                            "(2, null, null, null)," +
                            "(3, %s, row(null), null),".formatted(instantKolkata) +
                            "(4, null, row(%s), null),".formatted(instantKolkata) +
                            "(5, %1$s, row(%1$s), %1$s),".formatted(instantKolkata) +
                            "(6, %1$s, row(%1$s), %1$s)".formatted(instantKolkata));
        }
    }

    @Test
    void testSelectTimestampTzWithArrayType()
    {
        String instantUtc = "TIMESTAMP '1970-01-01 00:00:00.123 UTC'";
        String instantNewYork = "TIMESTAMP '1969-12-31 19:00:00.123 America/New_York'"; // 1970-01-01 00:00:00.123 UTC
        String instantTokyo = "TIMESTAMP '1970-01-01 09:00:00.123 Asia/Tokyo'"; // 1970-01-01 00:00:00.123 UTC
        String instantKolkata = "TIMESTAMP '1970-01-01 05:30:00.123000 Asia/Kolkata'"; // 1970-01-01 00:00:00.123 UTC

        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_array_",
                "(id int, array1 ARRAY(timestamp with time zone), array2 ARRAY(ARRAY(timestamp with time zone)))",
                ImmutableList.<String>builder()
                        .add("1, null, null")
                        .add("2, array[null], array[null]")
                        .add("3, array[], array[]")
                        .add("4, array[], array[array[]]")
                        .add("5, array[null, null], array[array[null]]")
                        .add("6, array[null, %s], array[array[null, %1$s]]".formatted(instantUtc))
                        .add("7, array[%1$s, null], array[array[%1$s, null]]".formatted(instantUtc))
                        .add("8, array[null, %1$s, null], array[array[null, %1$s, null]]".formatted(instantUtc))
                        .add("9, array[%1$s, %2$s], array[array[%1$s, %3$s]]".formatted(instantUtc, instantNewYork, instantTokyo))
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(1, null, null)," +
                            "(2, array[null], array[null])," +
                            "(3, array[], array[])," +
                            "(4, array[], array[array[]])," +
                            "(5, array[null, null], array[array[null]])," +
                            "(6, array[null, %1$s], array[array[null, %1$s]]),".formatted(instantKolkata) +
                            "(7, array[%1$s, null], array[array[%1$s, null]]),".formatted(instantKolkata) +
                            "(8, array[null, %s, null], array[array[null, %1$s, null]]),".formatted(instantKolkata) +
                            "(9, array[%1$s, %1$s], array[array[%1$s, %1$s]])".formatted(instantKolkata));
        }
    }

    @Test
    void testSelectTimestampTzWithMapType()
    {
        String instance1Utc = "TIMESTAMP '1970-01-01 00:00:00.123 UTC'";
        String instant1NewYork = "TIMESTAMP '1970-12-31 19:00:00.123 America/New_York'"; // 1970-01-01 00:00:00.123 UTC
        String instant1Tokyo = "TIMESTAMP '1970-01-01 09:00:00.123 Asia/Tokyo'"; // 1970-01-01 00:00:00.123 UTC
        String instant1Kolkata = "TIMESTAMP '1970-01-01 05:30:00.123000 Asia/Kolkata'"; // 1970-01-01 00:00:00.123 UTC

        String instant2Utc = "TIMESTAMP '1971-01-01 00:00:00.123 UTC'";
        String instant2Paris = "TIMESTAMP '1971-01-01 01:00:00.123 Europe/Paris'"; // 1971-01-01 00:00:00.123 UTC
        String instant2Tokyo = "TIMESTAMP '1971-01-01 09:00:00.123 Asia/Tokyo'"; // 1971-01-01 00:00:00.123 UTC
        String instant2Kolkata = "TIMESTAMP '1971-01-01 05:30:00.123 Asia/Kolkata'"; // 1971-01-01 00:00:00.123 UTC

        String instant3Utc = "TIMESTAMP '1972-01-01 00:00:00.123 UTC'";
        String instant3Kolkata = "TIMESTAMP '1972-01-01 05:30:00.123000 Asia/Kolkata'"; // 1972-01-01 00:00:00.123 UTC

        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_map_",
                "(id int, map1 MAP(timestamp with time zone, timestamp with time zone))",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, map()")
                        .add("3, map(null, null)")
                        .add("4, map(null, array[null])")
                        .add("5, map(array[null], null)")
                        .add("6, map(array[], array[])")
                        .add("7, map(array[%s], array[null])".formatted(instance1Utc))
                        .add("8, map(array[%1$s], array[%1$s])".formatted(instance1Utc))
                        .add("9, map(array[%s, %s, %s], array[null, null, null])".formatted(instance1Utc, instant2Utc, instant3Utc))
                        .add("10, map(array[%s, %s, %s], array[%s, null, null])".formatted(instance1Utc, instant1NewYork, instant3Utc, instant2Utc))
                        .add("11, map(array[%s, %s, %s], array[null, %s, null])".formatted(instance1Utc, instant2Paris, instant3Utc, instant2Utc))
                        .add("12, map(array[%s, %s, %s], array[null, null, %s])".formatted(instance1Utc, instant2Kolkata, instant3Utc, instant2Utc))
                        .add("13, map(array[%1$s, %2$s, %3$s], array[%1$s, %4$s, %1$s])".formatted(instance1Utc, instant2Tokyo, instant3Utc, instant1Tokyo))
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, map())," +
                            "(3, null)," +
                            "(4, null)," +
                            "(5, null)," +
                            "(6, map(array[], array[]))," +
                            "(7, map(array[%s], array[null])),".formatted(instant1Kolkata) +
                            "(8, map(array[%1$s], array[%1$s])),".formatted(instant1Kolkata) +
                            "(9, map(array[%s, %s, %s], array[null, null, null])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(10, map(array[%1$s, %2$s, %3$s], array[%2$s, null, null])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(11, map(array[%1$s, %2$s, %3$s], array[null, %2$s, null])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(12, map(array[%1$s, %2$s, %3$s], array[null, null, %2$s])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(13, map(array[%1$s, %2$s, %3$s], array[%1$s, %1$s, %1$s]))".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata));
        }
    }

    @Test
    void testSelectTimestampTzWithNestedMapType()
    {
        String instant1Utc = "TIMESTAMP '1970-01-01 00:00:00.123 UTC'";
        String instant1Kolkata = "TIMESTAMP '1970-01-01 05:30:00.123000 Asia/Kolkata'"; // 1970-01-01 00:00:00.123 UTC

        String instant2Utc = "TIMESTAMP '1971-01-01 00:00:00.123 UTC'";
        String instant2Kolkata = "TIMESTAMP '1971-01-01 05:30:00.123000 Asia/Kolkata'"; // 1971-01-01 00:00:00.123 UTC

        String instant3Utc = "TIMESTAMP '1972-01-01 00:00:00.123 UTC'";
        String instant3Kolkata = "TIMESTAMP '1972-01-01 05:30:00.123000 Asia/Kolkata'"; // 1972-01-01 00:00:00.123 UTC

        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_nested_map_",
                "(id int, map2 MAP(int, MAP(timestamp with time zone, timestamp with time zone)))",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, map()")
                        .add("3, map(null, null)")
                        .add("4, map(array[4], array[null])")
                        .add("5, map(array[5], array[map()])")
                        .add("6, map(array[6], array[map(array[], null)])")
                        .add("7, map(array[7], array[map(array[], array[])])")
                        .add("8, map(array[8], array[map(array[%s], array[null])])".formatted(instant1Utc))
                        .add("9, map(array[9], array[map(array[%s, %s, %s], array[null, null, null])])".formatted(instant1Utc, instant2Utc, instant3Utc))
                        .add("10, map(array[10], array[map(array[%1$s, %2$s, %3$s], array[%1$s, null, null])])".formatted(instant1Utc, instant2Utc, instant3Utc))
                        .add("11, map(array[11], array[map(array[%1$s, %2$s, %3$s], array[null, %1$s, null])])".formatted(instant1Utc, instant2Utc, instant3Utc))
                        .add("12, map(array[12], array[map(array[%1$s, %2$s, %3$s], array[null, null, %1$s])])".formatted(instant1Utc, instant2Utc, instant3Utc))
                        .add("13, map(array[13], array[map(array[%1$s, %2$s, %3$s], array[%1$s, %1$s, %1$s])])".formatted(instant1Utc, instant2Utc, instant3Utc))
                        .add("14, map(array[140, 141], array[map(array[%1$s], array[%2$s]), map(array[%1$s], array[%2$s])])".formatted(instant1Utc, instant2Utc))
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches(("VALUES " +
                            "(1, null)," +
                            "(2, map())," +
                            "(3, null)," +
                            "(4, map(array[4], array[null]))," +
                            "(5, map(array[5], array[map()]))," +
                            "(6, map(array[6], array[map(array[], null)]))," +
                            "(7, map(array[7], array[map(array[], array[])]))," +
                            "(8, map(array[8], array[map(array[%s], array[null])])),".formatted(instant1Kolkata) +
                            "(9, map(array[9], array[map(array[%s, %s, %s], array[null, null, null])])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(10, map(array[10], array[map(array[%1$s, %2$s, %3$s], array[%1$s, null, null])])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(11, map(array[11], array[map(array[%1$s, %2$s, %3$s], array[null, %1$s, null])])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(12, map(array[12], array[map(array[%1$s, %2$s, %3$s], array[null, null, %1$s])])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(13, map(array[13], array[map(array[%1$s, %2$s, %3$s], array[%1$s, %1$s, %1$s])])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(14, map(array[140, 141], array[map(array[%1$s], array[%2$s]), map(array[%1$s], array[%2$s])]))").formatted(instant1Kolkata, instant2Kolkata));
        }
    }

    @Test
    void testSelectTimestampTzWithNestedMixedType()
    {
        String instant1Utc = "TIMESTAMP '2023-01-01 10:00:00.123 UTC'";
        String instant1Kolkata = "TIMESTAMP '2023-01-01 15:30:00.123000 Asia/Kolkata'"; // 2023-01-01 10:00:00.123000 UTC

        String instant2NewYork = "TIMESTAMP '2023-01-01 10:00:00.456 America/New_York'"; // 2023-01-01 15:00:00.456000 UTC
        String instant2Kolkata = "TIMESTAMP '2023-01-01 20:30:00.456000 Asia/Kolkata'"; // 2023-01-01 15:00:00.456000 UTC

        String instant3La = "TIMESTAMP '2023-01-01 10:00:00.789 America/Los_Angeles'"; // 2023-01-01 18:00:00.789000 UTC
        String instant3Kolkata = "TIMESTAMP '2023-01-01 23:30:00.789000 Asia/Kolkata'"; // 2023-01-01 18:00:00.789000 UTC

        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_nested_mixed_",
                "(" +
                        "id int, " +
                        "c_array_of_row ARRAY(ROW(ts_field timestamp with time zone)), " +
                        "c_row_with_array ROW(arr_field ARRAY(timestamp with time zone)), " +
                        "c_map_to_array MAP(VARCHAR, ARRAY(timestamp with time zone)), " +
                        "c_array_of_map ARRAY(MAP(VARCHAR, timestamp with time zone)), " +
                        "c_row_with_map_of_row ROW(map_field MAP(VARCHAR, ROW(ts_field timestamp with time zone))), " +
                        "c_complex_nested ROW(outer_arr ARRAY(MAP(VARCHAR, ROW(inner_ts timestamp with time zone, inner_arr ARRAY(timestamp with time zone)))))" +
                        ")",
                ImmutableList.<String>builder()
                        .add("1, null, null, null, null, null, null")
                        .add("2, ARRAY[], ROW(ARRAY[]), MAP(), ARRAY[], ROW(MAP()), ROW(ARRAY[])")
                        .add("3, ARRAY[ROW(null)]," +
                                "ROW(ARRAY[null])," +
                                "MAP(ARRAY['key1'], ARRAY[ARRAY[null]])," +
                                "ARRAY[MAP(ARRAY['map_key1'], ARRAY[null])]," +
                                "ROW(MAP(ARRAY['outer_key'], ARRAY[ROW(null)]))," +
                                "ROW(ARRAY[MAP(ARRAY['complex_key'], ARRAY[ROW(null, ARRAY[null])])])")
                        .add("4, ARRAY[ROW(%s), ROW(%s)],".formatted(instant1Utc, instant2NewYork) +
                                "ROW(ARRAY[%s, %s]),".formatted(instant2NewYork, instant3La) +
                                "MAP(ARRAY['k1', 'k2'], ARRAY[ARRAY[%s], ARRAY[%s, %s]]),".formatted(instant1Utc, instant2NewYork, instant3La) +
                                "ARRAY[MAP(ARRAY['mk1'], ARRAY[%s]), MAP(ARRAY['mk2'], ARRAY[%s])],".formatted(instant2NewYork, instant3La) +
                                "ROW(MAP(ARRAY['ok1', 'ok2'], ARRAY[ROW(%s), ROW(%s)])),".formatted(instant1Utc, instant2NewYork) +
                                "ROW(ARRAY[MAP(ARRAY['ck1'], ARRAY[ROW(%1$s, ARRAY[%2$s, %3$s])]), MAP(ARRAY['ck2'], ARRAY[ROW(%3$s, ARRAY[])])])".formatted(instant1Utc, instant2NewYork, instant3La))
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES" +
                            " (1, null, null, null, null, null, null)," +
                            " (2, ARRAY[], ROW(ARRAY[]), MAP(), ARRAY[], ROW(MAP()), ROW(ARRAY[]))," +
                            " (3, ARRAY[ROW(null)]," +
                            "     ROW(ARRAY[null])," +
                            "     MAP(ARRAY['key1'], ARRAY[ARRAY[null]])," +
                            "     ARRAY[MAP(ARRAY['map_key1'], ARRAY[null])]," +
                            "     ROW(MAP(ARRAY['outer_key'], ARRAY[ROW(null)]))," +
                            "     ROW(ARRAY[MAP(ARRAY['complex_key'], ARRAY[ROW(null, ARRAY[null])])]))," +
                            " (4, ARRAY[ROW(%s), ROW(%s)],".formatted(instant1Kolkata, instant2Kolkata) +
                            "     ROW(ARRAY[%s, %s]),".formatted(instant2Kolkata, instant3Kolkata) +
                            "     MAP(ARRAY['k1', 'k2'], ARRAY[ARRAY[%s], ARRAY[%s, %s]]),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "     ARRAY[MAP(ARRAY['mk1'], ARRAY[%s]), MAP(ARRAY['mk2'], ARRAY[%s])],".formatted(instant2Kolkata, instant3Kolkata) +
                            "     ROW(MAP(ARRAY['ok1', 'ok2'], ARRAY[ROW(%s), ROW(%s)])),".formatted(instant1Kolkata, instant2Kolkata) +
                            "     ROW(ARRAY[MAP(ARRAY['ck1'], ARRAY[ROW(%1$s, ARRAY[%2$s, %3$s])]), MAP(ARRAY['ck2'], ARRAY[ROW(%3$s, ARRAY[])])]))".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata));

            assertThat(query("SELECT t.id, value.inner_ts FROM " + testTable.getName() +
                    " t, UNNEST(t.c_complex_nested.outer_arr) AS u(map_element), UNNEST(map_element) AS m(key, value)"))
                    .skippingTypesCheck()
                    .matches(("VALUES" +
                            " (3, null)," +
                            " (4, %1$s),".formatted(instant3Kolkata) +
                            " (4, %1$s)".formatted(instant1Kolkata)));
        }
    }

    @Test
    void testSelectTimestampTzWhenInsertedUsingInsert()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_insert_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2024-08-01 03:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2024-07-31 23:00:00.987 America/New_York'")
                        .build())) {
            try (TestTable newTestTable = newTrinoTable(
                    "test_timestamp_tz_insert_target_1_",
                    "(id int, timestamp_tz timestamp with time zone)")) {
                assertUpdate("INSERT INTO " + newTestTable.getName() + " SELECT * FROM " + testTable.getName(), 3);
                assertThat(query("SELECT * FROM " + newTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')," +
                                "(3, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')");
                assertThat(query("SELECT * FROM " + newTestTable.getName() + " WHERE timestamp_tz = TIMESTAMP '2024-08-01 12:00:00.987 Asia/Tokyo'"))
                        .matches("VALUES " +
                                "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')," +
                                "(3, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')");
            }

            try (TestTable newTestTable = newTrinoTable(
                    tablePrefixInCatalogWithoutTimeZone("test_timestamp_tz_insert_target_2_"),
                    "(id int, timestamp_tz timestamp with time zone)")) {
                assertUpdate("INSERT INTO " + newTestTable.getName() + " SELECT * FROM " + testTable.getName(), 3);
                assertThat(query("SELECT * FROM " + newTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2024-08-01 03:00:00.987 UTC')," +
                                "(3, TIMESTAMP '2024-08-01 03:00:00.987 UTC')");
                assertThat(query("SELECT * FROM " + newTestTable.getName() + " WHERE timestamp_tz = TIMESTAMP '2024-08-01 12:00:00.987 Asia/Tokyo'"))
                        .matches("VALUES " +
                                "(2, TIMESTAMP '2024-08-01 03:00:00.987 UTC')," +
                                "(3, TIMESTAMP '2024-08-01 03:00:00.987 UTC')");
            }
        }
    }

    @Test
    void testSelectTimestampTzOnRegisterTableWhenTimeZoneNotSet()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_when_tz_not_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata'")
                        .build())) {
            String tableLocation = getTableLocation(testTable.getName());

            // Verify select from the delta catalog where delta.time-zone is set to Asia/Kolkata
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987 Asia/Kolkata'), " +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata')");

            // Verify select from catalog where delta.time-zone is not set
            String tableName = "test_timestamp_tz_when_tz_not_set_target_" + randomNameSuffix();
            assertUpdate(WITHOUT_TIME_ZONE_SESSION, "CALL system.register_table('%s', '%s', '%s')".formatted(TPCH_SCHEMA, tableName, tableLocation));
            assertThat(query(WITHOUT_TIME_ZONE_SESSION, "SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987 UTC')");
            assertUpdate(WITHOUT_TIME_ZONE_SESSION, "CALL system.unregister_table('%s', '%s')".formatted(TPCH_SCHEMA, tableName));
        }
    }

    @Test
    void testSelectTimestampTzOnRegisterTableWhenTimeZoneSet()
    {
        try (TestTable testTable = newTrinoTable(
                tablePrefixInCatalogWithoutTimeZone("test_timestamp_tz_when_tz_set_source_"),
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata'")
                        .build())) {
            String tableLocation = getTableLocation(testTable.getName());

            // Verify select from the delta catalog where delta.time-zone is not set
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987 UTC')");

            // Verify select from the delta catalog where delta.time-zone is set to Asia/Kolkata
            String tableName = "test_timestamp_tz_when_tz_set_target_" + randomNameSuffix();
            assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(TPCH_SCHEMA, tableName, tableLocation));
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata')");
            assertUpdate("CALL system.unregister_table('%s', '%s')".formatted(TPCH_SCHEMA, tableName));
        }
    }

    @Test
    void testSelectTimestampTzWithCtasWhenTimeZoneNotSet()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_ctas_when_tz_not_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata'")
                        .build())) {
            try (TestTable ctasTestTable = newTrinoTable(
                    tablePrefixInCatalogWithoutTimeZone("test_timestamp_tz_ctas_when_tz_not_set_target_"),
                    "AS SELECT * FROM %s".formatted(testTable.getName()))) {
                // Verify select from the delta catalog where delta.time-zone is not set
                assertThat(query("SELECT * FROM " + ctasTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2023-08-01 12:00:00.987 UTC')," +
                                "(3, TIMESTAMP '2023-08-01 16:00:00.987 UTC')," +
                                "(4, TIMESTAMP '2023-08-01 06:30:00.987 UTC')");
            }
        }
    }

    @Test
    void testSelectTimestampTzWithCtasWhenTimeZoneSet()
    {
        try (TestTable testTable = newTrinoTable(
                tablePrefixInCatalogWithoutTimeZone("test_timestamp_tz_ctas_when_tz_set_source_"),
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata'")
                        .build())) {
            try (TestTable ctasTestTable = newTrinoTable(
                    "test_timestamp_tz_ctas_when_tz_set_target_",
                    "AS SELECT * FROM %s".formatted(testTable.getName()))) {
                // Verify select from the catalog where delta.time-zone is set to Asia/Kolkata
                assertThat(query("SELECT * FROM " + ctasTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2023-08-01 17:30:00.987 Asia/Kolkata')," +
                                "(3, TIMESTAMP '2023-08-01 21:30:00.987 Asia/Kolkata')," +
                                "(4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata')");
            }
        }
    }

    @Test
    void testSelectTimestampTzWithCreateOrReplaceWhenTimeZoneNotSet()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_create_or_replace_when_tz_not_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata'")
                        .build())) {
            String targetTableName = "test_timestamp_tz_create_or_replace_when_tz_not_set_target_" + randomNameSuffix();
            assertUpdate(WITHOUT_TIME_ZONE_SESSION, "CREATE OR REPLACE TABLE %s AS SELECT * FROM delta.tpch.%s".formatted(targetTableName, testTable.getName()), 4);
            // Verify select from the delta catalog where delta.time-zone is not set
            assertThat(query(WITHOUT_TIME_ZONE_SESSION, "SELECT * FROM " + targetTableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987 UTC')");
            assertUpdate(WITHOUT_TIME_ZONE_SESSION, "DROP TABLE " + targetTableName);
        }
    }

    @Test
    void testSelectTimestampTzWithCreateOrReplaceWhenTimeZoneSet()
    {
        try (TestTable testTable = newTrinoTable(
                tablePrefixInCatalogWithoutTimeZone("test_timestamp_tz_create_or_replace_when_tz_set_source_"),
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata'")
                        .build())) {
            String tableName = "test_timestamp_tz_create_or_replace_when_tz_set_target_" + randomNameSuffix();
            assertUpdate("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s".formatted(tableName, testTable.getName()), 4);
            // Verify select from the catalog where delta.time-zone is set to Asia/Kolkata
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata')");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testSelectTimestampTzWithTimeZoneSetInSession()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_session_timezone_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, TIMESTAMP '2024-08-01 03:00:00.987 UTC'")
                        .add("2, TIMESTAMP '2024-07-31 23:00:00.987 America/New_York'")
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')," +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')");
            assertThat(query(WITHOUT_TIME_ZONE_SESSION, "SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, TIMESTAMP '2024-08-01 03:00:00.987 UTC')," +
                            "(2, TIMESTAMP '2024-08-01 03:00:00.987 UTC')");

            // the current time zone in the Trino session does not influence
            // the reading of timestamp with time zone columns from Delta Lake tables
            assertThat(query("WITH SESSION time_zone_id = 'America/Los_Angeles' SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')," +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')");
            assertThat(query(WITHOUT_TIME_ZONE_SESSION, "WITH SESSION time_zone_id = 'America/Los_Angeles' SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, TIMESTAMP '2024-08-01 03:00:00.987 UTC')," +
                            "(2, TIMESTAMP '2024-08-01 03:00:00.987 UTC')");
        }
    }

    @Test
    void testSelectTimestampTzPredicate()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_predicate_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2024-08-01 03:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2024-07-31 23:00:00.987 America/New_York'")
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE timestamp_tz = TIMESTAMP '2024-08-01 12:00:00.987 Asia/Tokyo'"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')");
        }
    }

    @Test
    void testSelectTimestampTzDateTrunc()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_with_date_trunc_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, TIMESTAMP '2020-05-10 12:34:56.9876 Asia/Kathmandu'")
                        .build())) {
            assertSelectTimestampTzDateTrunc(testTable.getName(), "millisecond", "TIMESTAMP '2020-05-10 12:19:56.988 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "second", "TIMESTAMP '2020-05-10 12:19:56.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "minute", "TIMESTAMP '2020-05-10 12:19:00.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "hour", "TIMESTAMP '2020-05-10 12:00:00.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "day", "TIMESTAMP '2020-05-10 00:00:00.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "week", "TIMESTAMP '2020-05-04 00:00:00.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "month", "TIMESTAMP '2020-05-01 00:00:00.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "quarter", "TIMESTAMP '2020-04-01 00:00:00.000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "year", "TIMESTAMP '2020-01-01 00:00:00.000 Asia/Kolkata'");
        }
    }

    void assertSelectTimestampTzDateTrunc(String tableName, String unit, String expected)
    {
        assertThat(query("SELECT id, date_trunc('%s', timestamp_tz) FROM %s".formatted(unit, tableName)))
                .matches("VALUES (1, %s)".formatted(expected));
        assertThat(query("SELECT id FROM %s WHERE date_trunc('%s', timestamp_tz) = %s".formatted(tableName, unit, expected)))
                .matches("VALUES 1");
    }

    @Test
    void testSelectTimestampTzPredicateWithDateTrunc()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_predicate_with_date_trunc_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2024-08-01 03:00:00.987 UTC'")
                        .add("3, TIMESTAMP '2024-07-31 23:59:59.987 America/New_York'") // 2024-08-01 03:59:59.987 UTC
                        .add("4, TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata'") // 2025-07-02 22:30:00.000 UTC
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) IS NULL"))
                    .skippingTypesCheck()
                    .matches("VALUES (1, null)");
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) IS NOT NULL"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2025-07-03 04:00:00.0000000 Asia/Kolkata'"))
                    .matches("VALUES (4, TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 02:30:00.0000000 UTC'"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata', TIMESTAMP '2024-08-01 08:00:00.000 Asia/Kolkata')");
            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 03:30:00.0000000 UTC'"))
                    .matches("VALUES " +
                            "(3, TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', TIMESTAMP '2024-08-01 09:00:00.000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 08:00:00.0000000 Asia/Kolkata'"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata', TIMESTAMP '2024-08-01 08:00:00.000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 09:00:00.0000000 Asia/Kolkata'"))
                    .matches("VALUES " +
                            "(3, TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', TIMESTAMP '2024-08-01 09:00:00.000 Asia/Kolkata')");
        }
    }

    @Test
    void testSelectTimestampTzAfterMerge()
    {
        try (TestTable targetTable = newTrinoTable(
                "test_timestamp_tz_target_",
                "(key varchar, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("'k1', null")
                        .add("'delete-this-row', TIMESTAMP '2023-08-01 12:00:00.987 UTC'")
                        .add("'update-this-row', TIMESTAMP '2024-08-01 12:00:00.987 America/New_York'")
                        .build())) {
            try (TestTable sourceTable = newTrinoTable(
                    "test_timestamp_tz_source_",
                    "(key varchar, timestamp_tz timestamp with time zone)",
                    ImmutableList.<String>builder()
                            .add("'deleted-row', TIMESTAMP '2023-08-01 21:00:00.987 Asia/Tokyo'")
                            .add("'updated-row', TIMESTAMP '2024-08-01 12:00:00.987 America/New_York'")
                            .add("'inserted-row', TIMESTAMP '2023-08-01 12:00:00.987 America/New_York'")
                            .build())) {
                assertUpdate("""
                            MERGE INTO %s t USING %s s ON (t.timestamp_tz = s.timestamp_tz)
                            WHEN MATCHED AND t.key = 'update-this-row' THEN UPDATE SET key = 'updated-row', timestamp_tz = TIMESTAMP '1970-08-01 12:00:00.987 UTC'
                            WHEN MATCHED THEN DELETE
                            WHEN NOT MATCHED THEN INSERT (key, timestamp_tz) VALUES(s.key, s.timestamp_tz)
                            """.formatted(targetTable.getName(), sourceTable.getName()),
                        3);
                assertThat(query("SELECT * FROM " + targetTable.getName()))
                        .skippingTypesCheck()
                        .matches("VALUES " +
                                "('k1', null)," +
                                "('updated-row', TIMESTAMP '1970-08-01 17:30:00.987 Asia/Kolkata')," +
                                "('inserted-row', TIMESTAMP '2023-08-01 21:30:00.987 Asia/Kolkata')");
            }
        }
    }

    @Test
    void testOptimizeProcedure()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_for_optimize",
                "(id int, timestamp_tz timestamp with time zone)")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, null)", 1);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (2, TIMESTAMP '2023-08-01 12:00:00.987 UTC')", 1);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (3, TIMESTAMP '2023-08-01 12:00:00.987 America/New_York')", 1);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata')", 1);

            assertUpdate("ALTER TABLE " + testTable.getName() + " EXECUTE optimize");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987 Asia/Kolkata')");
        }
    }

    @Test
    void testSelectTimestampTzVariantType()
    {
        try (TestTable table = newTrinoTable(
                "test_variant",
                "(variant JSON)",
                List.of(
                        "JSON '{\"ts\": \"1957-11-07T12:33:54.123456+09:00\"}'",
                        "JSON '{\"ts\": null}'",
                        "JSON '{}'",
                        "null"))) {
            // the time zone will not impact the raw JSON value
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES JSON '{\"ts\": \"1957-11-07T12:33:54.123456+09:00\"}', JSON '{}', JSON '{\"ts\": null}', null");
            assertThat(query(WITHOUT_TIME_ZONE_SESSION, "SELECT * FROM " + table.getName()))
                    .matches("VALUES JSON '{\"ts\": \"1957-11-07T12:33:54.123456+09:00\"}', JSON '{}', JSON '{\"ts\": null}', null");

            // with from_iso8601_timestamp, the time zone will be applied
            assertThat(query("SELECT from_iso8601_timestamp(json_extract_scalar(variant, '$.ts')) FROM " + table.getName()))
                    .matches("VALUES TIMESTAMP '1957-11-07 12:33:54.123+09:00', null, null, null");

            // with at_timezone
            assertThat(query("SELECT at_timezone(from_iso8601_timestamp(json_extract_scalar(variant, '$.ts')), 'Asia/Kolkata') FROM " + table.getName()))
                    .matches("VALUES TIMESTAMP '1957-11-07 09:03:54.123 Asia/Kolkata', null, null, null");

            assertThat(query("SELECT 'true' FROM " + table.getName() + " WHERE variant = JSON '{\"ts\": \"1957-11-07T12:33:54.123456+09:00\"}'"))
                    .matches("VALUES 'true'");
            // with at_timezone
            assertThat(query("SELECT 'true' FROM " + table.getName() +
                    " WHERE at_timezone(from_iso8601_timestamp(json_extract_scalar(variant, '$.ts')), 'Asia/Kolkata') = TIMESTAMP '1957-11-07 03:33:54.123 UTC'"))
                    .matches("VALUES 'true'");
        }
    }

    @Test
    public void testTimeZoneWithCdfChanges()
    {
        try (TestTable testTable = newTrinoTable(
                "test_basic_operations_on_table_with_cdf_enabled_",
                "(page_url VARCHAR, ts TIMESTAMP WITH TIME ZONE, views INTEGER) WITH (change_data_feed_enabled = true)")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES" +
                    "('url1', TIMESTAMP '2024-08-01 03:00:00.987 UTC', 1)," +
                    "('url2', TIMESTAMP '2024-07-31 23:59:59.987 America/New_York', 2)," +
                    "('url3', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', 3)",
                    3);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES" +
                            "('url4', TIMESTAMP '2024-08-01 03:00:00.987 UTC', 4)," +
                            "('url5', TIMESTAMP '2024-07-31 23:59:59.987 America/New_York', 2)," +
                            "('url6', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', 6)",
                    3);
            assertUpdate("UPDATE " + testTable.getName() + " SET page_url = 'url22' WHERE views = 2", 2);

            assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + testTable.getName() + "'))",
                    """
                            VALUES
                                ('url1', TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata', 1, 'insert', BIGINT '1'),
                                ('url2', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'insert', BIGINT '1'),
                                ('url3', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', 3, 'insert', BIGINT '1'),
                                ('url4', TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata', 4, 'insert', BIGINT '2'),
                                ('url5', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'insert', BIGINT '2'),
                                ('url6', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', 6, 'insert', BIGINT '2'),
                                ('url2', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_preimage', BIGINT '3'),
                                ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_postimage', BIGINT '3'),
                                ('url5', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_preimage', BIGINT '3'),
                                ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_postimage', BIGINT '3')
                            """);
            assertUpdate("DELETE FROM " + testTable.getName() + " WHERE views = 2", 2);
            assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + testTable.getName() + "', 3))",
                """
                        VALUES
                            ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'delete', BIGINT '4'),
                            ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'delete', BIGINT '4')
                        """);

            assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + testTable.getName() + "')) ORDER BY _commit_version, _change_type, ts",
                    """
                            VALUES
                                ('url1', TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata', 1, 'insert', BIGINT '1'),
                                ('url2', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'insert', BIGINT '1'),
                                ('url3', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', 3, 'insert', BIGINT '1'),
                                ('url4', TIMESTAMP '2024-08-01 08:30:00.987 Asia/Kolkata', 4, 'insert', BIGINT '2'),
                                ('url5', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'insert', BIGINT '2'),
                                ('url6', TIMESTAMP '2025-07-03 04:00:00.000 Asia/Kolkata', 6, 'insert', BIGINT '2'),
                                ('url2', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_preimage', BIGINT '3'),
                                ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_postimage', BIGINT '3'),
                                ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_postimage', BIGINT '3'),
                                ('url5', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'update_preimage', BIGINT '3'),
                                ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'delete', BIGINT '4'),
                                ('url22', TIMESTAMP '2024-08-01 09:29:59.987 Asia/Kolkata', 2, 'delete', BIGINT '4')
                            """);
        }
    }

    private void assertTableChangesQuery(String sql, String expectedResult)
    {
        assertThat(query(sql))
                .result()
                .exceptColumns("_commit_timestamp")
                .skippingTypesCheck()
                .matches(expectedResult);
    }

    @Test
    void testSelectTimestampTzMetadataColumn()
    {
        assertThat(computeActual("SELECT timezone(\"$file_modified_time\") FROM region").getOnlyColumnAsSet())
                .containsExactly("Asia/Kolkata");
    }

    @Test
    void testHistoryTable()
    {
        // Verify that the timezone metadata column in the $history table reflects the session time zone, not the delta.time-zone property
        assertThat(computeActual("SELECT timezone(timestamp) FROM \"region$history\"").getOnlyColumnAsSet())
                .containsExactly(getSession().getTimeZoneKey().getId());
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private String tablePrefixInCatalogWithoutTimeZone(String tableName)
    {
        return "%s.%s.%s".formatted(DELTA_WITHOUT_TIME_ZONE_CATALOG_NAME, TPCH_SCHEMA, tableName);
    }
}
