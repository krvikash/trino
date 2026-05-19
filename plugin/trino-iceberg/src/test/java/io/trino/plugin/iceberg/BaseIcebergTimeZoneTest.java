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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.INT_COL_NAME;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_PRECISION;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.variants.Variants.metadata;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergTimeZoneTest
        extends AbstractTestQueryFramework
{
    protected static final VariantMetadata EMPTY_METADATA = metadata(VariantTestUtil.emptyMetadata());
    protected static final String ICEBERG_WITHOUT_TIME_ZONE = "iceberg_without_time_zone_" + randomNameSuffix();
    protected static final String ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME = ICEBERG_WITHOUT_TIME_ZONE + ".default";

    private final IcebergFileFormat format;
    private final int formatVersion;

    private File dataDirectory;
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    protected BaseIcebergTimeZoneTest(IcebergFileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
        this.formatVersion = formatVersion();
    }

    protected abstract void writeVariantDataToIcebergTable(String outputFilePath, DataFiles.Builder fileBuilder, Table table, Variant... variantValues)
            throws IOException;

    protected int formatVersion()
    {
        return 3;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("iceberg_with_time_zone").toFile();
        closeAfterClass(() -> deleteRecursively(dataDirectory.toPath(), ALLOW_INSECURE));
        metastore = createTestingFileHiveMetastore(HDFS_FILE_SYSTEM_FACTORY, Location.of(dataDirectory.getAbsolutePath()));

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.format-version", Integer.toString(formatVersion))
                        .put("iceberg.legacy-variant-type-mapping", "JSON")
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.time-zone", "Asia/Kolkata")
                        .put("hive.metastore.catalog.dir", dataDirectory.getPath())
                        .buildOrThrow())
                .setMetastoreDirectory(dataDirectory)
                .setInitialTables(REGION)
                .build();
        fileSystemFactory = getFileSystemFactory(queryRunner);

        queryRunner.createCatalog(
                ICEBERG_WITHOUT_TIME_ZONE,
                "iceberg",
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                        .put("hive.metastore.catalog.dir", dataDirectory.getPath())
                        .put("fs.hadoop.enabled", "true")
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.format-version", Integer.toString(formatVersion))
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .buildOrThrow());
        queryRunner.execute("CREATE SCHEMA " + ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME);
        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("milliSecondsTimestampPrecision")
    void testSelectMicrosecondsTimestampTz(int precision)
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_",
                "(id int, timestamp_tz timestamp(%s) with time zone)".formatted(precision),
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00 UTC'") // basic utc
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.1 UTC'") // basic utc with microseconds
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.12 UTC'") // basic utc with milliseconds
                        .add("5, TIMESTAMP '2023-08-01 12:00:00.123 America/New_York'") // EDT
                        .add("6, TIMESTAMP '2023-08-01 12:00:00.1234 UTC'") // basic utc with milliseconds
                        .add("7, TIMESTAMP '2023-03-12 01:59:59.12345 America/Los_Angeles'") // Just before DST starts
                        .add("8, TIMESTAMP '2023-03-12 03:00:00.123456 America/Los_Angeles'") // Just after DST starts (skipped hour)
                        .add("9, TIMESTAMP '2023-11-05 01:59:59.1234567 America/Los_Angeles'") // Just before DST ends
                        .add("10, TIMESTAMP '2023-11-05 02:00:00.12345678 America/Los_Angeles'") // Just after DST ends
                        .add("11, TIMESTAMP '1947-08-15 00:00:00.123456789 UTC'") // Pre-epoch
                        .add("12, TIMESTAMP '1970-01-01 00:00:00.1234567891 UTC'") // Unix epoch
                        .add("13, TIMESTAMP '2038-01-19 03:14:07.12345678912 UTC'") // 32-bit int overflow boundary
                        .add("14, TIMESTAMP '1900-01-01 00:00:00.123456789123 UTC'") // Pre-epoch
                        .add("15, TIMESTAMP '2024-02-29 23:59:59.999999999999 UTC'") // Leap year
                        .add("16, TIMESTAMP '2023-10-01 00:00:00.999999999 Australia/Sydney'") // Southern hemisphere DST
                        .add("17, TIMESTAMP '2023-08-01 12:00:00.999999 UTC'")
                        .add("18, TIMESTAMP '1970-01-01 05:30:00.999 Asia/Kathmandu'")
                        .build())) {
            // with format-version = 3, precision <= 6 is mapped to timestamp(6) with time zone
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.000000 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 17:30:00.100000 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 17:30:00.120000 Asia/Kolkata')," +
                            "(5, TIMESTAMP '2023-08-01 21:30:00.123000 Asia/Kolkata')," +
                            "(6, TIMESTAMP '2023-08-01 17:30:00.123400 Asia/Kolkata')," +
                            "(7, TIMESTAMP '2023-03-12 15:29:59.123450 Asia/Kolkata')," +
                            "(8, TIMESTAMP '2023-03-12 15:30:00.123456 Asia/Kolkata')," +
                            "(9, TIMESTAMP '2023-11-05 14:29:59.123457 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(10, TIMESTAMP '2023-11-05 15:30:00.123457 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(11, TIMESTAMP '1947-08-15 05:30:00.123457 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(12, TIMESTAMP '1970-01-01 05:30:00.123457 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(13, TIMESTAMP '2038-01-19 08:44:07.123457 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(14, TIMESTAMP '1900-01-01 05:21:10.123457 Asia/Kolkata')," + // rounded-up to 6 decimals // +05:21:10
                            "(15, TIMESTAMP '2024-03-01 05:30:00.000000 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(16, TIMESTAMP '2023-09-30 19:30:01.000000 Asia/Kolkata')," + // rounded-up to 6 decimals
                            "(17, TIMESTAMP '2023-08-01 17:30:00.999999 Asia/Kolkata')," +
                            "(18, TIMESTAMP '1970-01-01 05:30:00.999000 Asia/Kolkata')");
        }
    }

    @ParameterizedTest
    @MethodSource("nanoSecondsTimestampPrecision")
    void testSelectNanoSecondsTimestampTz(int precision)
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_",
                "(id int, timestamp_tz timestamp(%s) with time zone)".formatted(precision),
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00 UTC'") // basic utc
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.1 UTC'") // basic utc with microseconds
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.12 UTC'") // basic utc with milliseconds
                        .add("5, TIMESTAMP '2023-08-01 12:00:00.123 America/New_York'") // EDT
                        .add("6, TIMESTAMP '2023-08-01 12:00:00.1234 UTC'") // basic utc with milliseconds
                        .add("7, TIMESTAMP '2023-03-12 01:59:59.12345 America/Los_Angeles'") // Just before DST starts
                        .add("8, TIMESTAMP '2023-03-12 03:00:00.123456 America/Los_Angeles'") // Just after DST starts (skipped hour)
                        .add("9, TIMESTAMP '2023-11-05 01:59:59.1234567 America/Los_Angeles'") // Just before DST ends
                        .add("10, TIMESTAMP '2023-11-05 02:00:00.12345678 America/Los_Angeles'") // Just after DST ends
                        .add("11, TIMESTAMP '1947-08-15 00:00:00.123456789 UTC'") // Pre-epoch
                        .add("12, TIMESTAMP '1970-01-01 00:00:00.1234567891 UTC'") // Unix epoch
                        .add("13, TIMESTAMP '2038-01-19 03:14:07.12345678912 UTC'") // 32-bit int overflow boundary
                        .add("14, TIMESTAMP '1900-01-01 00:00:00.123456789123 UTC'") // Pre-epoch
                        .add("15, TIMESTAMP '2024-02-29 23:59:59.999999999999 UTC'") // Leap year
                        .add("16, TIMESTAMP '2023-10-01 00:00:00.999999999 Australia/Sydney'") // Southern hemisphere DST
                        .add("17, TIMESTAMP '2023-08-01 12:00:00.999999 UTC'")
                        .add("18, TIMESTAMP '1970-01-01 05:30:00.999 Asia/Kathmandu'")
                        .build())) {
            // with format-version = 3 precision > 6 and <= 12 is mapped to timestamp(9) with time zone
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.000000000 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 17:30:00.100000000 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 17:30:00.120000000 Asia/Kolkata')," +
                            "(5, TIMESTAMP '2023-08-01 21:30:00.123000000 Asia/Kolkata')," +
                            "(6, TIMESTAMP '2023-08-01 17:30:00.123400000 Asia/Kolkata')," +
                            "(7, TIMESTAMP '2023-03-12 15:29:59.123450000 Asia/Kolkata')," +
                            "(8, TIMESTAMP '2023-03-12 15:30:00.123456000 Asia/Kolkata')," +
                            "(9, TIMESTAMP '2023-11-05 14:29:59.123456700 Asia/Kolkata')," +
                            "(10, TIMESTAMP '2023-11-05 15:30:00.123456780 Asia/Kolkata')," +
                            "(11, TIMESTAMP '1947-08-15 05:30:00.123456789 Asia/Kolkata')," +
                            "(12, TIMESTAMP '1970-01-01 05:30:00.123456789 Asia/Kolkata')," + // rounded-up to 9 decimals
                            "(13, TIMESTAMP '2038-01-19 08:44:07.123456789 Asia/Kolkata')," + // rounded-up to 9 decimals
                            "(14, TIMESTAMP '1900-01-01 05:21:10.123456789 Asia/Kolkata')," + // rounded-up to 9 decimals // +05:21:10
                            "(15, TIMESTAMP '2024-03-01 05:30:00.000000000 Asia/Kolkata')," + // rounded-up to 9 decimals
                            "(16, TIMESTAMP '2023-09-30 19:30:00.999999999 Asia/Kolkata')," +
                            "(17, TIMESTAMP '2023-08-01 17:30:00.999999000 Asia/Kolkata')," +
                            "(18, TIMESTAMP '1970-01-01 05:30:00.999000000 Asia/Kolkata')");
        }
    }

    static Stream<Integer> milliSecondsTimestampPrecision()
    {
        return IntStream.rangeClosed(1, TIMESTAMP_TZ_MICROS.getPrecision()).boxed();
    }

    static Stream<Integer> nanoSecondsTimestampPrecision()
    {
        return IntStream.rangeClosed(TIMESTAMP_TZ_MICROS.getPrecision() + 1, MAX_PRECISION).boxed();
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

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
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

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
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

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_map_",
                "(id int, map1 MAP(timestamp with time zone, timestamp with time zone))",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, map()")
                        .add("3, map(null, null)")
                        .add("4, map(null, array[null])")
                        .add("5, map(array[null], null)")
                        .add("6, map(array[], array[])")
                        .add("7, map(array[%s], null)".formatted(instance1Utc))
                        .add("8, map(array[%s], array[null])".formatted(instance1Utc))
                        .add("9, map(array[%1$s], array[%1$s])".formatted(instance1Utc))
                        .add("10, map(array[%s, %s, %s], array[null, null, null])".formatted(instance1Utc, instant2Utc, instant3Utc))
                        .add("11, map(array[%s, %s, %s], array[%s, null, null])".formatted(instance1Utc, instant1NewYork, instant3Utc, instant2Utc))
                        .add("12, map(array[%s, %s, %s], array[null, %s, null])".formatted(instance1Utc, instant2Paris, instant3Utc, instant2Utc))
                        .add("13, map(array[%s, %s, %s], array[null, null, %s])".formatted(instance1Utc, instant2Kolkata, instant3Utc, instant2Utc))
                        .add("14, map(array[%1$s, %2$s, %3$s], array[%1$s, %4$s, %1$s])".formatted(instance1Utc, instant2Tokyo, instant3Utc, instant1Tokyo))
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
                            "(7, null)," +
                            "(8, map(array[%s], array[null])),".formatted(instant1Kolkata) +
                            "(9, map(array[%1$s], array[%1$s])),".formatted(instant1Kolkata) +
                            "(10, map(array[%s, %s, %s], array[null, null, null])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(11, map(array[%1$s, %2$s, %3$s], array[%2$s, null, null])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(12, map(array[%1$s, %2$s, %3$s], array[null, %2$s, null])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(13, map(array[%1$s, %2$s, %3$s], array[null, null, %2$s])),".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata) +
                            "(14, map(array[%1$s, %2$s, %3$s], array[%1$s, %1$s, %1$s]))".formatted(instant1Kolkata, instant2Kolkata, instant3Kolkata));
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

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
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

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
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
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_insert_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2024-08-01 03:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2024-07-31 23:00:00.987654 America/New_York'")
                        .build())) {
            try (TestTable newTestTable = new TestTable(
                    getQueryRunner()::execute,
                    "test_timestamp_tz_insert_target_1_",
                    "(id int, timestamp_tz timestamp with time zone)")) {
                assertUpdate("INSERT INTO " + newTestTable.getName() + " SELECT * FROM " + testTable.getName(), 3);
                assertThat(query("SELECT * FROM " + newTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')," +
                                "(3, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')");
                assertThat(query("SELECT * FROM " + newTestTable.getName() + " WHERE timestamp_tz = TIMESTAMP '2024-08-01 12:00:00.987654 Asia/Tokyo'"))
                        .matches("VALUES " +
                                "(2, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')," +
                                "(3, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')");
            }

            try (TestTable newTestTable = new TestTable(
                    getQueryRunner()::execute,
                    ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME + ".test_timestamp_tz_insert_target_2_",
                    "(id int, timestamp_tz timestamp with time zone)")) {
                assertUpdate("INSERT INTO " + newTestTable.getName() + " SELECT * FROM " + testTable.getName(), 3);
                assertThat(query("SELECT * FROM " + newTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2024-08-01 03:00:00.987654 UTC')," +
                                "(3, TIMESTAMP '2024-08-01 03:00:00.987654 UTC')");
                assertThat(query("SELECT * FROM " + newTestTable.getName() + " WHERE timestamp_tz = TIMESTAMP '2024-08-01 12:00:00.987654 Asia/Tokyo'"))
                        .matches("VALUES " +
                                "(2, TIMESTAMP '2024-08-01 03:00:00.987654 UTC')," +
                                "(3, TIMESTAMP '2024-08-01 03:00:00.987654 UTC')");
            }
        }
    }

    @Test
    void testSelectTimestampTzOnRegisterTableWhenTimeZoneNotSet()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_when_tz_not_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata'")
                        .build())) {
            String tableLocation = getTableLocation("iceberg.tpch", testTable.getName());

            // Verify select from the ICEBERG_WITH_TIME_ZONE catalog where iceberg.time-zone is set to Asia/Kolkata
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987654 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987654 Asia/Kolkata'), " +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata')");

            // Verify select from catalog where iceberg.time-zone is not set
            Session sessionForCatalogWithoutTz = Session.builder(getSession())
                    .setCatalog(ICEBERG_WITHOUT_TIME_ZONE)
                    .setSchema("default")
                    .build();
            String tableName = "test_timestamp_tz_when_tz_not_set_target_" + randomNameSuffix();
            assertUpdate(sessionForCatalogWithoutTz, "CALL system.register_table('%s', '%s', '%s')".formatted("default", tableName, tableLocation));
            assertThat(query(sessionForCatalogWithoutTz, "SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987654 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987654 UTC')");
            assertUpdate(sessionForCatalogWithoutTz, "CALL system.unregister_table('%s', '%s')".formatted("default", tableName));
        }
    }

    @Test
    void testSelectTimestampTzOnRegisterTableWhenTimeZoneSet()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME + ".test_timestamp_tz_when_tz_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata'")
                        .build())) {
            String tableLocation = getTableLocation(ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME, testTable.getName().split("\\.")[2]);

            // Verify select from the 'iceberg' catalog where iceberg.time-zone is not set
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987654 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987654 UTC')");

            // Verify select from the ICEBERG_WITH_TIME_ZONE catalog where iceberg.time-zone is set to Asia/Kolkata
            Session sessionForCatalogWithTz = Session.builder(getSession())
                    .setCatalog("iceberg")
                    .setSchema("tpch")
                    .build();
            String tableName = "test_timestamp_tz_when_tz_set_target_" + randomNameSuffix();
            assertUpdate(sessionForCatalogWithTz, "CALL system.register_table('%s', '%s', '%s')".formatted("tpch", tableName, tableLocation));
            assertThat(query(sessionForCatalogWithTz, "SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987654 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987654 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata')");
            assertUpdate(sessionForCatalogWithTz, "CALL system.unregister_table('%s', '%s')".formatted("tpch", tableName));
        }
    }

    @Test
    void testSelectTimestampTzWithCtasWhenTimeZoneNotSet()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_ctas_when_tz_not_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata'")
                        .build())) {
            try (TestTable ctasTestTable = new TestTable(
                    getQueryRunner()::execute,
                    ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME + ".test_timestamp_tz_ctas_when_tz_not_set_target_",
                    "AS SELECT * FROM %s".formatted(testTable.getName()))) {
                // Verify select from the 'iceberg' catalog where iceberg.time-zone is not set
                assertThat(query("SELECT * FROM " + ctasTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC')," +
                                "(3, TIMESTAMP '2023-08-01 16:00:00.987654 UTC')," +
                                "(4, TIMESTAMP '2023-08-01 06:30:00.987654 UTC')");
            }
        }
    }

    @Test
    void testSelectTimestampTzWithCtasWhenTimeZoneSet()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME + ".test_timestamp_tz_ctas_when_tz_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata'")
                        .build())) {
            try (TestTable ctasTestTable = new TestTable(
                    getQueryRunner()::execute,
                    "test_timestamp_tz_ctas_when_tz_set_target_",
                    "AS SELECT * FROM %s".formatted(testTable.getName()))) {
                // Verify select from the catalog where iceberg.time-zone is set to Asia/Kolkata
                assertThat(query("SELECT * FROM " + ctasTestTable.getName()))
                        .matches("VALUES " +
                                "(1, null)," +
                                "(2, TIMESTAMP '2023-08-01 17:30:00.987654 Asia/Kolkata')," +
                                "(3, TIMESTAMP '2023-08-01 21:30:00.987654 Asia/Kolkata')," +
                                "(4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata')");
            }
        }
    }

    @Test
    void testSelectTimestampTzWithCreateOrReplaceWhenTimeZoneNotSet()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_create_or_replace_when_tz_not_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata'")
                        .build())) {
            String tableName = ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME + ".test_timestamp_tz_create_or_replace_when_tz_not_set_target_" + randomNameSuffix();
            assertUpdate("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s".formatted(tableName, testTable.getName()), 4);
            // Verify select from the 'iceberg' catalog where iceberg.time-zone is not set
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987654 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987654 UTC')");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testSelectTimestampTzWithCreateOrReplaceWhenTimeZoneSet()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                ICEBERG_WITHOUT_TIME_ZONE_CATALOG_SCHEMA_NAME + ".test_timestamp_tz_create_or_replace_when_tz_set_source_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                        .add("4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata'")
                        .build())) {
            String tableName = "test_timestamp_tz_create_or_replace_when_tz_set_target_" + randomNameSuffix();
            assertUpdate("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s".formatted(tableName, testTable.getName()), 4);
            // Verify select from the catalog where iceberg.time-zone is set to Asia/Kolkata
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 17:30:00.987654 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2023-08-01 21:30:00.987654 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata')");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testSelectTimestampTzPredicate()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_predicate_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2024-08-01 03:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2024-07-31 23:00:00.987654 America/New_York'")
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE timestamp_tz = TIMESTAMP '2024-08-01 12:00:00.987654 Asia/Tokyo'"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')");
        }
    }

    @Test
    void testSelectTimestampTzDateTrunc()
    {
        try (TestTable testTable = newTrinoTable(
                "test_timestamp_tz_with_date_trunc_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, TIMESTAMP '2020-05-10 12:34:56.987654 Asia/Kathmandu'")
                        .build())) {
            assertSelectTimestampTzDateTrunc(testTable.getName(), "millisecond", "TIMESTAMP '2020-05-10 12:19:56.987000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "second", "TIMESTAMP '2020-05-10 12:19:56.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "minute", "TIMESTAMP '2020-05-10 12:19:00.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "hour", "TIMESTAMP '2020-05-10 12:00:00.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "day", "TIMESTAMP '2020-05-10 00:00:00.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "week", "TIMESTAMP '2020-05-04 00:00:00.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "month", "TIMESTAMP '2020-05-01 00:00:00.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "quarter", "TIMESTAMP '2020-04-01 00:00:00.000000 Asia/Kolkata'");
            assertSelectTimestampTzDateTrunc(testTable.getName(), "year", "TIMESTAMP '2020-01-01 00:00:00.000000 Asia/Kolkata'");
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
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_predicate_with_date_trunc_",
                "(id int, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("1, null")
                        .add("2, TIMESTAMP '2024-08-01 03:00:00.987654 UTC'")
                        .add("3, TIMESTAMP '2024-07-31 23:59:59.987654 America/New_York'") // 2024-08-01 03:59:59.987654 UTC
                        .add("4, TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata'") // 2025-07-02 22:30:00.000000 UTC
                        .build())) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) IS NULL"))
                    .skippingTypesCheck()
                    .matches("VALUES (1, null)");
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) IS NOT NULL"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata')," +
                            "(3, TIMESTAMP '2024-08-01 09:29:59.987654 Asia/Kolkata')," +
                            "(4, TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata'"))
                    .matches("VALUES (4, TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata', TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 02:30:00.000000 UTC'"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata', TIMESTAMP '2024-08-01 08:00:00.000000 Asia/Kolkata')");
            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 03:30:00.000000 UTC'"))
                    .matches("VALUES " +
                            "(3, TIMESTAMP '2024-08-01 09:29:59.987654 Asia/Kolkata', TIMESTAMP '2024-08-01 09:00:00.000000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 08:00:00.000000 Asia/Kolkata'"))
                    .matches("VALUES " +
                            "(2, TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata', TIMESTAMP '2024-08-01 08:00:00.000000 Asia/Kolkata')");

            assertThat(query("SELECT *, date_trunc('hour', timestamp_tz) FROM " + testTable.getName() + " WHERE date_trunc('hour', timestamp_tz) = TIMESTAMP '2024-08-01 09:00:00.000000 Asia/Kolkata'"))
                    .matches("VALUES " +
                            "(3, TIMESTAMP '2024-08-01 09:29:59.987654 Asia/Kolkata', TIMESTAMP '2024-08-01 09:00:00.000000 Asia/Kolkata')");
        }
    }

    @Test
    void testSelectTimestampTzAfterMerge()
    {
        try (TestTable targetTable = new TestTable(
                getQueryRunner()::execute,
                "test_timestamp_tz_target_",
                "(key varchar, timestamp_tz timestamp with time zone)",
                ImmutableList.<String>builder()
                        .add("'k1', null")
                        .add("'delete-this-row', TIMESTAMP '2023-08-01 12:00:00.987654 UTC'")
                        .add("'update-this-row', TIMESTAMP '2024-08-01 12:00:00.987654 America/New_York'")
                        .build())) {
            try (TestTable sourceTable = new TestTable(
                    getQueryRunner()::execute,
                    "test_timestamp_tz_source_",
                    "(key varchar, timestamp_tz timestamp with time zone)",
                    ImmutableList.<String>builder()
                            .add("'deleted-row', TIMESTAMP '2023-08-01 21:00:00.987654 Asia/Tokyo'")
                            .add("'updated-row', TIMESTAMP '2024-08-01 12:00:00.987654 America/New_York'")
                            .add("'inserted-row', TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York'")
                            .build())) {
                assertUpdate(
                        """
                        MERGE INTO %s t USING %s s ON (t.timestamp_tz = s.timestamp_tz)
                        WHEN MATCHED AND t.key = 'update-this-row' THEN UPDATE SET key = 'updated-row', timestamp_tz = TIMESTAMP '1970-08-01 12:00:00.987654 UTC'
                        WHEN MATCHED THEN DELETE
                        WHEN NOT MATCHED THEN INSERT (key, timestamp_tz) VALUES(s.key, s.timestamp_tz)
                        """.formatted(targetTable.getName(), sourceTable.getName()),
                        3);
                assertThat(query("SELECT * FROM " + targetTable.getName()))
                        .skippingTypesCheck()
                        .matches("VALUES " +
                                "('k1', null)," +
                                "('updated-row', TIMESTAMP '1970-08-01 17:30:00.987654 Asia/Kolkata')," +
                                "('inserted-row', TIMESTAMP '2023-08-01 21:30:00.987654 Asia/Kolkata')");
            }
        }
    }

    @Test
    void testWriteDefaultValue()
    {
        testWriteDefaultValue("TIMESTAMP(6) WITH TIME ZONE", null, "CAST (null as TIMESTAMP(6) WITH TIME ZONE)");
        testWriteDefaultValue("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2024-08-01 03:00:00.987654 UTC'", "TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata'");
        testWriteDefaultValue("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2024-07-31 23:59:59.987654 America/New_York'", "TIMESTAMP '2024-08-01 09:29:59.987654 Asia/Kolkata'");
        testWriteDefaultValue("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata'", "TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata'");

        testWriteDefaultValue("TIMESTAMP(9) WITH TIME ZONE", null, "CAST (null as TIMESTAMP(9) WITH TIME ZONE)");
        testWriteDefaultValue("TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2024-08-01 03:00:00.987654321 UTC'", "TIMESTAMP '2024-08-01 08:30:00.987654321 Asia/Kolkata'");
        testWriteDefaultValue("TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2024-07-31 23:59:59.987654321 America/New_York'", "TIMESTAMP '2024-08-01 09:29:59.987654321 Asia/Kolkata'");
        testWriteDefaultValue("TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-07-03 04:00:00.000000000 Asia/Kolkata'", "TIMESTAMP '2025-07-03 04:00:00.000000000 Asia/Kolkata'");
    }

    private void testWriteDefaultValue(String type, String defaultValue, String expectedValue)
    {
        try (TestTable table = newTrinoTable("test_default_value", "(id int, data %s DEFAULT %s)".formatted(type, defaultValue))) {
            assertUpdate("INSERT INTO " + table.getName() + "(id) VALUES 1", 1);

            assertThat(query("SELECT data FROM " + table.getName()))
                    .as("%s type expected %s", type, defaultValue)
                    .matches("VALUES " + expectedValue);
        }
    }

    @Test
    void testInitialDefaultValue()
    {
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2024-08-01T03:00:00.987654+00:00"), "TIMESTAMP '2024-08-01 08:30:00.987654 Asia/Kolkata'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2024-07-31T23:59:59.987654-04:00"), "TIMESTAMP '2024-08-01 09:29:59.987654 Asia/Kolkata'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-07-03T04:00:00.000000+05:30"), "TIMESTAMP '2025-07-03 04:00:00.000000 Asia/Kolkata'");

        testInitialDefaultValue(format, Types.TimestampNanoType.withZone(), Literal.of("2024-08-01T03:00:00.987654321+00:00"), "TIMESTAMP '2024-08-01 08:30:00.987654321 Asia/Kolkata'");
        testInitialDefaultValue(format, Types.TimestampNanoType.withZone(), Literal.of("2024-07-31T23:59:59.987654321-04:00"), "TIMESTAMP '2024-08-01 09:29:59.987654321 Asia/Kolkata'");
        testInitialDefaultValue(format, Types.TimestampNanoType.withZone(), Literal.of("2025-07-03T04:00:00.000000000+05:30"), "TIMESTAMP '2025-07-03 04:00:00.000000000 Asia/Kolkata'");
    }

    private void testInitialDefaultValue(IcebergFileFormat format, Type type, Literal<?> defaultValue, @Language("SQL") String expectedValue)
    {
        try (TestTable table = newTrinoTable("test_initial_default", "WITH (format='" + format + "') AS SELECT 1 id")) {
            loadTable(table.getName()).updateSchema()
                    .addColumn("data", type, defaultValue)
                    .commit();

            assertThat(query("SELECT data FROM " + table.getName()))
                    .matches("VALUES " + expectedValue);
        }
    }

    @Test
    void testOptimizeProcedure()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "default.test_timestamp_tz_for_optimize",
                "(id int, timestamp_tz timestamp with time zone)")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, null)", 1);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC')", 1);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (3, TIMESTAMP '2023-08-01 12:00:00.987654 America/New_York')", 1);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (4, TIMESTAMP '2023-08-01 12:00:00.987654 Asia/Kolkata')", 1);

            assertUpdate("ALTER TABLE " + testTable.getName() + " EXECUTE optimize");

            // Verify select from the 'iceberg' catalog where iceberg.time-zone is not set
            assertThat(query("SELECT * FROM %s.%s".formatted(ICEBERG_WITHOUT_TIME_ZONE, testTable.getName())))
                    .matches("VALUES " +
                            "(1, null)," +
                            "(2, TIMESTAMP '2023-08-01 12:00:00.987654 UTC')," +
                            "(3, TIMESTAMP '2023-08-01 16:00:00.987654 UTC')," +
                            "(4, TIMESTAMP '2023-08-01 06:30:00.987654 UTC')");
        }
    }

    @Test
    void testSelectTimestampTzVariantType()
            throws Exception
    {
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")), "JSON '\"2024-11-07 12:33:54.123456+00:00\"'");
    }

    protected void testVariantTypeMappings(Variant variantData, @Language("SQL") String expectedVariant)
            throws Exception
    {
        String tableName = "test_variant_" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s (%s int, var json)".formatted(tableName, INT_COL_NAME));
        BaseTable table = loadTable(tableName);

        writeVariantDataToIcebergTable(
                dataDirectory + "/variant-%s.%s".formatted(randomNameSuffix(), format),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                variantData);

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, %s)".formatted(expectedVariant));
        assertThat(query("SELECT id FROM " + tableName + " WHERE var = " + expectedVariant)).matches("VALUES 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSelectTimestampTzMetadataColumn()
    {
        assertThat(computeActual("SELECT timezone(\"$file_modified_time\") FROM region").getOnlyColumnAsSet())
                .containsExactly("Asia/Kolkata");
    }

    private String getTableLocation(String catalogSchemaName, String tableName)
    {
        return (String) computeScalar("SELECT value FROM %s.\"%s$properties\" WHERE key = 'location'".formatted(catalogSchemaName, tableName));
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
