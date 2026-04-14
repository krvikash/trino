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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetTestUtils;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHiveGpuQueriesTest
        extends AbstractTestQueryFramework
{
    protected abstract Location newExternalTableLocation();

    @Test
    public void testAllTypes()
    {
        // Create table with all types (both supported and unsupported)
        assertUpdate("CREATE TABLE test_gpu_types AS SELECT " +
                "CAST(true AS boolean) AS col_boolean, " +
                "CAST(127 AS tinyint) AS col_tinyint, " +
                "CAST(32767 AS smallint) AS col_smallint, " +
                "CAST(2147483647 AS integer) AS col_integer, " +
                "CAST(9223372036854775807 AS bigint) AS col_bigint, " +
                "CAST(3.14 AS real) AS col_real, " +
                "CAST(3.141592653589793 AS double) AS col_double, " +
                "CAST('hello' AS varchar) AS col_varchar, " +
                "CAST(12.345 AS decimal(5,3)) AS col_decimal, " +
                "CAST(12345678901234567890123.5678 AS decimal(27,4)) AS col_long_decimal, " +
                "DATE '2024-01-01' AS col_date, " +
                "CAST(TIMESTAMP '2020-02-12 15:03:00' AS timestamp(3)) AS col_timestamp, " +
                "X'12ab3f' AS col_varbinary, " +
                "CAST('abc' AS char(3)) AS col_char", 1);

        // Verify all supported types execute with GPU
        assertThat(query("SELECT col_boolean FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_tinyint FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_smallint FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_integer FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_bigint FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_real FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_double FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_varchar FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_decimal FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_date FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT col_timestamp FROM test_gpu_types"))
                .executesWithGpu(TableScanNode.class);

        // Verify all unsupported types execute without GPU
        assertThat(query("SELECT col_long_decimal FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_varbinary FROM test_gpu_types"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_char FROM test_gpu_types"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_types");
    }

    @Test
    public void testShortDecimalVariants()
    {
        // Trino's Hive connector writes decimals as FIXED_LEN_BYTE_ARRAY; cuDF narrows the on-read
        // type by precision: DECIMAL32 for precision ≤9, DECIMAL64 for 10-18, DECIMAL128 beyond.
        // Short-decimal columns in Trino always expect DECIMAL64, so d_small (decimal(5,3)) goes
        // through the DECIMAL32→DECIMAL64 widening cast and d_large (decimal(18,4)) is an exact
        // match. Include negative, zero and null values.
        assertUpdate("CREATE TABLE test_gpu_decimals AS SELECT * FROM (VALUES " +
                "(CAST(1.23 AS decimal(5,3)), CAST(123456789.1234 AS decimal(18,4))), " +
                "(CAST(-9.999 AS decimal(5,3)), CAST(-999999999999.9999 AS decimal(18,4))), " +
                "(CAST(0 AS decimal(5,3)), CAST(0 AS decimal(18,4))), " +
                "(CAST(NULL AS decimal(5,3)), CAST(NULL AS decimal(18,4)))) " +
                "t(d_small, d_large)", 4);

        // executesWithGpu cross-checks GPU output against CPU execution, so a wrong DECIMAL32→
        // DECIMAL64 cast (e.g. scale off by a power of 10) would fail the comparison.
        assertThat(query("SELECT d_small, d_large FROM test_gpu_decimals"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_decimals");
    }

    @Test
    public void testInt32AndInt64BackedDecimalRead()
            throws IOException
    {
        // Pin the INT32/INT64 on-disk encoding paths explicitly via ParquetTestUtils. Trino's
        // Hive connector only writes FIXED_LEN_BYTE_ARRAY (though cuDF happens to narrow it to
        // DECIMAL32/DECIMAL64 by precision, coinciding with this path), so covering INT32/INT64
        // here directly guards against cuDF behavior changes. Non-legacy encoding used by the
        // Parquet writer library: precision ≤9 → INT32, 10-18 → INT64.
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location directory = newExternalTableLocation();
        fileSystem.createDirectory(directory);
        try {
            Location dataFile = directory.appendPath("data.parquet");

            DecimalType int32Decimal = DecimalType.createDecimalType(7, 2);
            DecimalType int64Decimal = DecimalType.createDecimalType(15, 4);
            ImmutableList<Type> types = ImmutableList.of(int32Decimal, int64Decimal);
            ImmutableList<String> columnNames = ImmutableList.of("d_int32", "d_int64");
            try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                    ParquetWriter writer = ParquetTestUtils.createParquetWriter(
                            out,
                            ParquetWriterOptions.builder().build(),
                            types,
                            columnNames,
                            CompressionCodec.SNAPPY)) {
                PageBuilder pageBuilder = new PageBuilder(types);
                BlockBuilder d32 = pageBuilder.getBlockBuilder(0);
                BlockBuilder d64 = pageBuilder.getBlockBuilder(1);
                int32Decimal.writeLong(d32, 12345L);
                int64Decimal.writeLong(d64, 1234567891234L);
                int32Decimal.writeLong(d32, -9999L);
                int64Decimal.writeLong(d64, -999999999999999L);
                int32Decimal.writeLong(d32, 0L);
                int64Decimal.writeLong(d64, 0L);
                d32.appendNull();
                d64.appendNull();
                pageBuilder.declarePositions(4);
                writer.write(pageBuilder.build());
            }

            String tableName = "test_gpu_int32_64_decimals_" + randomNameSuffix();
            assertUpdate(
                    """
                            CREATE TABLE %s (d_int32 decimal(7,2), d_int64 decimal(15,4))
                            WITH (external_location = '%s', format = 'PARQUET')
                            """.formatted(tableName, directory));
            assertThat(query("SELECT d_int32, d_int64 FROM " + tableName))
                    .executesWithGpu(TableScanNode.class);
            assertUpdate("DROP TABLE " + tableName);
        }
        finally {
            fileSystem.deleteDirectory(directory);
        }
    }

    @Test
    public void testInt96TimestampEdgeCases()
    {
        // Trino's Hive writer stores timestamps as INT96. Verify the GPU reader handles the full
        // representable range, including years that would wrap under cuDF's old int64-nanos path.
        assertUpdate("CREATE TABLE test_gpu_int96_ts AS SELECT * FROM (VALUES " +
                "(CAST(TIMESTAMP '1970-01-01 00:00:00.000' AS timestamp(3))), " +
                "(CAST(TIMESTAMP '2024-06-15 12:34:56.789' AS timestamp(3))), " +
                "(CAST(TIMESTAMP '1500-03-01 00:00:00.000' AS timestamp(3))), " +
                "(CAST(TIMESTAMP '9999-12-31 23:59:59.999' AS timestamp(3))), " +
                "(CAST(NULL AS timestamp(3)))) " +
                "t(ts)", 5);
        assertThat(query("SELECT ts FROM test_gpu_int96_ts"))
                .executesWithGpu(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_int96_ts");
    }

    @Test
    public void testInt64BackedTimestampRead()
            throws IOException
    {
        // Trino's Hive writer always emits INT96 timestamps, so INT64-backed timestamps can only
        // be exercised via externally-written files. cuDF's withTimeUnit(MICROS) forces all reads
        // to TIMESTAMP_MICROSECONDS regardless of on-disk format. Reading at precision 3 exercises
        // the evolveColumn MICROS→MILLIS cast; reading at precision 6 is the exact-match path.
        // Without the cast, precision-3 values would be off by a factor of 1000.
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location directory = newExternalTableLocation();
        fileSystem.createDirectory(directory);
        try {
            Location dataFile = directory.appendPath("data.parquet");

            ImmutableList<Type> types = ImmutableList.of(TIMESTAMP_MILLIS);
            ImmutableList<String> columnNames = ImmutableList.of("ts");
            try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                    ParquetWriter writer = ParquetTestUtils.createParquetWriter(
                            out,
                            ParquetWriterOptions.builder().build(),
                            types,
                            columnNames,
                            CompressionCodec.SNAPPY)) {
                PageBuilder pageBuilder = new PageBuilder(types);
                BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                // Include values outside the INT96→int64-nanos wrap range (~1677..2262) to exercise
                // the INT64 timestamp reader's wider range.
                TIMESTAMP_MILLIS.writeLong(builder, epochMicros(1970, 1, 1, 0, 0, 0, 0));
                TIMESTAMP_MILLIS.writeLong(builder, epochMicros(2024, 1, 1, 0, 0, 0, 0));
                TIMESTAMP_MILLIS.writeLong(builder, epochMicros(1500, 6, 15, 12, 0, 0, 0));
                TIMESTAMP_MILLIS.writeLong(builder, epochMicros(2500, 12, 31, 23, 59, 59, 999));
                builder.appendNull();
                pageBuilder.declarePositions(5);
                writer.write(pageBuilder.build());
            }

            // The Hive connector rejects CREATE TABLE when the column precision differs from the
            // session's hive.timestamp_precision, so each read runs under a matching session.
            readAndAssertTimestamps(HiveTimestampPrecision.MILLISECONDS, directory);
            readAndAssertTimestamps(HiveTimestampPrecision.MICROSECONDS, directory);
        }
        finally {
            fileSystem.deleteDirectory(directory);
        }
    }

    private void readAndAssertTimestamps(HiveTimestampPrecision precision, Location directory)
    {
        String catalog = getSession().getCatalog().orElseThrow();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "timestamp_precision", precision.name())
                .build();
        String tableName = "test_gpu_int64_ts_" + precision.getPrecision() + "_" + randomNameSuffix();
        assertUpdate(session,
                """
                        CREATE TABLE %s (ts timestamp(%s))
                        WITH (external_location = '%s', format = 'PARQUET')
                        """.formatted(tableName, precision.getPrecision(), directory));
        assertThat(query(session, "SELECT ts FROM " + tableName))
                .executesWithGpu(TableScanNode.class);
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    private static long epochMicros(int year, int month, int day, int hour, int minute, int second, int millis)
    {
        return LocalDateTime.of(year, month, day, hour, minute, second)
                .toEpochSecond(ZoneOffset.UTC) * 1_000_000L + millis * 1_000L;
    }

    @Test
    public void testDateEdgeValues()
    {
        assertUpdate("CREATE TABLE test_gpu_temporal AS SELECT * FROM (VALUES " +
                "(DATE '1970-01-01'), " +
                "(DATE '2024-02-29'), " +
                "(DATE '9999-12-31'), " +
                "(DATE '1900-06-15'), " +
                "(CAST(NULL AS date))) " +
                "t(d)", 5);

        assertThat(query("SELECT d FROM test_gpu_temporal"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_temporal");
    }

    @Test
    public void testMultipleRowsWithInterleavedNulls()
    {
        assertUpdate("CREATE TABLE test_gpu_interleaved_nulls AS SELECT * FROM (VALUES " +
                "(true, CAST(1 AS tinyint), CAST(10 AS smallint), 100, CAST(1000 AS bigint), " +
                "REAL '1.5', DOUBLE '2.5', 'a', CAST(1.23 AS decimal(5,3)), DATE '2024-01-01', CAST(TIMESTAMP '2024-01-01 00:00:00' AS timestamp(3))), " +
                "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                "(false, CAST(-1 AS tinyint), CAST(-10 AS smallint), -100, CAST(-1000 AS bigint), " +
                "REAL '-1.5', DOUBLE '-2.5', '', CAST(-9.999 AS decimal(5,3)), DATE '1970-01-01', CAST(TIMESTAMP '1970-01-01 00:00:00' AS timestamp(3))), " +
                "(NULL, CAST(2 AS tinyint), NULL, 200, NULL, REAL '3.5', NULL, 'c', NULL, " +
                "DATE '2025-06-15', NULL)) " +
                "t(c_bool, c_tiny, c_small, c_int, c_big, c_real, c_double, c_varchar, c_decimal, c_date, c_timestamp)", 4);

        assertThat(query("SELECT * FROM test_gpu_interleaved_nulls"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_interleaved_nulls");
    }

    @Test
    public void testSelectWithNulls()
    {
        assertUpdate("CREATE TABLE test_gpu_nulls AS SELECT " +
                "CAST(NULL AS boolean) AS col_boolean, " +
                "CAST(NULL AS bigint) AS col_bigint, " +
                "CAST(NULL AS double) AS col_double, " +
                "CAST(NULL AS varchar) AS col_varchar", 1);

        assertThat(query("SELECT col_boolean FROM test_gpu_nulls WHERE col_boolean IS NULL"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_bigint FROM test_gpu_nulls WHERE col_bigint IS NULL"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_double FROM test_gpu_nulls WHERE col_double IS NULL"))
                .executesWithoutGpu();
        assertThat(query("SELECT col_varchar FROM test_gpu_nulls WHERE col_varchar IS NULL"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_nulls");
    }

    @Test
    public void testSelectWithMultipleRows()
    {
        assertUpdate("CREATE TABLE test_gpu_multiple_rows AS " +
                "SELECT CAST(orderkey % 3 = 0 AS boolean) AS is_divisible, orderkey, totalprice " +
                "FROM tpch.tiny.orders", 15000);

        assertThat(query("SELECT count(*) FROM test_gpu_multiple_rows WHERE is_divisible"))
                .executesWithGpu(AggregationNode.class);
        assertThat(query("SELECT count(*) FROM test_gpu_multiple_rows WHERE orderkey < 100"))
                .executesWithGpu(AggregationNode.class);
        assertThat(query("SELECT sum(totalprice) FROM test_gpu_multiple_rows"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_multiple_rows");
    }

    @Test
    public void testSelectWithColumnPruning()
    {
        assertUpdate("CREATE TABLE test_gpu_pruning AS " +
                        "SELECT orderkey, orderstatus, totalprice, orderdate FROM tpch.tiny.orders",
                15000);

        // Read only some columns (tests column pruning)
        assertThat(query("SELECT orderkey FROM test_gpu_pruning WHERE orderkey < 10"))
                .executesWithoutGpu();
        assertThat(query("SELECT totalprice FROM test_gpu_pruning WHERE orderkey < 10"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_gpu_pruning");
    }

    @Test
    public void testSelectWithPartitioning()
    {
        assertUpdate("CREATE TABLE test_gpu_partitioned " +
                        "WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                        "SELECT orderkey, totalprice, orderstatus FROM tpch.tiny.orders",
                15000);

        // Test partition pruning (partition filters are pushed down to splits, so GPU can execute)
        assertThat(query("SELECT count(*) FROM test_gpu_partitioned WHERE orderstatus = 'F'"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT sum(totalprice) FROM test_gpu_partitioned WHERE orderstatus = 'O'"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_partitioned");
    }

    @Test
    public void testEmptyTable()
    {
        // Create table without inserting any data
        assertUpdate("CREATE TABLE test_gpu_empty (" +
                "col_boolean boolean, " +
                "col_bigint bigint, " +
                "col_double double, " +
                "col_varchar varchar)");

        // Verify empty table queries execute with GPU
        assertThat(query("SELECT * FROM test_gpu_empty"))
                .executesWithGpu(TableScanNode.class)
                .returnsEmptyResult();
        assertThat(query("SELECT col_bigint FROM test_gpu_empty"))
                .executesWithGpu(TableScanNode.class)
                .returnsEmptyResult();
        assertThat(query("SELECT count(*) FROM test_gpu_empty"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES BIGINT '0'");

        assertUpdate("DROP TABLE test_gpu_empty");
    }
}
