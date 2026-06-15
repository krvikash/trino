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
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.BaseGpuQueriesTest;
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
        extends BaseGpuQueriesTest
{
    protected abstract Location newExternalTableLocation();

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

            // Same on-disk INT32/INT64 parquet read through long-decimal columns: cuDF reads
            // each as DECIMAL32/DECIMAL64 and evolveColumn widens to DECIMAL128.
            String longTableName = "test_gpu_int32_64_as_long_decimal_" + randomNameSuffix();
            assertUpdate(
                    """
                    CREATE TABLE %s (d_int32 decimal(27,2), d_int64 decimal(27,4))
                    WITH (external_location = '%s', format = 'PARQUET')
                    """.formatted(longTableName, directory));
            assertThat(query("SELECT d_int32, d_int64 FROM " + longTableName))
                    .executesWithGpu(TableScanNode.class);
            assertUpdate("DROP TABLE " + longTableName);
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
    public void testSelectWithPartitioning()
    {
        assertUpdate(
                "CREATE TABLE test_gpu_partitioned " +
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
}
