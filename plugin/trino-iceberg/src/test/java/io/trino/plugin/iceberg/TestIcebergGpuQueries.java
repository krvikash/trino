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
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.BaseGpuQueriesTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestIcebergGpuQueries
        extends BaseGpuQueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        checkState(
                !new FeaturesConfig().isGpuExecution(),
                "GPU execution must not be enabled by default, otherwise this test is redundant");

        return IcebergQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
    }

    @Test
    public void testSchemaEvolution()
    {
        // Write data with original schema
        assertUpdate("CREATE TABLE test_gpu_schema_evolution (col1 bigint)");
        assertUpdate("INSERT INTO test_gpu_schema_evolution VALUES (1), (2), (3)", 3);

        // Add a new column — old files lack this field ID
        assertUpdate("ALTER TABLE test_gpu_schema_evolution ADD COLUMN col2 varchar");

        // Old rows: col2 is NULL (field ID absent in the original file)
        assertThat(query("SELECT col1 FROM test_gpu_schema_evolution WHERE col2 IS NULL"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'");

        // Insert a row using the new schema
        assertUpdate("INSERT INTO test_gpu_schema_evolution VALUES (4, 'new')", 1);

        // New row has the value present; old rows still null
        assertThat(query("SELECT col1, col2 FROM test_gpu_schema_evolution WHERE col2 IS NOT NULL"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES (BIGINT '4', VARCHAR 'new')");

        assertThat(query("SELECT count(*) FROM test_gpu_schema_evolution WHERE col2 IS NULL"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES BIGINT '3'");

        assertUpdate("DROP TABLE test_gpu_schema_evolution");
    }

    @Test
    public void testMetadataColumns()
    {
        assertThat(query("SELECT \"$path\" FROM nation"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT nationkey, \"$path\", name FROM nation"))
                .executesWithGpu(TableScanNode.class);

        // TODO: the 'timestamp with time zone' type is not supported yet
        //  https://starburstdata.atlassian.net/browse/ENG-18047
        assertThat(query("SELECT \"$file_modified_time\" FROM nation"))
                .executesWithoutGpu();
        assertThat(query("SELECT nationkey, \"$file_modified_time\", name FROM nation"))
                .executesWithoutGpu();

        assertUpdate(
                "CREATE TABLE test_gpu_partition_meta " +
                        "WITH (partitioning = ARRAY['orderstatus']) AS " +
                        "SELECT orderkey, totalprice, orderstatus FROM tpch.tiny.orders",
                15000);
        assertThat(query("SELECT orderkey, \"$partition\" FROM test_gpu_partition_meta WHERE orderstatus = 'F'"))
                .executesWithGpu(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_partition_meta");
    }

    @Test
    public void testFileFormats()
    {
        for (IcebergFileFormat format : IcebergFileFormat.values()) {
            try (TestTable table = new TestTable(
                    getQueryRunner()::execute,
                    "nation_" + format.name().toLowerCase(ENGLISH),
                    "WITH (format = '%s') AS TABLE nation".formatted(format.name()))) {
                QueryAssertions.QueryAssert queryAssert = assertThat(query("TABLE " + table.getName()));
                if (format == IcebergFileFormat.PARQUET) {
                    queryAssert.executesWithGpu(TableScanNode.class);
                }
                else {
                    // Only Parquet is read on the GPU; other formats fall back to the CPU reader per split
                    queryAssert.executesWithGpuCpuFallback(TableScanNode.class);
                }
            }
        }
    }

    @Test
    public void testDelete()
    {
        assertUpdate("CREATE TABLE test_gpu_eq_delete AS SELECT nationkey, name FROM nation", 25);
        assertUpdate("DELETE FROM test_gpu_eq_delete WHERE nationkey = 1", 1);
        assertThat(query("SELECT nationkey FROM test_gpu_eq_delete"))
                // Splits with deletes fall back to the CPU reader
                .executesWithGpuCpuFallback(TableScanNode.class);
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT name FROM test_gpu_eq_delete",
                "TableScan",
                "GPU: supported \\(GPU splits: 0\\.00%\\)");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT name FROM test_gpu_eq_delete WHERE nationkey = 1",
                "ScanFilterProject",
                "GPU: supported \\(GPU splits: 0\\.00%\\)");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT cast(nationkey AS varchar) FROM test_gpu_eq_delete",
                "ScanProject",
                "GPU: supported \\(GPU splits: 0\\.00%\\)");
        assertThat((String) computeActual(
                Session.builder(getSession())
                        .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                        .build(),
                "EXPLAIN ANALYZE SELECT cast(nationkey AS varchar) FROM test_gpu_eq_delete")
                .getOnlyValue())
                .doesNotContain("GPU");
        assertUpdate("DROP TABLE test_gpu_eq_delete");
    }

    @Test
    public void testExplainAnalyze()
    {
        assertUpdate("CREATE TABLE test_gpu_explain AS SELECT nationkey, name FROM nation", 25);
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT name FROM test_gpu_explain WHERE nationkey = rand()",
                "ScanFilterProject",
                "GPU: unsupported",
                "\\QUnsupported expression: (CAST(bigint AS double) = random())");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT IF(rand()<42, nationkey) FROM test_gpu_explain",
                "ScanProject",
                "GPU: unsupported",
                "\\QUnsupported expression: (CASE WHEN (random() < double) THEN bigint ELSE bigint END)");
        assertThat((String) computeActual(
                Session.builder(getSession())
                        .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                        .build(),
                "EXPLAIN ANALYZE SELECT IF(rand()<42, nationkey) FROM test_gpu_explain")
                .getOnlyValue())
                .doesNotContain("GPU");
        assertUpdate("DROP TABLE test_gpu_explain");

        assertUpdate("CREATE TABLE test_gpu_struct (id bigint, info ROW(name varchar, age bigint))");
        assertUpdate("INSERT INTO test_gpu_struct VALUES (1, ROW('alice', 30))", 1);
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT info.name FROM test_gpu_struct",
                "GPU: unsupported",
                "Non-primitive columns are not supported");
        assertUpdate("DROP TABLE test_gpu_struct");
    }

    @Test
    public void testSelectWithPartitioning()
    {
        assertUpdate(
                "CREATE TABLE test_gpu_partitioned " +
                        "WITH (partitioning = ARRAY['orderstatus']) AS " +
                        "SELECT orderkey, totalprice, orderstatus FROM tpch.tiny.orders",
                15000);

        assertThat(query("SELECT count(*) FROM test_gpu_partitioned WHERE orderstatus = 'F'"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT sum(totalprice) FROM test_gpu_partitioned WHERE orderstatus = 'O'"))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE test_gpu_partitioned");
    }

    @Test
    public void testPartitionColumnValues()
    {
        String tableName = "test_gpu_partition_values_" + randomNameSuffix();
        assertUpdate(
                "CREATE TABLE " + tableName +
                        " WITH (partitioning = ARRAY['part_key']) AS " +
                        "SELECT orderkey, orderstatus AS part_key FROM tpch.tiny.orders",
                15000);

        assertThat(query("SELECT part_key, orderkey FROM " + tableName + " WHERE part_key = 'F' ORDER BY orderkey LIMIT 5"))
                .executesWithGpu(TableScanNode.class)
                .matches("SELECT CAST(orderstatus AS varchar), orderkey FROM tpch.tiny.orders WHERE orderstatus = 'F' ORDER BY orderkey LIMIT 5");

        assertThat(query("SELECT DISTINCT part_key FROM " + tableName))
                .executesWithGpu(TableScanNode.class)
                .matches("SELECT DISTINCT CAST(orderstatus AS varchar) FROM tpch.tiny.orders");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionColumnWithNulls()
    {
        String tableName = "test_gpu_partition_nulls_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (val bigint, part_key varchar) WITH (partitioning = ARRAY['part_key'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, NULL), (4, NULL), (5, 'a')", 5);

        assertThat(query("SELECT part_key, val FROM " + tableName + " ORDER BY val"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES (VARCHAR 'a', BIGINT '1'), (VARCHAR 'b', BIGINT '2'), (CAST(NULL AS varchar), BIGINT '3'), (CAST(NULL AS varchar), BIGINT '4'), (VARCHAR 'a', BIGINT '5')");

        assertThat(query("SELECT val FROM " + tableName + " WHERE part_key IS NULL"))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES BIGINT '3', BIGINT '4'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testIntegerWideningRead()
    {
        String tableName = "test_gpu_int_widening_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (n integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (0), (42), (-1), (2147483647), (-2147483648), (NULL)", 6);

        assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN n SET DATA TYPE bigint");

        assertThat(query("SELECT n FROM " + tableName))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES BIGINT '0', BIGINT '42', BIGINT '-1', BIGINT '2147483647', BIGINT '-2147483648', NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDecimalWideningRead()
    {
        String tableName = "test_gpu_decimal_widening_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (d_short decimal(7,2), d_long decimal(15,4))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (123.45, 123456789.1234), (-99.99, -99999999999.9999), (0, 0), (NULL, NULL)", 4);

        assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN d_short SET DATA TYPE decimal(27,2)");
        assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN d_long SET DATA TYPE decimal(27,4)");

        assertThat(query("SELECT d_short, d_long FROM " + tableName))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampMicrosecondPrecision()
    {
        String tableName = "test_gpu_timestamp_micros_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(6))");
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(TIMESTAMP '2024-06-15 12:34:56.123456'), " +
                "(TIMESTAMP '1970-01-01 00:00:00.000001'), " +
                "(TIMESTAMP '2000-01-01 00:00:00.000000'), " +
                "(NULL)", 4);

        assertThat(query("SELECT ts FROM " + tableName))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES " +
                        "TIMESTAMP '2024-06-15 12:34:56.123456', " +
                        "TIMESTAMP '1970-01-01 00:00:00.000001', " +
                        "TIMESTAMP '2000-01-01 00:00:00.000000', " +
                        "NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampNanosecondPrecision()
    {
        String tableName = "test_gpu_timestamp_nanos_" + randomNameSuffix();
        // Iceberg's timestampNano type requires format version 3; version 2 silently coerces to timestamp(6)
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(9)) WITH (format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(TIMESTAMP '2024-06-15 12:34:56.123456789'), " +
                "(NULL)", 2);

        assertThat(query("SELECT ts FROM " + tableName))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInt96TimestampRead()
            throws Exception
    {
        // TODO: Rewrite this test to use iceberg.system.migrate() once TIMESTAMP WITH TIME ZONE
        //  support is available (https://starburstdata.atlassian.net/browse/ENG-18047).

        // Iceberg spec mandates INT64 MICROS for timestamps, but tables migrated from Hive
        // can contain Parquet files with INT96-encoded timestamps. The GPU path falls back to
        // CPU for INT96 columns.
        String tableName = "test_gpu_int96_ts_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(6))");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        int fieldId = icebergTable.schema().findField("ts").fieldId();

        // Build a Parquet schema with INT96 (no time-unit annotation) and the Iceberg field ID
        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT96).id(fieldId).named("ts"))
                .named("schema");

        TimestampType timestampType = createTimestampType(6);
        List<Type> types = ImmutableList.of(timestampType);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, ImmutableList.of("ts"), false, true);

        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        String tableLocation = icebergTable.location();
        Location dataFile = Location.of(tableLocation + "/data/int96_test.parquet");
        long fileSize;

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        parquetSchema,
                        schemaConverter.getPrimitiveTypes(),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.of(UTC),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            timestampType.writeLong(builder, epochMicros(1970, 1, 1, 0, 0, 0, 0));
            timestampType.writeLong(builder, epochMicros(2024, 6, 15, 12, 34, 56, 123456));
            timestampType.writeLong(builder, epochMicros(1500, 3, 1, 0, 0, 0, 0));
            timestampType.writeLong(builder, epochMicros(9999, 12, 31, 23, 59, 59, 999999));
            builder.appendNull();
            pageBuilder.declarePositions(5);
            writer.write(pageBuilder.build());
        }

        fileSize = fileSystem.newInputFile(dataFile).length();

        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(dataFile.toString())
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(5)
                        .withFormat(PARQUET)
                        .build())
                .commit();

        assertThat(query("SELECT ts FROM " + tableName))
                // INT96 is not supported on the GPU; the scan is planned for the GPU but every split
                // falls back to the CPU reader after the footer is read.
                .executesWithGpuCpuFallback(TableScanNode.class)
                .matches("VALUES " +
                        "TIMESTAMP '1970-01-01 00:00:00.000000', " +
                        "TIMESTAMP '2024-06-15 12:34:56.123456', " +
                        "TIMESTAMP '1500-03-01 00:00:00.000000', " +
                        "TIMESTAMP '9999-12-31 23:59:59.999999', " +
                        "NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInt64NanosTimestampRead()
            throws Exception
    {
        // Data written as timestamp(9) (INT64 NANOS on disk) then narrowed to timestamp(6)
        // via schema evolution. cuDF reads the annotated NANOS values and evolveColumn casts
        // them to microseconds.
        String tableName = "test_gpu_int64_nanos_ts_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(6))");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        int fieldId = icebergTable.schema().findField("ts").fieldId();

        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS))
                        .id(fieldId)
                        .named("ts"))
                .named("schema");

        TimestampType timestampType = createTimestampType(9);
        List<Type> types = ImmutableList.of(timestampType);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, ImmutableList.of("ts"), false, true);

        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        String tableLocation = icebergTable.location();
        Location dataFile = Location.of(tableLocation + "/data/int64_nanos_test.parquet");
        long fileSize;

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        parquetSchema,
                        schemaConverter.getPrimitiveTypes(),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.of(UTC),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            timestampType.writeObject(builder, new LongTimestamp(epochMicros(1970, 1, 1, 0, 0, 0, 0), 0));
            timestampType.writeObject(builder, new LongTimestamp(epochMicros(2024, 6, 15, 12, 34, 56, 123456), 0));
            timestampType.writeObject(builder, new LongTimestamp(epochMicros(2000, 1, 1, 0, 0, 0, 999999), 0));
            builder.appendNull();
            pageBuilder.declarePositions(4);
            writer.write(pageBuilder.build());
        }

        fileSize = fileSystem.newInputFile(dataFile).length();

        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(dataFile.toString())
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(4)
                        .withFormat(PARQUET)
                        .build())
                .commit();

        assertThat(query("SELECT ts FROM " + tableName))
                .executesWithGpu(TableScanNode.class)
                .matches("VALUES " +
                        "TIMESTAMP '1970-01-01 00:00:00.000000', " +
                        "TIMESTAMP '2024-06-15 12:34:56.123456', " +
                        "TIMESTAMP '2000-01-01 00:00:00.999999', " +
                        "NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnannotatedInt64TimestampRead()
            throws Exception
    {
        // Parquet file with unannotated INT64 (no logical type annotation) for a timestamp
        // column. Both CPU and GPU paths reject this.
        String tableName = "test_gpu_unannotated_int64_ts_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(6))");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        int fieldId = icebergTable.schema().findField("ts").fieldId();

        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64)
                        .id(fieldId)
                        .named("ts"))
                .named("schema");

        List<Type> types = ImmutableList.of(BIGINT);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, ImmutableList.of("ts"), false, true);

        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        String tableLocation = icebergTable.location();
        Location dataFile = Location.of(tableLocation + "/data/unannotated_int64_test.parquet");
        long fileSize;

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        parquetSchema,
                        schemaConverter.getPrimitiveTypes(),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.of(UTC),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            BIGINT.writeLong(builder, epochMicros(1970, 1, 1, 0, 0, 0, 0));
            BIGINT.writeLong(builder, epochMicros(2024, 6, 15, 12, 34, 56, 123456));
            BIGINT.writeLong(builder, epochMicros(2000, 1, 1, 0, 0, 0, 999999));
            builder.appendNull();
            pageBuilder.declarePositions(4);
            writer.write(pageBuilder.build());
        }

        fileSize = fileSystem.newInputFile(dataFile).length();

        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(dataFile.toString())
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(4)
                        .withFormat(PARQUET)
                        .build())
                .commit();

        // GPU path falls back to CPU due to unannotated INT64 timestamp
        assertThat(query("SELECT ts FROM " + tableName))
                .failure().hasMessageContaining("Unsupported Trino column type (timestamp(6))");

        // CPU path also rejects unannotated INT64 timestamps
        Session cpuSession = Session.builder(getSession())
                .setSystemProperty("gpu_execution_enabled", "false")
                .build();
        assertThat(query(cpuSession, "SELECT ts FROM " + tableName))
                .failure().hasMessageContaining("Unsupported Trino column type (timestamp(6))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInt64NanosTimestampRoundingDisparity()
            throws Exception
    {
        // Exposes a rounding discrepancy: the CPU Parquet reader rounds nanos -> micros
        // (ValueDecoders.getInt64TimestampNanosToShortTimestampDecoder rounds via Timestamps.round),
        // while cuDF's NANOS -> MICROS cast truncates. For example, 123456789 epoch nanos:
        //   CPU:  round(123456789, 3) = 123457000, / 1000 = 123457 micros
        //   GPU:  123456789 / 1000 = 123456 micros (truncation)
        String tableName = "test_gpu_int64_nanos_rounding_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(6))");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
        int fieldId = icebergTable.schema().findField("ts").fieldId();

        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS))
                        .id(fieldId)
                        .named("ts"))
                .named("schema");

        TimestampType timestampType = createTimestampType(9);
        List<Type> types = ImmutableList.of(timestampType);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, ImmutableList.of("ts"), false, true);

        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        String tableLocation = icebergTable.location();
        Location dataFile = Location.of(tableLocation + "/data/int64_nanos_rounding_test.parquet");
        long fileSize;

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        parquetSchema,
                        schemaConverter.getPrimitiveTypes(),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.of(UTC),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            // 789 nanos remainder: CPU rounds .123456789 → .123457, GPU truncates → .123456
            timestampType.writeObject(builder, new LongTimestamp(epochMicros(2024, 6, 15, 12, 34, 56, 123456), 789_000));
            pageBuilder.declarePositions(1);
            writer.write(pageBuilder.build());
        }

        fileSize = fileSystem.newInputFile(dataFile).length();

        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(dataFile.toString())
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(1)
                        .withFormat(PARQUET)
                        .build())
                .commit();

        // GPU truncates: 123456789 nanos → 123456 micros
        assertThat(query("SELECT ts FROM " + tableName))
                .matches("VALUES TIMESTAMP '2024-06-15 12:34:56.123456'");

        // CPU rounds: 123456789 nanos → 123457 micros
        Session cpuSession = Session.builder(getSession())
                .setSystemProperty("gpu_execution_enabled", "false")
                .build();
        assertThat(query(cpuSession, "SELECT ts FROM " + tableName))
                .matches("VALUES TIMESTAMP '2024-06-15 12:34:56.123457'");

        assertUpdate("DROP TABLE " + tableName);
    }

    private static long epochMicros(int year, int month, int day, int hour, int minute, int second, int micros)
    {
        return LocalDateTime.of(year, month, day, hour, minute, second)
                .toEpochSecond(ZoneOffset.UTC) * 1_000_000L + micros;
    }

    @Test
    public void testParquetFileWithoutNameMappingAndFieldIds()
            throws Exception
    {
        // Parquet files from non-Iceberg writers (e.g. manually added via add_files) may lack
        // field IDs. Without a NameMapping, columns cannot be mapped to Iceberg fields and are
        // read as nulls — matching the CPU reader behavior.
        String tableName = "test_gpu_no_field_ids_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col bigint)");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");

        // Build a Parquet schema WITHOUT .id() — mimics a non-Iceberg writer
        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).named("col"))
                .named("schema");

        List<Type> types = ImmutableList.of(BIGINT);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, ImmutableList.of("col"), false, true);

        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        String tableLocation = icebergTable.location();
        Location dataFile = Location.of(tableLocation + "/data/no_field_ids.parquet");
        long fileSize;

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        parquetSchema,
                        schemaConverter.getPrimitiveTypes(),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.empty(),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            BIGINT.writeLong(builder, 42);
            BIGINT.writeLong(builder, 100);
            builder.appendNull();
            pageBuilder.declarePositions(3);
            writer.write(pageBuilder.build());
        }

        fileSize = fileSystem.newInputFile(dataFile).length();

        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(dataFile.toString())
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(3)
                        .withFormat(PARQUET)
                        .build())
                .commit();

        assertThat(query("SELECT col FROM " + tableName))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testParquetFileWithNameMappingAndWithoutFieldIds()
            throws Exception
    {
        String tableName = "test_gpu_no_ids_name_mapping_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col bigint)");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        BaseTable icebergTable = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");

        icebergTable.updateProperties()
                .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(MappingUtil.create(icebergTable.schema())))
                .commit();

        MessageType parquetSchema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).named("col"))
                .named("schema");

        List<Type> types = ImmutableList.of(BIGINT);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, ImmutableList.of("col"), false, true);

        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        String tableLocation = icebergTable.location();
        Location dataFile = Location.of(tableLocation + "/data/no_ids_with_mapping.parquet");
        long fileSize;

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        parquetSchema,
                        schemaConverter.getPrimitiveTypes(),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.empty(),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            BIGINT.writeLong(builder, 42);
            BIGINT.writeLong(builder, 100);
            builder.appendNull();
            pageBuilder.declarePositions(3);
            writer.write(pageBuilder.build());
        }

        fileSize = fileSystem.newInputFile(dataFile).length();

        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(dataFile.toString())
                        .withFileSizeInBytes(fileSize)
                        .withRecordCount(3)
                        .withFormat(PARQUET)
                        .build())
                .commit();

        assertThat(query("SELECT col FROM " + tableName))
                .executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }
}
