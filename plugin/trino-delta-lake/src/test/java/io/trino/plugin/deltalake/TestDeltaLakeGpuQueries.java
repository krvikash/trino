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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.BaseGpuQueriesTest;
import io.trino.testing.QueryRunner;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeGpuQueries
        extends BaseGpuQueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .configureGpuDistributedExecution()
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                // delta.parquet.time-zone defaults to the JVM zone; pin UTC so timestamp columns stay GPU-eligible
                .addDeltaProperty("delta.parquet.time-zone", "UTC")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void testPartitionedTable()
    {
        assertUpdate("CREATE TABLE test_gpu_partitioned WITH (partitioned_by = ARRAY['regionkey']) AS " +
                "SELECT nationkey, name, regionkey FROM nation", 25);
        // Partition columns are synthesized as GPU constant columns, not read from Parquet
        assertThat(query("SELECT nationkey, name, regionkey FROM test_gpu_partitioned"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT regionkey FROM test_gpu_partitioned WHERE regionkey = 1"))
                .executesWithGpu(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_partitioned");
    }

    @Test
    public void testColumnMappingName()
    {
        assertUpdate("CREATE TABLE test_gpu_cm_name WITH (column_mapping_mode = 'name') AS " +
                "SELECT nationkey, name, regionkey FROM nation", 25);
        assertThat(query("SELECT nationkey, name, regionkey FROM test_gpu_cm_name"))
                .executesWithGpu(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_cm_name");
    }

    @Test
    public void testColumnMappingId()
    {
        assertUpdate("CREATE TABLE test_gpu_cm_id WITH (column_mapping_mode = 'id') AS " +
                "SELECT nationkey, name, regionkey FROM nation", 25);
        assertThat(query("SELECT nationkey, name, regionkey FROM test_gpu_cm_id"))
                .executesWithGpu(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_cm_id");
    }

    @Test
    public void testAddedColumnReadsNulls()
    {
        assertUpdate("CREATE TABLE test_gpu_added AS SELECT nationkey, name FROM nation", 25);
        // Existing files predate the column, so it is read as a NULL constant on the GPU
        assertUpdate("ALTER TABLE test_gpu_added ADD COLUMN extra bigint");
        assertThat(query("SELECT nationkey, name, extra FROM test_gpu_added"))
                .executesWithGpu(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_added");
    }

    @Test
    public void testDeletionVectorFallsBackToCpuPerSplit()
    {
        assertUpdate("CREATE TABLE test_gpu_dv (id bigint, val varchar) WITH (deletion_vectors_enabled = true)");
        assertUpdate("INSERT INTO test_gpu_dv VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);
        assertUpdate("DELETE FROM test_gpu_dv WHERE id = 2", 1);
        // Splits carrying a deletion vector fall back to the CPU reader inside the GPU operator
        assertThat(query("SELECT id, val FROM test_gpu_dv"))
                .executesWithGpuCpuFallback(TableScanNode.class);
        assertUpdate("DROP TABLE test_gpu_dv");
    }

    @Test
    public void testSyntheticColumns()
    {
        assertThat(query("SELECT \"$path\" FROM nation")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT \"$file_size\" FROM nation")).executesWithGpu(TableScanNode.class);
        // $file_modified_time is a timestamp with time zone, which the GPU carries in UTC
        assertThat(query("SELECT \"$file_modified_time\" FROM nation")).executesWithGpu(TableScanNode.class);

        assertThat(query("SELECT nationkey, \"$path\", \"$file_size\", name FROM nation")).executesWithGpu(TableScanNode.class);
    }

    @Test
    public void testTimestampWithTimeZoneRoundingDisparityBetweenGpuAndCpu()
            throws Exception
    {
        String tableName = "test_gpu_tstz_rounding_disparity_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts timestamp(3) with time zone)");
        assertUpdate("INSERT INTO " + tableName + " VALUES NULL", 1);
        String path = (String) computeScalar("SELECT \"$path\" FROM " + tableName);
        // Sub-millisecond parts of exactly half a millisecond, in the INT64 microseconds Spark writes
        writeMicrosDataFile(
                path.substring(0, path.lastIndexOf('/')),
                2,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1718454896_123L, 500_000_000, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1L, 500_000_000, UTC_KEY));
        String selectQuery = "SELECT ts FROM " + tableName;

        // TODO (https://starburstdata.atlassian.net/browse/ENG-28211): round instead of truncating
        assertThat(query(selectQuery))
                .matches("VALUES NULL, TIMESTAMP '2024-06-15 12:34:56.123 UTC', TIMESTAMP '1969-12-31 23:59:59.999 UTC'");

        Session gpuDisabled = Session.builder(getSession())
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .build();
        assertThat(query(gpuDisabled, selectQuery))
                .matches("VALUES NULL, TIMESTAMP '2024-06-15 12:34:56.124 UTC', TIMESTAMP '1970-01-01 00:00:00.000 UTC'");

        assertUpdate("DROP TABLE " + tableName);
    }

    private void writeMicrosDataFile(String tableLocation, int version, LongTimestampWithTimeZone... values)
            throws IOException
    {
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        String fileName = "int64_micros.parquet";
        Location dataFile = Location.of(tableLocation + "/" + fileName);
        MessageType schema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS)).named("ts"))
                .named("schema");
        try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                ParquetWriter writer = new ParquetWriter(
                        out,
                        schema,
                        ImmutableMap.of(ImmutableList.of("ts"), TIMESTAMP_TZ_MICROS),
                        ParquetWriterOptions.builder().build(),
                        SNAPPY,
                        "test-version",
                        Optional.of(DateTimeZone.UTC),
                        Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(TIMESTAMP_TZ_MICROS));
            BlockBuilder builder = pageBuilder.getBlockBuilder(0);
            for (LongTimestampWithTimeZone value : values) {
                TIMESTAMP_TZ_MICROS.writeObject(builder, value);
            }
            pageBuilder.declarePositions(values.length);
            writer.write(pageBuilder.build());
        }
        long fileSize = fileSystem.newInputFile(dataFile).length();
        String commit = "{\"add\":{\"path\":\"%s\",\"partitionValues\":{},\"size\":%s,\"modificationTime\":1,\"dataChange\":true}}\n".formatted(fileName, fileSize);
        try (OutputStream out = fileSystem.newOutputFile(Location.of("%s/_delta_log/%020d.json".formatted(tableLocation, version))).create()) {
            out.write(commit.getBytes(UTF_8));
        }
    }
}
