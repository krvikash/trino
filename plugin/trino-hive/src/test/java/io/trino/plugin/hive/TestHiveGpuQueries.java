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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetTestUtils;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;

import static io.trino.plugin.hive.HiveStorageFormat.ESRI;
import static io.trino.plugin.hive.HiveStorageFormat.ESRI_GEO_JSON;
import static io.trino.plugin.hive.HiveStorageFormat.REGEX;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE_PROTOBUF;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveGpuQueries
        extends BaseHiveGpuQueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setInitialTables(ImmutableList.of(NATION, REGION, ORDERS))
                .addHiveProperty("hive.storage-format", "PARQUET")
                .addHiveProperty("hive.parquet.time-zone", "UTC")
                .build();
    }

    @Override
    protected Location newExternalTableLocation()
    {
        return Location.of("local:///gpu_test_" + randomNameSuffix());
    }

    @Test
    public void testIntegerWideningRead()
            throws IOException
    {
        // INTEGER (INT32) Parquet column declared as BIGINT in Trino (schema evolution case).
        // evolveColumn must cast INT32→INT64. Without the cast, the BIGINT consumer in
        // CopyToBlocks would try to read longs from an INT32 cuDF column — a type mismatch that
        // either throws or yields wrong values. INT32_MAX and INT32_MIN are included so any
        // byte-level misinterpretation is clearly visible.
        // Local-only: Parquet type widening doesn't need exercise against the S3/MinIO variant.
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location directory = newExternalTableLocation();
        fileSystem.createDirectory(directory);
        try {
            Location dataFile = directory.appendPath("data.parquet");
            try (OutputStream out = fileSystem.newOutputFile(dataFile).create();
                    ParquetWriter writer = ParquetTestUtils.createParquetWriter(
                            out,
                            ParquetWriterOptions.builder().build(),
                            ImmutableList.of(INTEGER),
                            ImmutableList.of("n"),
                            CompressionCodec.SNAPPY)) {
                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(INTEGER));
                BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                INTEGER.writeLong(builder, 0L);
                INTEGER.writeLong(builder, 42L);
                INTEGER.writeLong(builder, -1L);
                INTEGER.writeLong(builder, 2147483647L);   // Integer.MAX_VALUE
                INTEGER.writeLong(builder, -2147483648L);  // Integer.MIN_VALUE
                builder.appendNull();
                pageBuilder.declarePositions(6);
                writer.write(pageBuilder.build());
            }

            String tableName = "test_gpu_int_widening_" + randomNameSuffix();
            assertUpdate("CREATE TABLE %s (n bigint) WITH (external_location = '%s', format = 'PARQUET')"
                    .formatted(tableName, directory));
            assertThat(query("SELECT n FROM " + tableName))
                    .executesWithGpu(TableScanNode.class);
            assertUpdate("DROP TABLE " + tableName);
        }
        finally {
            fileSystem.deleteDirectory(directory);
        }
    }

    @Test
    public void testUnsupportedFileFormat()
    {
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            if (hiveStorageFormat == HiveStorageFormat.CSV) {
                // CSV supports only unbounded VARCHAR type
                continue;
            }
            if (hiveStorageFormat == REGEX) {
                // REGEX format is read-only
                continue;
            }
            if (hiveStorageFormat == ESRI) {
                // ESRI format is read-only
                continue;
            }
            if (hiveStorageFormat == ESRI_GEO_JSON) {
                // ESRI_GEO_JSON format is read-only
                continue;
            }
            if (hiveStorageFormat == SEQUENCEFILE_PROTOBUF) {
                // SEQUENCEFILE_PROTOBUF format is read-only
                continue;
            }

            try (var table = new TestTable(
                    getQueryRunner()::execute,
                    "nation_" + hiveStorageFormat.name().toLowerCase(ENGLISH),
                    "WITH (format = '%s') AS TABLE nation".formatted(hiveStorageFormat.name()))) {
                assertThat(query("TABLE " + table.getName())).executesWithGpu(TableScanNode.class);
            }
        }
    }

    @Test
    public void testSyntheticColumns()
    {
        // synthetic columns only
        assertThat(query("SELECT \"$path\" FROM nation")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT \"$file_size\" FROM nation")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT \"$file_modified_time\" FROM nation")).executesWithoutGpu();

        // data columns and synthetic columns
        assertThat(query("SELECT nationkey, \"$path\", name FROM nation")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT nationkey, \"$file_size\", name FROM nation")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT nationkey, \"$file_modified_time\", name FROM nation")).executesWithoutGpu();
    }

    @Test
    public void testSyntheticColumnsWithFilter()
    {
        // synthetic columns only
        assertThat(query("SELECT \"$path\" FROM nation WHERE \"$path\" LIKE '%/%'")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT \"$file_size\" FROM nation WHERE \"$file_size\" > 0")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT \"$file_size\" FROM nation WHERE \"$file_size\" < 1024")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT \"$file_modified_time\" FROM nation WHERE \"$file_modified_time\" < CURRENT_DATE - INTERVAL '10' DAY")).executesWithoutGpu();

        // data columns and synthetic columns
        assertThat(query("SELECT nationkey, \"$path\", \"$file_size\", name FROM nation WHERE name LIKE '%a%'")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT nationkey, \"$path\", \"$file_size\", name FROM nation WHERE name LIKE '%a%' OR \"$path\" LIKE '%/%'")).executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT nationkey, \"$path\", \"$file_size\", \"$file_modified_time\", name FROM nation WHERE name LIKE '%a%'")).executesWithoutGpu();
    }
}
