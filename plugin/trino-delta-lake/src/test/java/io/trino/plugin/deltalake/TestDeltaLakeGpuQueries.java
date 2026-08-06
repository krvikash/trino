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

import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.BaseGpuQueriesTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

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
        // $file_modified_time is timestamp with time zone, which the engine does not run on the GPU
        assertThat(query("SELECT \"$file_modified_time\" FROM nation")).executesWithoutGpu();

        assertThat(query("SELECT nationkey, \"$path\", \"$file_size\", name FROM nation")).executesWithGpu(TableScanNode.class);
    }
}
