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

import io.trino.operator.OperatorStats;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.function.IntFunction;

import static io.trino.operator.gpu.GpuOperator.FUSED_PLAN_NODE_METRIC_PREFIX;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeGpuCompositeSplits
        extends AbstractTestQueryFramework
{
    private static final int FILE_COUNT = 6;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .configureGpuDistributedExecution()
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                // delta.parquet.time-zone defaults to the JVM zone; pin UTC so timestamp columns stay GPU-eligible
                .addDeltaProperty("delta.parquet.time-zone", "UTC")
                .addDeltaProperty("delta.experimental.composite-splits.enabled", "true")
                // Small pages, so that pages start and end inside files in testPagesCrossFileBoundaries
                .addDeltaProperty("gpu.scan.max-page-size", "1MB")
                .build();
    }

    @Test
    void testUnpartitionedTable()
    {
        String tableName = "test_gpu_composite_unpartitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

        assertThat(query("SELECT id, value FROM " + tableName)).executesWithGpu(TableScanNode.class);
        assertThat(gpuScanSplitCount("SELECT id, value FROM " + tableName)).isBetween(1L, FILE_COUNT - 1L);
        assertThat(query("SELECT id FROM " + tableName + " WHERE id > 2")).executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testPartitionedTable()
    {
        String tableName = "test_gpu_composite_partitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, part varchar) WITH (partitioned_by = ARRAY['part'])");
        // Partitions interleave in the transaction log, and each partition gets its own composite
        insertOneFilePerStatement(tableName, "part", i -> "'p" + (i % 3) + "'");

        assertThat(query("SELECT id, part FROM " + tableName)).executesWithGpu(TableScanNode.class);
        assertThat(gpuScanSplitCount("SELECT id, part FROM " + tableName)).isBetween(1L, FILE_COUNT - 1L);
        assertThat(query("SELECT id FROM " + tableName + " WHERE part = 'p1'")).executesWithGpu(TableScanNode.class);
        // No Parquet columns are read, so the page consists of constants only
        assertThat(query("SELECT part FROM " + tableName)).executesWithGpu(TableScanNode.class);
        assertThat(gpuScanSplitCount("SELECT part FROM " + tableName)).isBetween(1L, FILE_COUNT - 1L);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testNullPartitionValue()
    {
        String tableName = "test_gpu_composite_null_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, part bigint) WITH (partitioned_by = ARRAY['part'])");
        insertOneFilePerStatement(tableName, "part", i -> i % 2 == 0 ? "NULL" : String.valueOf(i));

        assertThat(query("SELECT id, part FROM " + tableName)).executesWithGpu(TableScanNode.class);
        assertThat(gpuScanSplitCount("SELECT id, part FROM " + tableName)).isBetween(1L, FILE_COUNT - 1L);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testColumnMappingModes()
    {
        for (String mode : new String[] {"id", "name"}) {
            String tableName = "test_gpu_composite_column_mapping_" + mode + "_" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar) WITH (column_mapping_mode = '" + mode + "')");
            insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

            assertThat(query("SELECT id, value FROM " + tableName)).executesWithGpu(TableScanNode.class);
            assertThat(gpuScanSplitCount("SELECT id, value FROM " + tableName)).isBetween(1L, FILE_COUNT - 1L);

            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testSchemaMismatchFallsBackToCpu()
    {
        String tableName = "test_gpu_composite_schema_mismatch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN extra bigint");
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c', 30)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'd', 40)", 1);

        // Older files lack the added column, so they cannot be fabricated into one file with the newer ones
        assertThat(query("SELECT id, value, extra FROM " + tableName)).executesWithGpuCpuFallback(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testDeletionVectorFallsBackToCpu()
    {
        String tableName = "test_gpu_composite_deletion_vectors_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar) WITH (deletion_vectors_enabled = true)");
        for (int i = 0; i < FILE_COUNT; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES (" + (2 * i) + ", 'a'), (" + (2 * i + 1) + ", 'b')", 2);
        }
        assertUpdate("DELETE FROM " + tableName + " WHERE value = 'b'", FILE_COUNT);

        assertThat(query("SELECT id, value FROM " + tableName)).executesWithGpuCpuFallback(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSynthesizedColumns()
    {
        String tableName = "test_gpu_composite_synthesized_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

        // Composites are disabled when synthesized columns are projected
        assertThat(query("SELECT id, \"$path\", \"$file_size\" FROM " + tableName)).executesWithGpu(TableScanNode.class);
        assertThat(gpuScanSplitCount("SELECT id, \"$path\", \"$file_size\" FROM " + tableName)).isEqualTo(FILE_COUNT);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testPagesCrossFileBoundaries()
    {
        // A file holds far more rows than fit in a 1MB page, so pages start and end inside files and span several of them
        String tableName = "test_gpu_composite_page_boundaries_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, flag boolean, part varchar) WITH (partitioned_by = ARRAY['part'])");
        String[] partitions = {"p1", "p1", "p1", "p2"};
        for (int i = 0; i < partitions.length; i++) {
            // flag is false only in the second insert
            assertUpdate(
                    "INSERT INTO %s SELECT %s + x * 1000 + y, %s, '%s' FROM UNNEST(sequence(1, 500)) a(x) CROSS JOIN UNNEST(sequence(1, 500)) b(y)"
                            .formatted(tableName, i * 1_000_000, i != 1, partitions[i]),
                    250_000);
        }
        long fileCount = fileCount(tableName);

        // Constants only
        assertThat(query("SELECT part FROM " + tableName)).executesWithGpu(TableScanNode.class);
        // Parquet columns and constants
        assertThat(query("SELECT id, part FROM " + tableName)).executesWithGpu(TableScanNode.class);
        assertThat(gpuScanSplitCount("SELECT id, part FROM " + tableName)).isBetween(1L, fileCount - 1);
        // Delta Lake has no file statistics for boolean columns, so the second insert's files stay in the
        // composite, but all their row groups are pruned, leaving files without rows between the others
        assertThat(query("SELECT id, part FROM " + tableName + " WHERE flag")).executesWithGpu(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    private void insertOneFilePerStatement(String tableName, String secondColumn, IntFunction<String> secondValue)
    {
        for (int i = 0; i < FILE_COUNT; i++) {
            assertUpdate("INSERT INTO " + tableName + " (id, " + secondColumn + ") VALUES (" + i + ", " + secondValue.apply(i) + ")", 1);
        }
    }

    private long fileCount(String tableName)
    {
        return (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName);
    }

    private long gpuScanSplitCount(@Language("SQL") String sql)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        PlanNodeId scanId = searchFrom(result.queryPlan().orElseThrow().getRoot())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findOnlyElement()
                .getId();
        // The scan is either the GpuOperator's own plan node or fused into it, and the operator runs one driver per split
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("GpuOperator"))
                .filter(summary -> summary.getPlanNodeId().equals(scanId) || summary.getMetrics().getMetrics().containsKey(FUSED_PLAN_NODE_METRIC_PREFIX + scanId))
                .mapToLong(OperatorStats::getTotalDrivers)
                .sum();
    }
}
