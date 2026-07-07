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

import io.trino.Session;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_MIN_WRITER_COUNT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergGpuCompositeSplitsConnectorTest
        extends BaseIcebergCompositeSplitsConnectorTest
{
    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return super.createQueryRunnerBuilder()
                // GPU execution disables include-coordinator, so an extra worker is needed to
                // match the default 3 data-processing nodes that base tests assume (2 workers + coordinator).
                .setWorkerCount(3)
                .configureGpuDistributedExecution();
    }

    @Test
    @Override
    public void testSplitPruningForFilterOnPartitionColumn()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-18150
        assertThatThrownBy(super::testSplitPruningForFilterOnPartitionColumn)
                .hasStackTraceContaining("Couldn't find operator summary, probably due to query statistic collection error");
    }

    @Test
    @Override
    public void testSplitPruningForFilterOnNonPartitionColumn()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-18150
        assertThatThrownBy(super::testSplitPruningForFilterOnNonPartitionColumn)
                .hasStackTraceContaining("Couldn't find operator summary, probably due to query statistic collection error");
    }

    @Test
    @Override
    public void testSplitPruningFromDataFileStatistics()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-18150
        assertThatThrownBy(super::testSplitPruningFromDataFileStatistics)
                .hasStackTraceContaining("Couldn't find operator summary, probably due to query statistic collection error");
    }

    @Test
    @Override
    public void testDeleteOnIdentityPartitionedTableProducesOneDeleteEntryPerDataFile()
    {
        // TODO: The error message is misleading due to https://starburstdata.atlassian.net/browse/ENG-18118,
        //  but it indicates that there was an attempt to fall back to the CPU because deletes are not yet
        //  supported on the GPU, which is expected.
        assertThatThrownBy(super::testDeleteOnIdentityPartitionedTableProducesOneDeleteEntryPerDataFile)
                .hasMessageMatching("Execution of 'actual' query .* failed: SELECT count\\(\\*\\) FROM test_identity_partitioned_deletes_.*")
                .hasStackTraceContaining("No columns to copy");
    }

    @Test
    @Override
    public void testInsertIntoBucketedColumnTaskWriterCount()
    {
        // Base test uses taskWriterCount=4 and asserts it's greater than getNodeCount().
        // With 3 workers + coordinator, getNodeCount() returns 4, so we need a higher value that is also a power of 2.
        int taskWriterCount = 8;
        assertThat(taskWriterCount).isGreaterThan(getQueryRunner().getNodeCount());
        Session session = Session.builder(getSession())
                .setSystemProperty(TASK_MIN_WRITER_COUNT, String.valueOf(taskWriterCount))
                .setSystemProperty(TASK_MAX_WRITER_COUNT, String.valueOf(taskWriterCount))
                .build();

        String tableName = "test_inserting_into_bucketed_column_task_writer_count_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x INT) WITH (partitioning = ARRAY['bucket(x, 7)'])");

        assertUpdate(session, "INSERT INTO " + tableName + " SELECT nationkey FROM nation", 25);
        assertQuery("SELECT * FROM " + tableName, "SELECT nationkey FROM nation");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testScanMetrics()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-18150
        assertThatThrownBy(super::testScanMetrics)
                .hasStackTraceContaining("Couldn't find operator summary, probably due to query statistic collection error");
    }

    @Test
    @Override
    public void testIgnoreParquetStatistics()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-18150
        assertThatThrownBy(super::testIgnoreParquetStatistics)
                .hasStackTraceContaining("Couldn't find operator summary, probably due to query statistic collection error");
    }

    @Test
    @Override
    public void testPushdownPredicateToParquetAfterColumnRename()
    {
        // TODO: https://starburstdata.atlassian.net/browse/ENG-18150
        assertThatThrownBy(super::testPushdownPredicateToParquetAfterColumnRename)
                .hasStackTraceContaining("Couldn't find operator summary, probably due to query statistic collection error");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        // TODO: The error message is misleading due to https://starburstdata.atlassian.net/browse/ENG-18118,
        //  but it indicates that there was an attempt to fall back to the CPU because deletes are not yet
        //  supported on the GPU, which is expected.
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessageMatching("Execution of 'actual' query .* failed: SELECT count\\(\\*\\) FROM test_row_level_delete.*")
                .hasStackTraceContaining("No columns to copy");
    }
}
