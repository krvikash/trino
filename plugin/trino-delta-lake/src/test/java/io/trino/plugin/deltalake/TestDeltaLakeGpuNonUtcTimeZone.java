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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeGpuNonUtcTimeZone
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .configureGpuDistributedExecution()
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.parquet.time-zone", "UTC")
                .addDeltaProperty("delta.time-zone", "America/New_York")
                .build();
    }

    @Test
    void testTimestampWithTimeZone()
    {
        String tableName = "test_gpu_non_utc_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, ts timestamp(3) with time zone)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2020-01-01 12:34:56.789 UTC'), (2, NULL)", 2);

        assertThat(query("SELECT id, ts FROM " + tableName)).executesWithoutGpu();
        assertThat(query("SELECT id FROM " + tableName)).executesWithGpuCpuFallback(TableScanNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testFileModifiedTime()
    {
        String tableName = "test_gpu_non_utc_modified_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);

        assertThat(query("SELECT id, \"$file_modified_time\" FROM " + tableName)).executesWithoutGpu();

        assertUpdate("DROP TABLE " + tableName);
    }
}
