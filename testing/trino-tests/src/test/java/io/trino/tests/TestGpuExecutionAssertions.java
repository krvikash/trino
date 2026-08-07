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
package io.trino.tests;

import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies that the GPU execution assertions ([QueryAssert#executesWithGpu] and
/// [QueryAssert#executesWithGpuCpuFallback]) actually discriminate between a table scan that executed fully on
/// the GPU and one that was planned for the GPU but fell back to the CPU reader.
class TestGpuExecutionAssertions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // This uses the Iceberg connector only for technical reasons. TODO make connector-independent by using the mock connector
        return IcebergQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setInitialTables(NATION)
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
    }

    @Test
    void testExecutesWithGpuRequiresFullyNativeScan()
    {
        // Iceberg Parquet scan runs fully on the GPU
        assertThat(query("SELECT nationkey, name FROM nation"))
                .executesWithGpu(TableScanNode.class);
        assertThatThrownBy(() -> assertThat(query("SELECT nationkey, name FROM nation"))
                .executesWithGpuCpuFallback(TableScanNode.class))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Query plan has 1 PlanNodes of class io.trino.sql.planner.plan.TableScanNode, but none of these was fully executed on GPU. These were [TableScanNode], and these were partially []");
    }

    @Test
    void testExecutesWithGpuCpuFallbackRequiresFullFallback()
    {
        // Iceberg ORC scan is planned for the GPU but every split falls back to the CPU reader
        assertUpdate("CREATE TABLE nation_orc WITH (format = 'ORC') AS SELECT nationkey, name FROM nation", 25);
        assertThat(query("SELECT nationkey, name FROM nation_orc"))
                .executesWithGpuCpuFallback(TableScanNode.class);
        assertThatThrownBy(() -> assertThat(query("SELECT nationkey, name FROM nation_orc"))
                .executesWithGpu(TableScanNode.class))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Query plan has 1 PlanNodes of class io.trino.sql.planner.plan.TableScanNode, but none of these was fully executed on GPU. These were [], and these were partially [TableScanNode]");
    }
}
