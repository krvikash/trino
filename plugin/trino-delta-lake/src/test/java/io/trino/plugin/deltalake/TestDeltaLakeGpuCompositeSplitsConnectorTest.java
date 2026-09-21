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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDeltaLakeGpuCompositeSplitsConnectorTest
        extends TestDeltaLakeConnectorTest
{
    @Override
    protected DeltaLakeQueryRunner.Builder createQueryRunnerBuilder()
    {
        return super.createQueryRunnerBuilder()
                // GPU execution disables include-coordinator, so an extra worker is needed to
                // match the default 3 data-processing nodes that base tests assume (2 workers + coordinator).
                .setWorkerCount(3)
                .configureGpuDistributedExecution()
                // delta.parquet.time-zone defaults to the JVM zone; pin UTC so timestamp columns stay GPU-eligible
                .addDeltaProperty("delta.parquet.time-zone", "UTC")
                .addDeltaProperty("delta.experimental.composite-splits.enabled", "true");
    }

    @Test
    @Override
    public void testReadVersionedTableWithVacuum()
    {
        // The GPU path reads the footer of the vacuumed file itself and reports a different error than the CPU path
        assertThatThrownBy(super::testReadVersionedTableWithVacuum)
                .hasStackTraceContaining("Failed to create GPU Parquet page source for: ");
    }

    @Test
    @Override
    public void testReadTemporalVersionedTableWithVacuum()
    {
        // The GPU path reads the footer of the vacuumed file itself and reports a different error than the CPU path
        assertThatThrownBy(super::testReadTemporalVersionedTableWithVacuum)
                .hasStackTraceContaining("Failed to create GPU Parquet page source for: ");
    }

    @Test
    @Override
    public void testTimestampPredicatePushdown()
    {
        // TODO: GpuTableScan records physical input only, so the processed input the test checks is always zero
        assertThatThrownBy(super::testTimestampPredicatePushdown)
                .isInstanceOf(AssertionError.class)
                .hasMessageMatching("\\s*Expecting actual:\\s+0L\\s+to be greater than:\\s+0L\\s*")
                .hasStackTraceContaining("TestDeltaLakeConnectorTest.lambda$testTimestampPredicatePushdown$");
    }
}
