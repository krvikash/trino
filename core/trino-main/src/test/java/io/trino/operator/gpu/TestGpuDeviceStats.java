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
package io.trino.operator.gpu;

import ai.rapids.cudf.Rmm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD)
class TestGpuDeviceStats
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testNoGpuReturnsMinusOne()
            throws Exception
    {
        assumeTrue(!Rmm.isInitialized(), "Skipped on GPU instances — use testWithGpu instead");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        GpuDeviceStats stats = new GpuDeviceStats(new GpuConfig(), executor);
        stats.start();
        try {
            assertThat(stats.getDeviceTotalBytes()).isEqualTo(-1);
            assertThat(stats.getDeviceUsedBytes()).isEqualTo(-1);
            // RMM calls throw when not initialized; catch returns -1
            assertThat(stats.getRmmAllocatedBytes()).isEqualTo(-1);
            assertThat(stats.getRmmPeakAllocatedBytes()).isEqualTo(-1);
        }
        finally {
            stats.stop();
            executor.shutdownNow();
        }
    }

    @Test
    void testWithGpu()
            throws Exception
    {
        assumeTrue(Rmm.isInitialized(), "Requires a GPU — run on a GPU instance");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        GpuDeviceStats stats = new GpuDeviceStats(new GpuConfig(), executor);
        stats.start();
        try {
            assertThat(stats.getDeviceTotalBytes()).isGreaterThan(0);
            assertThat(stats.getDeviceUsedBytes()).isGreaterThan(0).isLessThanOrEqualTo(stats.getDeviceTotalBytes());
            assertThat(stats.getRmmAllocatedBytes()).isGreaterThanOrEqualTo(0);
            assertThat(stats.getRmmPeakAllocatedBytes()).isGreaterThanOrEqualTo(stats.getRmmAllocatedBytes());
        }
        finally {
            stats.stop();
            executor.shutdownNow();
        }
    }
}
