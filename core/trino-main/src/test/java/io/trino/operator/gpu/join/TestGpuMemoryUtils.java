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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.Table;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getHashJoinAdditionalGpuDeviceMemoryUsage;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
class TestGpuMemoryUtils
{
    @Test
    void testGetHashJoinAdditionalGpuDeviceMemoryUsage()
    {
        maybeSetGpuMemoryPoolForTests();

        for (Type type : TESTED_GPU_TYPES) {
            for (NullsProvider nullsProvider : NullsProvider.values()) {
                for (Integer positionCount : List.of(1, 10, 25, 1024, 10_000, 1_234_567)) {
                    testGetHashJoinAdditionalGpuDeviceMemoryUsage(type, nullsProvider, positionCount);
                }
            }
        }
    }

    private static void testGetHashJoinAdditionalGpuDeviceMemoryUsage(Type type, NullsProvider nullsProvider, int positionCount)
    {
        Block block = createBlock(type, positionCount, nullsProvider);
        long baseline = Rmm.getTotalBytesAllocated();

        try (GpuPage gpuPage = getOnlyElement(copyToDevice(List.of(new Page(block)), List.of(type)));
                Table keyTable = toTable(gpuPage)) {
            long afterKeyTable = Rmm.getTotalBytesAllocated();

            try (HashJoin hashJoin = new HashJoin(keyTable, /*compareNullsEqual=*/ false)) {
                long actual = Rmm.getTotalBytesAllocated() - afterKeyTable;
                long reported = getHashJoinAdditionalGpuDeviceMemoryUsage(keyTable);

                assertThat(reported)
                        .as("reported >= actual, type=%s nullsProvider=%s positionCount=%s", type, nullsProvider, positionCount)
                        .isGreaterThanOrEqualTo(actual);

                if (reported < 1024) {
                    // "small" values can be overestimated as long as the estimate is also "small"
                }
                else {
                    assertThat(reported)
                            .as("reported within 5%% of actual, type=%s nullsProvider=%s positionCount=%s", type, nullsProvider, positionCount)
                            .isLessThanOrEqualTo((long) (actual * 1.05));
                }
            }
        }
        assertThat(Rmm.getTotalBytesAllocated())
                .as("liveBytes after free, type=%s nullsProvider=%s positionCount=%s", type, nullsProvider, positionCount)
                .isEqualTo(baseline);
    }
}
