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
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Set;

import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
class TestGpuPageMemory
{
    private static final int POSITION_COUNT = 1024;

    @Test
    void testRetainedDeviceMemoryMatchesActualAllocations()
    {
        maybeSetGpuMemoryPoolForTests();

        for (Type type : TESTED_GPU_TYPES) {
            for (NullsProvider nullsProvider : NullsProvider.values()) {
                verifyMemoryAccounting(type, nullsProvider);
            }
        }
    }

    private static void verifyMemoryAccounting(Type type, NullsProvider nullsProvider)
    {
        Block block = createBlock(type, POSITION_COUNT, nullsProvider);
        Page inputPage = new Page(block);
        long baseline = Rmm.getTotalBytesAllocated();

        try (BufferPages bufferPages = new BufferPages();
                CopyToDevice copyToDevice = new CopyToDevice(bufferPages, List.of(type), Set.of(0))) {
            bufferPages.addInput(inputPage);
            bufferPages.noMoreInput();

            GpuOperation.Result result = copyToDevice.execute();
            while (result instanceof Yielded) {
                result = copyToDevice.execute();
            }
            try (GpuPage gpuPage = ((Data) result).page()) {
                assertThat(gpuPage.retainedDeviceMemoryBytes())
                        .as("type=%s nullsProvider=%s", type, nullsProvider)
                        .isEqualTo(Rmm.getTotalBytesAllocated() - baseline);
            }
            assertThat(copyToDevice.execute()).isInstanceOf(Finished.class);
            assertThat(Rmm.getTotalBytesAllocated()).as("liveBytes after free").isEqualTo(baseline);
        }
    }
}
