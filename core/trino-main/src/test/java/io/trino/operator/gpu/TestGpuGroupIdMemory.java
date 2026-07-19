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

import ai.rapids.cudf.DType;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuGroupIdMemory
{
    private static final int ROWS_PER_PAGE = 200_000;
    private static final int PAGE_COUNT = 4;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testSingleGroupingSet()
    {
        testGroupId(
                List.of(Map.of(0, 0, 1, 1, 2, 2)),
                List.of(DType.INT64, DType.INT64, DType.INT64, DType.INT64),
                List.of(BIGINT, BIGINT, BIGINT));
    }

    @Test
    void testTwoGroupingSets()
    {
        testGroupId(
                List.of(Map.of(0, 0, 1, 1), Map.of(0, 0, 2, 2)),
                List.of(DType.INT64, DType.INT64, DType.INT64, DType.INT64),
                List.of(BIGINT, BIGINT, BIGINT));
    }

    @Test
    void testVarcharColumns()
    {
        testGroupId(
                List.of(Map.of(0, 0), Map.of(1, 1)),
                List.of(DType.STRING, DType.STRING, DType.INT64),
                List.of(VARCHAR, VARCHAR));
    }

    private void testGroupId(List<Map<Integer, Integer>> groupingSetMappings, List<DType> outputTypes, List<Type> inputTypes)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            GpuGroupId.Factory factory = new GpuGroupId.Factory(groupingSetMappings, outputTypes);
            Iterator<Page> inputPages = createPages(inputTypes, NO_NULLS, PAGE_COUNT, ROWS_PER_PAGE, true);

            try (var source = new SettableGpuOperation();
                    GpuOperation groupId = factory.create(context, source)) {
                int outputCount = 0;
                boolean finished = false;
                while (!finished) {
                    switch (groupId.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                memoryValidation.withoutValidation(() -> {
                                    if (inputPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), inputTypes));
                                        source.setPending(context.taskMemoryContext().allocate("source", next.retainedMemory()), next);
                                    }
                                    else {
                                        source.noMoreInput();
                                    }
                                });
                            }
                        }
                        case Data(var allocation, var page) -> {
                            try (allocation; page) {
                                outputCount++;
                            }
                        }
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
                assertThat(outputCount).isEqualTo(PAGE_COUNT * groupingSetMappings.size());
            }
        }
    }
}
