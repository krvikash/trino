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

import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuTopNMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int MULTIPLE_PAGES = 4;
    private static final int SMALL_LIMIT = 100;
    private static final int LIMIT_LARGER_THAN_INPUT = ROWS_PER_PAGE * (MULTIPLE_PAGES + 1);

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testSingleBigintColumn()
    {
        testTopN(List.of(BIGINT), SMALL_LIMIT, new int[] {0}, List.of(ASC_NULLS_LAST));
    }

    @Test
    void testSingleVarcharColumn()
    {
        testTopN(List.of(VARCHAR), SMALL_LIMIT, new int[] {0}, List.of(ASC_NULLS_LAST));
    }

    @Test
    void testMultipleColumns()
    {
        testTopN(List.of(BIGINT, VARCHAR, BIGINT), SMALL_LIMIT, new int[] {0, 1}, List.of(ASC_NULLS_LAST, ASC_NULLS_LAST));
    }

    @Test
    void testLimitGreaterThanInput()
    {
        // No row is ever truncated; buffered grows with each input page.
        testTopN(List.of(BIGINT), LIMIT_LARGER_THAN_INPUT, new int[] {0}, List.of(ASC_NULLS_LAST));
    }

    private void testTopN(List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        // pageCount=1 exercises the single-batch path; MULTIPLE_PAGES exercises accumulate+sortAndTruncate per batch.
        testTopN(types, limit, sortChannels, sortOrders, 1);
        testTopN(types, limit, sortChannels, sortOrders, MULTIPLE_PAGES);
    }

    private void testTopN(List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders, int pageCount)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(types, NO_NULLS, pageCount, ROWS_PER_PAGE, true);

            GpuTopN.Factory factory = new GpuTopN.Factory(limit, sortChannels, sortOrders);
            try (var source = new SettableGpuOperation();
                    GpuOperation topN = factory.create(context, source)) {
                boolean finished = false;
                while (!finished) {
                    switch (topN.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                memoryValidation.withoutValidation(() -> {
                                    if (inputPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), types));
                                        source.setPending(context.taskMemoryContext().allocate(getClass().getSimpleName(), next.retainedMemory()), next);
                                    }
                                    else {
                                        source.noMoreInput();
                                    }
                                });
                            }
                        }
                        case Data(var memory, var page) -> {
                            try (memory; page) {
                                // result consumed; memory accounting is the only thing under test
                            }
                        }
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
            }
        }
    }
}
