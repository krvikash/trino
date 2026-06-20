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
package io.trino.operator.gpu.aggregation;

import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
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

import static ai.rapids.cudf.DType.INT64;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuAggregationMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int MULTIPLE_PAGES = 4;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testGlobalCount()
    {
        testAggregation(
                List.of(BIGINT),
                new int[] {},
                List.of(),
                List.of(new GpuCountNonNull(0, BIGINT, INT64)));
    }

    @Test
    void testGroupByCountBigintKey()
    {
        testAggregation(
                List.of(BIGINT, BIGINT),
                new int[] {0},
                List.of(BIGINT),
                List.of(new GpuCountNonNull(1, BIGINT, INT64)));
    }

    @Test
    void testGroupByCountVarcharKey()
    {
        testAggregation(
                List.of(VARCHAR, BIGINT),
                new int[] {0},
                List.of(VARCHAR),
                List.of(new GpuCountNonNull(1, BIGINT, INT64)));
    }

    @Test
    void testFinalGroupByCountBigintKey()
    {
        // Exercises inputRaw=false code path through compactPeakMultiplier.
        testAggregation(
                List.of(BIGINT, BIGINT),
                new int[] {0},
                List.of(BIGINT),
                List.of(new GpuCountNonNull(1, BIGINT, INT64)),
                /*inputRaw=*/ false);
    }

    @Test
    void testFinalGroupByCountVarcharKey()
    {
        testAggregation(
                List.of(VARCHAR, BIGINT),
                new int[] {0},
                List.of(VARCHAR),
                List.of(new GpuCountNonNull(1, BIGINT, INT64)),
                /*inputRaw=*/ false);
    }

    private void testAggregation(List<Type> inputTypes, int[] groupByChannels, List<Type> groupByTypes, List<GpuAggregateFunction> aggregates)
    {
        testAggregation(inputTypes, groupByChannels, groupByTypes, aggregates, /*inputRaw=*/ true);
    }

    private void testAggregation(List<Type> inputTypes, int[] groupByChannels, List<Type> groupByTypes, List<GpuAggregateFunction> aggregates, boolean inputRaw)
    {
        // pageCount=1 exercises the no-mid-stream-compaction path;
        // MULTIPLE_PAGES with threshold=1 forces a compaction per buffered page plus a final compaction.
        testAggregation(inputTypes, groupByChannels, groupByTypes, aggregates, inputRaw, 1);
        testAggregation(inputTypes, groupByChannels, groupByTypes, aggregates, inputRaw, MULTIPLE_PAGES);
    }

    private void testAggregation(List<Type> inputTypes, int[] groupByChannels, List<Type> groupByTypes, List<GpuAggregateFunction> aggregates, boolean inputRaw, int pageCount)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(inputTypes, NO_NULLS, pageCount, ROWS_PER_PAGE, true);

            GpuAggregation.Factory factory = new GpuAggregation.Factory(
                    aggregates,
                    groupByChannels,
                    groupByTypes,
                    inputRaw,
                    /*compactionThresholdBytes=*/ 1,
                    inputTypes.size());
            try (var source = new SettableGpuOperation();
                    GpuOperation aggregation = factory.create(context, source)) {
                boolean finished = false;
                while (!finished) {
                    switch (aggregation.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                memoryValidation.withoutValidation(() -> {
                                    if (inputPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), inputTypes));
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
