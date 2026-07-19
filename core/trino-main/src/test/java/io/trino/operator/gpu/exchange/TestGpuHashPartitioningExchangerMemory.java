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
package io.trino.operator.gpu.exchange;

import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.exchange.GpuLocalExchangeBuffer.BufferedPage;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuHashPartitioningExchangerMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int PAGE_COUNT = 10;
    private static final int MULTIPLE_PARTITIONS = 4;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testPartitionBigint()
    {
        testPartition(List.of(BIGINT));
    }

    @Test
    void testPartitionVarchar()
    {
        testPartition(List.of(VARCHAR));
    }

    @Test
    void testPartitionDecimal()
    {
        testPartition(List.of(createDecimalType(18, 3)));
    }

    @Test
    void testPartitionMultipleColumns()
    {
        testPartition(List.of(VARCHAR, BIGINT, BOOLEAN, VARCHAR));
    }

    private void testPartition(List<Type> types)
    {
        testPartition(types, 1);
        testPartition(types, MULTIPLE_PARTITIONS);
    }

    private void testPartition(List<Type> types, int partitionCount)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(types, NO_NULLS, PAGE_COUNT, ROWS_PER_PAGE, true);

            LocalExchangeMemoryManager exchangeMemoryManager = new LocalExchangeMemoryManager(1L << 30);
            List<GpuLocalExchangeBuffer> buffers = IntStream.range(0, partitionCount)
                    .mapToObj(_ -> new GpuLocalExchangeBuffer(exchangeMemoryManager, _ -> {}))
                    .toList();
            GpuExchanger exchanger = new GpuHashPartitioningExchanger(buffers, exchangeMemoryManager, new int[] {0}, context);

            while (inputPages.hasNext()) {
                AtomicReference<AllocatedMemory> inputMemory = new AtomicReference<>();
                AtomicReference<GpuPage> inputPage = new AtomicReference<>();
                memoryValidation.withoutValidation(() -> {
                    inputPage.set(getOnlyElement(copyToDevice(List.of(inputPages.next()), types)));
                    inputMemory.set(context.taskMemoryContext().allocate(getClass().getSimpleName(), inputPage.get().retainedMemory()));
                });

                exchanger.accept(inputMemory.get(), inputPage.get());

                for (GpuLocalExchangeBuffer buffer : buffers) {
                    while (true) {
                        BufferedPage buffered = buffer.removePage();
                        if (buffered == null) {
                            break;
                        }
                        try (var closer = UncheckedCloser.create()) {
                            closer.register(buffered.memory());
                            closer.register(buffered.page());
                        }
                    }
                }
            }
        }
    }
}
