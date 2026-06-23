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
package io.trino.operator.gpu.memory;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.spi.gpu.MemoryAmount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static org.assertj.core.api.Assertions.assertThat;

class TestGpuTaskMemoryContext
{
    private TestMemoryReservationHandler heapMemoryHandler;
    private TestMemoryReservationHandler gpuDeviceMemoryHandler;
    private TestMemoryReservationHandler offHeapMemoryHandler;

    private AggregatedMemoryContext heapMemory;
    private AggregatedMemoryContext gpuDeviceMemory;
    private AggregatedMemoryContext offHeapMemory;

    private GpuTaskMemoryContext context;

    @BeforeEach
    void setup()
    {
        heapMemoryHandler = new TestMemoryReservationHandler();
        gpuDeviceMemoryHandler = new TestMemoryReservationHandler();
        offHeapMemoryHandler = new TestMemoryReservationHandler();
        heapMemory = newRootAggregatedMemoryContext(heapMemoryHandler, 0);
        gpuDeviceMemory = newRootAggregatedMemoryContext(gpuDeviceMemoryHandler, 0);
        offHeapMemory = newRootAggregatedMemoryContext(offHeapMemoryHandler, 0);
        context = new GpuTaskMemoryContext(heapMemory, gpuDeviceMemory, offHeapMemory);
    }

    @Test
    void testAllocate()
    {
        try (AllocatedMemory heapAllocation = context.allocate("foo", MemoryAmount.heap(1))) {
            assertThat(heapMemory.getBytes()).isEqualTo(1);
            assertThat(gpuDeviceMemory.getBytes()).isEqualTo(0);
            assertThat(offHeapMemory.getBytes()).isEqualTo(0);

            assertThat(heapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 1L));
            assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEmpty();
            assertThat(offHeapMemoryHandler.allocationPerTag).isEmpty();

            try (AllocatedMemory gpuAllocation = context.allocate("foo", MemoryAmount.gpuDevice(2))) {
                assertThat(heapMemory.getBytes()).isEqualTo(1);
                assertThat(gpuDeviceMemory.getBytes()).isEqualTo(2);
                assertThat(offHeapMemory.getBytes()).isEqualTo(0);

                assertThat(heapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 1L));
                assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 2L));
                assertThat(offHeapMemoryHandler.allocationPerTag).isEmpty();

                try (AllocatedMemory offHeapAllocation = context.allocate("foo", MemoryAmount.offHeap(3))) {
                    assertThat(heapMemory.getBytes()).isEqualTo(1);
                    assertThat(gpuDeviceMemory.getBytes()).isEqualTo(2);
                    assertThat(offHeapMemory.getBytes()).isEqualTo(3);

                    assertThat(heapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 1L));
                    assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 2L));
                    assertThat(offHeapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 3L));
                }

                assertThat(heapMemory.getBytes()).isEqualTo(1);
                assertThat(gpuDeviceMemory.getBytes()).isEqualTo(2);
                assertThat(offHeapMemory.getBytes()).isEqualTo(0);

                assertThat(heapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 1L));
                assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 2L));
                assertThat(offHeapMemoryHandler.allocationPerTag).isEmpty();
            }

            assertThat(heapMemory.getBytes()).isEqualTo(1);
            assertThat(gpuDeviceMemory.getBytes()).isEqualTo(0);
            assertThat(offHeapMemory.getBytes()).isEqualTo(0);

            assertThat(heapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 1L));
            assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEmpty();
            assertThat(offHeapMemoryHandler.allocationPerTag).isEmpty();
        }

        assertThat(heapMemory.getBytes()).isEqualTo(0);
        assertThat(gpuDeviceMemory.getBytes()).isEqualTo(0);
        assertThat(offHeapMemory.getBytes()).isEqualTo(0);

        assertThat(heapMemoryHandler.allocationPerTag).isEmpty();
        assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEmpty();
        assertThat(offHeapMemoryHandler.allocationPerTag).isEmpty();
    }

    @Test
    void testAllocateAllAtOnce()
    {
        try (AllocatedMemory allocation = context.allocate("foo", new MemoryAmount(11, 22, 33))) {
            assertThat(heapMemory.getBytes()).isEqualTo(11);
            assertThat(gpuDeviceMemory.getBytes()).isEqualTo(22);
            assertThat(offHeapMemory.getBytes()).isEqualTo(33);

            assertThat(heapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 11L));
            assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 22L));
            assertThat(offHeapMemoryHandler.allocationPerTag).isEqualTo(Map.of("foo", 33L));
        }

        assertThat(heapMemory.getBytes()).isEqualTo(0);
        assertThat(gpuDeviceMemory.getBytes()).isEqualTo(0);
        assertThat(offHeapMemory.getBytes()).isEqualTo(0);

        assertThat(heapMemoryHandler.allocationPerTag).isEmpty();
        assertThat(gpuDeviceMemoryHandler.allocationPerTag).isEmpty();
        assertThat(offHeapMemoryHandler.allocationPerTag).isEmpty();
    }

    private static final class TestMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private final Map<String, Long> allocationPerTag = new ConcurrentHashMap<>();

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            allocationPerTag.merge(allocationTag, delta, Long::sum);
            allocationPerTag.compute(allocationTag, (_, value) -> value != 0 ? value : null /* remove */);
            return NOT_BLOCKED;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            throw new UnsupportedOperationException();
        }
    }
}
