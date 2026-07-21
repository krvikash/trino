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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.spi.gpu.MemoryAmount;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAllocatedMemory
{
    private static final long HEAP_LIMIT = 1001;
    private static final long GPU_DEVICE_LIMIT = 2002;
    private static final long OFF_HEAP_LIMIT = 3003;

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
        heapMemoryHandler = new TestMemoryReservationHandler(HEAP_LIMIT);
        gpuDeviceMemoryHandler = new TestMemoryReservationHandler(GPU_DEVICE_LIMIT);
        offHeapMemoryHandler = new TestMemoryReservationHandler(OFF_HEAP_LIMIT);
        heapMemory = newRootAggregatedMemoryContext(heapMemoryHandler, 0);
        gpuDeviceMemory = newRootAggregatedMemoryContext(gpuDeviceMemoryHandler, 0);
        offHeapMemory = newRootAggregatedMemoryContext(offHeapMemoryHandler, 0);
        context = new GpuTaskMemoryContext(heapMemory, gpuDeviceMemory, offHeapMemory);
    }

    @AfterEach
    void verifyNoLeak()
    {
        assertAllocations(Map.of());
    }

    @Test
    void testAllocate()
    {
        try (AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
            assertThat(allocation.amount()).isEqualTo(new MemoryAmount(11, 22, 33));
            assertAllocations(Map.of("foo", new MemoryAmount(11, 22, 33)));
        }
    }

    @Test
    void testAllocateMultiple()
    {
        try (AllocatedMemory allocationOne = AllocatedMemory.allocate(context, "foo", new MemoryAmount(1, 2, 3))) {
            try (AllocatedMemory allocationTwo = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
                assertAllocations(Map.of("foo", new MemoryAmount(12, 24, 36)));
            }
            assertAllocations(Map.of("foo", new MemoryAmount(1, 2, 3)));
            try (AllocatedMemory allocationThree = AllocatedMemory.allocate(context, "bar", new MemoryAmount(11, 22, 33))) {
                assertAllocations(Map.of(
                        "foo", new MemoryAmount(1, 2, 3),
                        "bar", new MemoryAmount(11, 22, 33)));
            }
        }
    }

    @Test
    void testAllocateTooMuchHeap()
    {
        assertThatThrownBy(() -> AllocatedMemory.allocate(context, "foo", new MemoryAmount(HEAP_LIMIT + 1, 5, 7)))
                .hasMessage("Cannot satisfy allocation of 1002 while 1001 bytes remaining");
    }

    @Test
    void testAllocateTooMuchGpuDevice()
    {
        assertThatThrownBy(() -> AllocatedMemory.allocate(context, "foo", new MemoryAmount(3, GPU_DEVICE_LIMIT + 5, 7)))
                .hasMessage("Cannot satisfy allocation of 2007 while 2002 bytes remaining");
    }

    @Test
    void testAllocateTooMuchOffHeap()
    {
        assertThatThrownBy(() -> AllocatedMemory.allocate(context, "foo", new MemoryAmount(3, 5, OFF_HEAP_LIMIT + 7)))
                .hasMessage("Cannot satisfy allocation of 3010 while 3003 bytes remaining");
    }

    @Test
    void testAllocateZero()
    {
        try (AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", MemoryAmount.ZERO)) {
            assertThat(allocation.amount()).isEqualTo(MemoryAmount.ZERO);
            assertAllocations(Map.of());
        }
    }

    @Test
    void testUpdateIncrease()
    {
        try (AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
            allocation.update(new MemoryAmount(50, 60, 70));
            assertThat(allocation.amount()).isEqualTo(new MemoryAmount(50, 60, 70));
            assertAllocations(Map.of("foo", new MemoryAmount(50, 60, 70)));
        }
    }

    @Test
    void testUpdateDecrease()
    {
        try (AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", new MemoryAmount(50, 60, 70))) {
            allocation.update(new MemoryAmount(11, 22, 33));
            assertThat(allocation.amount()).isEqualTo(new MemoryAmount(11, 22, 33));
            assertAllocations(Map.of("foo", new MemoryAmount(11, 22, 33)));
        }
    }

    @Test
    void testUpdateToZero()
    {
        try (AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
            allocation.update(MemoryAmount.ZERO);

            assertThat(allocation.amount()).isEqualTo(MemoryAmount.ZERO);
            assertAllocations(Map.of());
        }
    }

    @Test
    void testClose()
    {
        AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33));
        assertAllocations(Map.of("foo", new MemoryAmount(11, 22, 33)));
        allocation.close();
        assertAllocations(Map.of());
    }

    @Test
    void testDoubleClose()
    {
        AllocatedMemory allocation = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33));
        allocation.close();
        // close is idempotent
        allocation.close();
    }

    @Test
    void testRetag()
    {
        try (AllocatedMemory allocationOne = AllocatedMemory.allocate(context, "foo", new MemoryAmount(1, 2, 3));
                AllocatedMemory allocationTwo = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
            allocationTwo.retag("bar");
            assertAllocations(Map.of(
                    "foo", new MemoryAmount(1, 2, 3),
                    "bar", new MemoryAmount(11, 22, 33)));
        }
    }

    @Test
    void testTransferFrom()
    {
        try (AllocatedMemory target = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
            try (AllocatedMemory source = AllocatedMemory.allocate(context, "bar", new MemoryAmount(100, 200, 300))) {
                target.transferFrom(source);
                assertThat(target.amount()).isEqualTo(new MemoryAmount(111, 222, 333));
                assertThat(source.amount()).isEqualTo(MemoryAmount.ZERO);
                assertAllocations(Map.of("foo", new MemoryAmount(111, 222, 333)));
            }
            // Closing source does not change anything
            assertAllocations(Map.of("foo", new MemoryAmount(111, 222, 333)));
        }
    }

    @Test
    void testTransferFromZero()
    {
        try (AllocatedMemory target = AllocatedMemory.allocate(context, "foo", new MemoryAmount(11, 22, 33))) {
            try (AllocatedMemory source = AllocatedMemory.allocate(context, "bar", MemoryAmount.ZERO)) {
                target.transferFrom(source);
                assertThat(target.amount()).isEqualTo(new MemoryAmount(11, 22, 33));
                assertThat(source.amount()).isEqualTo(MemoryAmount.ZERO);
                assertAllocations(Map.of("foo", new MemoryAmount(11, 22, 33)));
            }
            // Closing source does not change anything
            assertAllocations(Map.of("foo", new MemoryAmount(11, 22, 33)));
        }
    }

    @Test
    void testPartialTransferFrom()
    {
        try (AllocatedMemory target = AllocatedMemory.allocate(context, "foo", MemoryAmount.ZERO)) {
            try (AllocatedMemory source = AllocatedMemory.allocate(context, "bar", new MemoryAmount(100, 200, 300))) {
                target.transferFrom(source, new MemoryAmount(10, 20, 30));
                assertThat(target.amount()).isEqualTo(new MemoryAmount(10, 20, 30));
                assertThat(source.amount()).isEqualTo(new MemoryAmount(90, 180, 270));
                assertAllocations(Map.of(
                        "foo", new MemoryAmount(10, 20, 30),
                        "bar", new MemoryAmount(90, 180, 270)));
            }
            // Closing source does not change anything
            assertAllocations(Map.of("foo", new MemoryAmount(10, 20, 30)));
        }
    }

    private void assertAllocations(Map<String, MemoryAmount> allTaggedAllocations)
    {
        long expectedHeap = allTaggedAllocations.values().stream().map(MemoryAmount::heapBytes).reduce(Long::sum).orElse(0L);
        long expectedGpuDevice = allTaggedAllocations.values().stream().map(MemoryAmount::gpuDeviceBytes).reduce(Long::sum).orElse(0L);
        long expectedOffHeap = allTaggedAllocations.values().stream().map(MemoryAmount::offHeapBytes).reduce(Long::sum).orElse(0L);

        assertThat(heapMemory.getBytes()).isEqualTo(expectedHeap);
        assertThat(gpuDeviceMemory.getBytes()).isEqualTo(expectedGpuDevice);
        assertThat(offHeapMemory.getBytes()).isEqualTo(expectedOffHeap);

        assertThat((Object) heapMemoryHandler.taggedBytes)
                .isEqualTo(allTaggedAllocations.entrySet().stream()
                        .collect(toImmutableMultiset(Map.Entry::getKey, entry -> toIntExact(entry.getValue().heapBytes()))));
        assertThat((Object) gpuDeviceMemoryHandler.taggedBytes)
                .isEqualTo(allTaggedAllocations.entrySet().stream()
                        .collect(toImmutableMultiset(Map.Entry::getKey, entry -> toIntExact(entry.getValue().gpuDeviceBytes()))));
        assertThat((Object) offHeapMemoryHandler.taggedBytes)
                .isEqualTo(allTaggedAllocations.entrySet().stream()
                        .collect(toImmutableMultiset(Map.Entry::getKey, entry -> toIntExact(entry.getValue().offHeapBytes()))));
    }

    private static final class TestMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private long remaining;
        private final Multiset<String> taggedBytes = HashMultiset.create();

        public TestMemoryReservationHandler(long limit)
        {
            checkArgument(limit >= 0, "limit must be non-negative");
            this.remaining = limit;
        }

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            checkState(delta <= remaining, "Cannot satisfy allocation of %s while %s bytes remaining", delta, remaining);
            remaining -= delta;

            if (delta >= 0) {
                taggedBytes.add(allocationTag, toIntExact(delta));
            }
            else {
                remove(taggedBytes, allocationTag, toIntExact(-delta));
            }
            return NOT_BLOCKED;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void transferTags(String fromTag, String toTag, long bytes)
        {
            checkArgument(bytes > 0, "Bytes transferred must be positive");
            remove(taggedBytes, fromTag, toIntExact(bytes));
            taggedBytes.add(toTag, toIntExact(bytes));
        }
    }

    private static <T> void remove(Multiset<T> multiset, T element, int occurrences)
    {
        int count = multiset.count(element);
        checkState(occurrences <= count, "Cannot remove %s occurrences of %s, only %s are available", occurrences, element, count);
        verify(multiset.setCount(element, count, count - occurrences), "Concurrent modification");
    }
}
