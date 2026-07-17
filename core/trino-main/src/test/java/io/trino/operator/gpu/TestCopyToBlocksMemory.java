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

import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.TestingGpuOperationContext.ReservationListener;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static org.assertj.core.api.Assertions.assertThat;

class TestCopyToBlocksMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int PAGE_COUNT = 10;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCopyToBlocksFromDevice(NullsProvider nullsProvider)
    {
        for (Type type : TESTED_GPU_TYPES) {
            TestingGpuOperationContext context = new TestingGpuOperationContext();

            TestingHeapReservationListener heapListener = new TestingHeapReservationListener();
            context.setHeapMemoryReservationListener(heapListener);

            Iterator<Page> inputPages = createPages(List.of(type), nullsProvider, PAGE_COUNT, ROWS_PER_PAGE, true);
            try (var source = new SettableGpuOperation();
                    GpuOperation copyToBlocks = new CopyToBlocks(context, source, List.of(type))) {
                boolean finished = false;
                while (!finished) {
                    switch (copyToBlocks.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                if (inputPages.hasNext()) {
                                    GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), List.of(type)));
                                    source.setPending(context.taskMemoryContext().allocate(getClass().getSimpleName(), next.retainedMemory()), next);
                                }
                                else {
                                    source.noMoreInput();
                                }
                            }
                        }
                        case Data(AllocatedMemory memory, GpuPage page) -> {
                            try (memory; page) {
                                assertThat(page.retainedDeviceMemoryBytes()).isEqualTo(0);
                                assertThat(page.retainedOffHeapMemoryBytes()).isEqualTo(0);

                                // Expect exactly two heap reservations: the upfront estimate, then reconciliation to actual size.
                                List<Long> reservations = heapListener.getReservations();
                                assertThat(reservations).hasSize(2);
                                long estimated = reservations.getFirst();
                                long actual = reservations.getLast();
                                assertThat(estimated)
                                        .as("heap estimation for %s", type)
                                        .isBetween((long) (page.retainedHeapMemoryBytes() * 0.99), page.retainedHeapMemoryBytes());
                                assertThat(page.retainedHeapMemoryBytes()).isEqualTo(actual);
                            }
                            finally {
                                heapListener.clear();
                            }
                        }
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
            }

            context.removeHeapMemoryReservationListener(heapListener);
        }
    }

    private static class TestingHeapReservationListener
            implements ReservationListener
    {
        private final List<Long> reservations = new ArrayList<>();

        @Override
        public void onReservationChange(long currentReservation, long delta)
        {
            reservations.add(currentReservation);
        }

        public List<Long> getReservations()
        {
            return ImmutableList.copyOf(reservations);
        }

        public void clear()
        {
            reservations.clear();
        }
    }
}
