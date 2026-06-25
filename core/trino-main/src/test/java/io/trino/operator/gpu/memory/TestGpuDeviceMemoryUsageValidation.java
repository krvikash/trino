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

import ai.rapids.cudf.ColumnVector;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.MemoryAmount;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuDeviceMemoryUsageValidation
{
    private static final int MEGABYTE = 1024 * 1024;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testAllocateNothing()
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        GpuDeviceMemoryUsageValidation.createAndRegister(context, 0)
                .close();
    }

    @Test
    void testAllocateLittle()
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation _ = GpuDeviceMemoryUsageValidation.createAndRegister(context, MEGABYTE)) {
            try (DeviceMemory column = new DeviceMemory(ColumnVector.fromInts(1, 2, 3, 4));
                    AllocatedMemory _ = context.taskMemoryContext()
                            .allocate("test", new MemoryAmount(0, column.retainedDeviceMemoryBytes(), 0))) {
                assertThat(context.taskMemoryContext().taskGpuDeviceMemory().getBytes())
                        .isEqualTo(column.retainedDeviceMemoryBytes());
            }
        }
    }

    @Test
    void testAllocateMoreThanMargin()
    {
        assertThatThrownBy(() -> {
            TestingGpuOperationContext context = new TestingGpuOperationContext();
            try (GpuDeviceMemoryUsageValidation _ = GpuDeviceMemoryUsageValidation.createAndRegister(context, 0)) {
                try (ClosingRef<AllocatedMemory> allocation = ClosingRef.empty();
                        DeviceMemory column = new DeviceMemory(ColumnVector.fromInts(1, 2, 3, 4))) {
                    allocation.set(
                            context.taskMemoryContext().allocate("test", new MemoryAmount(0, column.retainedDeviceMemoryBytes(), 0)));
                }
            }
        })
                .isInstanceOf(AssertionError.class)
                .hasMessage("There are 1 errors recorded, first few being shown")
                .satisfies(thrown -> assertThat(getOnlyElement(asList(thrown.getSuppressed())))
                        .hasMessage("Actual GPU device memory usage peak 16 exceeded previous reservation of 0 by 16 (Infinity %), new reservation is 16, delta 16"));
    }

    @Test
    void testReservationWithoutAllocation()
    {
        assertThatThrownBy(() -> {
            TestingGpuOperationContext context = new TestingGpuOperationContext();
            try (GpuDeviceMemoryUsageValidation _ = GpuDeviceMemoryUsageValidation.createAndRegister(context, MEGABYTE)) {
                // Reserve 10MB but never allocate matching GPU memory; releasing fires
                // the "actual peak << reservation" check.
                context.taskMemoryContext()
                        .allocate("unbacked", new MemoryAmount(0, 10 * MEGABYTE, 0))
                        .close();
            }
        })
                .isInstanceOf(AssertionError.class)
                .hasMessage("There are 1 errors recorded, first few being shown")
                .satisfies(thrown -> assertThat(getOnlyElement(asList(thrown.getSuppressed())))
                        .hasMessage("Actual GPU device memory usage peak 0 is significantly lower than previous reservation 10485760 by 100.0 %"));
    }

    @Test
    void testReservationTooMuch()
    {
        assertThatThrownBy(() -> {
            TestingGpuOperationContext context = new TestingGpuOperationContext();
            try (GpuDeviceMemoryUsageValidation _ = GpuDeviceMemoryUsageValidation.createAndRegister(context, MEGABYTE)) {
                try (AllocatedMemory allocation = context.taskMemoryContext()
                        .allocate("too much", new MemoryAmount(0, 10 * MEGABYTE, 0))) {
                    int bytesAllocated = MEGABYTE * 13 / 2;
                    getOnlyElement(copyToDevice(
                            List.of(new Page(new ByteArrayBlock(
                                    bytesAllocated,
                                    Optional.empty(),
                                    new byte[bytesAllocated]))),
                            List.of(BOOLEAN)))
                            .close();
                }
            }
        })
                .isInstanceOf(AssertionError.class)
                .hasMessage("There are 1 errors recorded, first few being shown")
                .satisfies(thrown -> assertThat(getOnlyElement(asList(thrown.getSuppressed())))
                        .hasMessage("Actual GPU device memory usage peak 6815744 is significantly lower than previous reservation 10485760 by 35.0 %"));
    }

    @Test
    void testUnreleasedReservation()
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        assertThatThrownBy(() -> {
            try (GpuDeviceMemoryUsageValidation _ = GpuDeviceMemoryUsageValidation.createAndRegister(context, MEGABYTE)) {
                // Reserve but never release.
                @SuppressWarnings("resource")
                AllocatedMemory _ = context.taskMemoryContext().allocate("leak", new MemoryAmount(0, 100, 0));
            }
        })
                .isInstanceOf(AssertionError.class)
                .hasMessage("There are 1 errors recorded, first few being shown")
                .satisfies(thrown -> assertThat(getOnlyElement(asList(thrown.getSuppressed())))
                        .hasMessage("Reservation at finish is non-zero: 100"));
    }
}
