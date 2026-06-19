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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import static java.util.Objects.requireNonNull;

public class GpuTaskMemoryContext
{
    private final @Borrow AggregatedMemoryContext taskUserMemory;
    private final @Borrow AggregatedMemoryContext taskGpuDeviceMemory;
    private final @Borrow AggregatedMemoryContext taskOffHeapMemory;

    public GpuTaskMemoryContext(
            @Borrow AggregatedMemoryContext taskUserMemory,
            @Borrow AggregatedMemoryContext taskGpuDeviceMemory,
            @Borrow AggregatedMemoryContext taskOffHeapMemory)
    {
        this.taskUserMemory = requireNonNull(taskUserMemory, "taskUserMemory is null");
        this.taskGpuDeviceMemory = requireNonNull(taskGpuDeviceMemory, "taskGpuDeviceMemory is null");
        this.taskOffHeapMemory = requireNonNull(taskOffHeapMemory, "taskOffHeapMemory is null");
    }

    public @Borrow AggregatedMemoryContext taskUserMemory()
    {
        return taskUserMemory;
    }

    public @Borrow AggregatedMemoryContext taskGpuDeviceMemory()
    {
        return taskGpuDeviceMemory;
    }

    public @Borrow AggregatedMemoryContext taskOffHeapMemory()
    {
        return taskOffHeapMemory;
    }

    public @Move AllocatedMemory allocate(String allocationTag, MemoryAmount amount)
    {
        return AllocatedMemory.allocate(this, allocationTag, amount);
    }
}
