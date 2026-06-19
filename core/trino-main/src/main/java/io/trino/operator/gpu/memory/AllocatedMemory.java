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

import io.trino.spi.gpu.MemoryAllocation;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Move;

import static java.util.Objects.requireNonNull;

public final class AllocatedMemory
        implements MemoryAllocation,
                   RuntimeCloseable
{
    static @Move AllocatedMemory allocate(GpuTaskMemoryContext memoryContext, String allocationTag, MemoryAmount amount)
    {
        requireNonNull(memoryContext, "memoryContext is null");
        requireNonNull(allocationTag, "allocationTag is null");
        // TODO implement actual allocations
        return new AllocatedMemory(amount);
    }

    private MemoryAmount amount;

    private AllocatedMemory(MemoryAmount amount)
    {
        this.amount = requireNonNull(amount, "amount is null");
    }

    @Override
    public void update(MemoryAmount newAmount)
    {
        // TODO implement
        amount = requireNonNull(newAmount, "newAmount is null");
    }

    public void transferFrom(@Move AllocatedMemory other)
    {
        // TODO implement
    }

    public void transferTags(String newAllocationTag)
    {
        // TODO implement
    }

    public MemoryAmount amount()
    {
        return amount;
    }

    @Override
    public void close()
    {
        // TODO implement
        amount = MemoryAmount.ZERO;
    }
}
