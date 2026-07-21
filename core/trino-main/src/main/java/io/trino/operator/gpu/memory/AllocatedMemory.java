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
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.MemoryAllocation;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

public final class AllocatedMemory
        implements MemoryAllocation,
                   RuntimeCloseable
{
    private static final AllocatedMemory UNTRACKED = new AllocatedMemory(
            new GpuTaskMemoryContext(
                    newSimpleAggregatedMemoryContext(),
                    newSimpleAggregatedMemoryContext(),
                    newSimpleAggregatedMemoryContext()),
            "untracked",
            MemoryAmount.ZERO);

    static @Move AllocatedMemory allocate(GpuTaskMemoryContext memoryContext, String allocationTag, MemoryAmount amount)
    {
        requireNonNull(memoryContext, "memoryContext is null");
        requireNonNull(allocationTag, "allocationTag is null");
        requireNonNull(amount, "amount is null");

        try (var allocation = ClosingRef.own(new AllocatedMemory(memoryContext, allocationTag, MemoryAmount.ZERO))) {
            allocation.borrow().update(amount);
            return allocation.take();
        }
    }

    @Deprecated(forRemoval = true)
    public static AllocatedMemory untracked()
    {
        return UNTRACKED;
    }

    private final GpuTaskMemoryContext memoryContext;
    private String allocationTag;
    private MemoryAmount amount;

    private boolean closed;

    private AllocatedMemory(GpuTaskMemoryContext memoryContext, String allocationTag, MemoryAmount amount)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.allocationTag = requireNonNull(allocationTag, "allocationTag is null");
        this.amount = requireNonNull(amount, "amount is null");
    }

    @Override
    public void update(MemoryAmount newAmount)
    {
        requireNonNull(newAmount, "newAmount is null");
        checkState(!closed, "Already closed");
        checkState(this != UNTRACKED, "Cannot update untracked allocation");

        updateBytes(memoryContext.taskUserMemory(), allocationTag, newAmount.heapBytes() - amount.heapBytes());
        amount = new MemoryAmount(newAmount.heapBytes(), amount.gpuDeviceBytes(), amount.offHeapBytes());

        updateBytes(memoryContext.taskGpuDeviceMemory(), allocationTag, newAmount.gpuDeviceBytes() - amount.gpuDeviceBytes());
        amount = new MemoryAmount(amount.heapBytes(), newAmount.gpuDeviceBytes(), amount.offHeapBytes());

        updateBytes(memoryContext.taskOffHeapMemory(), allocationTag, newAmount.offHeapBytes() - amount.offHeapBytes());
        amount = new MemoryAmount(amount.heapBytes(), amount.gpuDeviceBytes(), newAmount.offHeapBytes());
    }

    public void transferFrom(@Borrow AllocatedMemory other)
    {
        transferFrom(other, other.amount);
    }

    public void transferFrom(@Borrow AllocatedMemory other, MemoryAmount amountToTransfer)
    {
        checkState(!closed, "Already closed");
        checkArgument(this != other, "Cannot transfer from self");
        checkState(this != UNTRACKED, "Cannot update untracked allocation");
        if (other == UNTRACKED) {
            return;
        }
        checkArgument(this.memoryContext == other.memoryContext, "Cannot transfer between memory contexts: %s != %s", this.memoryContext, other.memoryContext);
        other.transferTags(allocationTag, amountToTransfer);
        MemoryAmount newOtherAmount = other.amount.subtract(amountToTransfer);
        amount = amount.add(amountToTransfer);
        other.amount = newOtherAmount;
    }

    public void retag(String newAllocationTag)
    {
        transferTags(newAllocationTag, amount);
        if (this == UNTRACKED) {
            return;
        }
        this.allocationTag = newAllocationTag;
    }

    private void transferTags(String newAllocationTag, MemoryAmount amountToTransfer)
    {
        requireNonNull(newAllocationTag, "newAllocationTag is null");
        checkState(!closed, "Already closed");
        if (this == UNTRACKED) {
            return;
        }

        // Must not fail
        transferTags(memoryContext.taskUserMemory(), allocationTag, newAllocationTag, amountToTransfer.heapBytes());
        transferTags(memoryContext.taskGpuDeviceMemory(), allocationTag, newAllocationTag, amountToTransfer.gpuDeviceBytes());
        transferTags(memoryContext.taskOffHeapMemory(), allocationTag, newAllocationTag, amountToTransfer.offHeapBytes());
    }

    public MemoryAmount amount()
    {
        return amount;
    }

    @Override
    public void close()
    {
        if (this == UNTRACKED) {
            return;
        }
        if (closed) {
            return;
        }
        closed = true;

        updateBytes(memoryContext.taskUserMemory(), allocationTag, -amount.heapBytes());
        updateBytes(memoryContext.taskGpuDeviceMemory(), allocationTag, -amount.gpuDeviceBytes());
        updateBytes(memoryContext.taskOffHeapMemory(), allocationTag, -amount.offHeapBytes());
        amount = MemoryAmount.ZERO;
    }

    private static void updateBytes(AggregatedMemoryContext memoryContext, String allocationTag, long delta)
    {
        requireNonNull(allocationTag, "allocationTag is null");
        if (delta == 0) {
            // Do not call synchronized method unnecessarily
            return;
        }
        memoryContext.updateBytes(allocationTag, delta);
    }

    private static void transferTags(AggregatedMemoryContext memoryContext, String fromTag, String toTag, long delta)
    {
        requireNonNull(fromTag, "fromTag is null");
        requireNonNull(toTag, "toTag is null");
        if (fromTag.equals(toTag) || delta == 0) {
            // Do not call synchronized method unnecessarily
            return;
        }
        memoryContext.transferTags(fromTag, toTag, delta);
    }
}
