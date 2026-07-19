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

import ai.rapids.cudf.Cuda;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class GpuHashPartitioningExchanger
        implements GpuExchanger
{
    private final List<GpuLocalExchangeBuffer> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final int[] keyChannels;
    private final GpuTaskMemoryContext taskMemoryContext;

    GpuHashPartitioningExchanger(
            List<GpuLocalExchangeBuffer> buffers,
            LocalExchangeMemoryManager memoryManager,
            int[] keyChannels,
            GpuOperation.Context context)
    {
        this.buffers = ImmutableList.copyOf(buffers);
        checkArgument(!this.buffers.isEmpty(), "buffers is empty");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.keyChannels = keyChannels.clone();
        this.taskMemoryContext = context.taskMemoryContext();
    }

    @Override
    public void accept(@Move AllocatedMemory memory, @Move GpuPage page)
    {
        try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
                memory;
                page) {
            if (page.positionCount() == 0) {
                // No rows to partition; zero-row partitions are dropped by the loop below anyway.
                // Skipping here avoids the cuDF contiguousSplit call and its cross-thread sync.
                return;
            }

            int partitionCount = buffers.size();

            allocation.borrow().transferFrom(memory);
            if (partitionCount > 1) {
                // Peak during partition: hashPartition output (~input) and contiguousSplit output (~input) coexist with the input
                allocation.borrow().update(allocation.borrow().amount().add(MemoryAmount.gpuDevice(2 * page.retainedDeviceMemoryBytes())));
            }

            @Own GpuPage[] partitions = GpuPartitioner.partition(page, keyChannels, partitionCount);
            try {
                // Cross-thread handoff to readers; cudaStreamSynchronize drains this thread's PTDS
                // completely so all GPU writes are committed to device memory before another thread's
                // PTDS reads from the page. Mirrors GpuPassthroughExchanger.accept.
                Cuda.DEFAULT_STREAM.sync();
                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                    GpuPage partition = partitions[partitionIndex];
                    if (partition.positionCount() == 0) {
                        // Mirrors host PartitioningExchanger: zero-row partitions don't reach a buffer.
                        partition.close();
                        partitions[partitionIndex] = null;
                        continue;
                    }
                    try (ClosingRef<AllocatedMemory> partitionMemory = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO))) {
                        // The pre-partition reservation may underestimate total partition memory,
                        // so transfer as much as available and reconcile to the actual retained amount.
                        MemoryAmount toTransfer = MemoryAmount.min(allocation.borrow().amount(), partition.retainedMemory());
                        partitionMemory.borrow().transferFrom(allocation.borrow(), toTransfer);
                        partitionMemory.borrow().update(partition.retainedMemory());

                        GpuLocalExchangeBuffer buffer = buffers.get(partitionIndex);
                        partitions[partitionIndex] = null;
                        buffer.add(partitionMemory.take(), partition, partition.retainedDeviceMemoryBytes());
                    }
                }
            }
            finally {
                for (GpuPage partition : partitions) {
                    if (partition != null) {
                        partition.close();
                    }
                }
            }
        }
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
