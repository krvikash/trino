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
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
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

    GpuHashPartitioningExchanger(
            List<GpuLocalExchangeBuffer> buffers,
            LocalExchangeMemoryManager memoryManager,
            int[] keyChannels)
    {
        this.buffers = ImmutableList.copyOf(buffers);
        checkArgument(!this.buffers.isEmpty(), "buffers is empty");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.keyChannels = keyChannels.clone();
    }

    @Override
    public void accept(@Borrow GpuPage page)
    {
        if (page.positionCount() == 0) {
            // No rows to partition; zero-row partitions are dropped by the loop below anyway.
            // Skipping here avoids the cuDF contiguousSplit call and its cross-thread sync.
            return;
        }
        @Own GpuPage[] partitions = GpuPartitioner.partition(page, keyChannels, buffers.size());
        try {
            // Cross-thread handoff to readers; cudaStreamSynchronize drains this thread's PTDS
            // completely so all GPU writes are committed to device memory before another thread's
            // PTDS reads from the page. Mirrors GpuPassthroughExchanger.accept.
            Cuda.DEFAULT_STREAM.sync();
            for (int partitionIndex = 0; partitionIndex < buffers.size(); partitionIndex++) {
                @Borrow GpuPage partition = partitions[partitionIndex];
                if (partition.positionCount() == 0) {
                    // Mirrors host PartitioningExchanger: zero-row partitions don't reach a buffer.
                    continue;
                }
                buffers.get(partitionIndex).add(partition, partition.retainedDeviceMemoryBytes());
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

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
