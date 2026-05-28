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

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.gpu.GpuUtils.retainedDeviceBytes;
import static java.util.Objects.requireNonNull;

final class GpuRoundRobinExchanger
        implements GpuExchanger
{
    private final List<GpuLocalExchangeBuffer> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    // Per-sink counter: each call routes a whole page to the next buffer in rotation. Round-robin
    // has no key-based correctness constraint, so a sink-local index is sufficient.
    private int nextBuffer;

    GpuRoundRobinExchanger(List<GpuLocalExchangeBuffer> buffers, LocalExchangeMemoryManager memoryManager)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "buffers is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        checkArgument(!this.buffers.isEmpty(), "buffers is empty");
    }

    @Override
    public void accept(@Borrow GpuPage page)
    {
        // Cross-thread handoff to readers; cudaStreamSynchronize drains this thread's PTDS
        // completely so all GPU writes are committed to device memory before another thread's
        // PTDS reads from the page. Mirrors GpuPassthroughExchanger.accept.
        Cuda.DEFAULT_STREAM.sync();
        int target = nextBuffer;
        nextBuffer = (nextBuffer + 1) % buffers.size();
        buffers.get(target).add(page, retainedDeviceBytes(page));
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
