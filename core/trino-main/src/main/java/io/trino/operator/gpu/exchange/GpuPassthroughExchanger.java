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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;

import static java.util.Objects.requireNonNull;

final class GpuPassthroughExchanger
        implements GpuExchanger
{
    private final GpuLocalExchangeBuffer buffer;
    private final LocalExchangeMemoryManager memoryManager;

    GpuPassthroughExchanger(GpuLocalExchangeBuffer buffer, LocalExchangeMemoryManager memoryManager)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
    }

    @Override
    public void accept(@Move AllocatedMemory memory, @Move GpuPage page)
    {
        try (ClosingRef<AllocatedMemory> allocated = ClosingRef.own(memory);
                ClosingRef<GpuPage> ownedPage = ClosingRef.own(page)) {
            allocated.borrow().retag(getClass().getSimpleName());
            // Cross-thread handoff to readers; cudaStreamSynchronize drains this thread's PTDS
            // completely so all GPU writes are committed to device memory before another thread's
            // PTDS reads from the page.
            Cuda.DEFAULT_STREAM.sync();
            buffer.add(allocated.take(), ownedPage.take(), page.retainedDeviceMemoryBytes());
        }
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
