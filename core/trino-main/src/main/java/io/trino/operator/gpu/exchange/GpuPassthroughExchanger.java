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
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;

import static io.trino.operator.gpu.GpuUtils.retainedDeviceBytes;
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
    public void accept(@Borrow GpuPage page)
    {
        // Cross-thread handoff to readers; cudaStreamSynchronize drains this thread's PTDS
        // completely so all GPU writes are committed to device memory before another thread's
        // PTDS reads from the page.
        Cuda.DEFAULT_STREAM.sync();
        buffer.add(page, retainedDeviceBytes(page));
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
