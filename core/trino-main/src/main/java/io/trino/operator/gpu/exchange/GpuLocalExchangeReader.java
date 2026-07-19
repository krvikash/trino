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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.metadata.Split;
import io.trino.operator.gpu.GpuSourceOperation;
import io.trino.operator.gpu.exchange.GpuLocalExchangeBuffer.BufferedPage;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static java.util.Objects.requireNonNull;

/**
 * Reader half of a {@link GpuLocalExchange}. The no-op control plane lets it slot into a regular
 * {@link io.trino.operator.gpu.GpuOperator.Factory} as the GPU-resident source operation.
 */
final class GpuLocalExchangeReader
        implements GpuSourceOperation
{
    private final GpuLocalExchangeBuffer buffer;

    GpuLocalExchangeReader(GpuLocalExchangeBuffer buffer)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
    }

    @Override
    public @Move Result execute()
    {
        @Own BufferedPage buffered = buffer.removePage();
        if (buffered != null) {
            try (ClosingRef<AllocatedMemory> bufferedAllocation = ClosingRef.own(buffered.memory());
                    ClosingRef<GpuPage> bufferedPage = ClosingRef.own(buffered.page())) {
                bufferedAllocation.borrow().retag(getClass().getSimpleName());
                return new Data(bufferedAllocation.take(), bufferedPage.take());
            }
        }
        if (buffer.isFinished()) {
            return new Finished();
        }
        ListenableFuture<Void> waitForReading = buffer.waitForReading();
        return waitForReading.isDone() ? new Yielded() : new Blocked(waitForReading);
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException("GpuLocalExchangeReader pulls from the exchange buffer, not from addInput");
    }

    @Override
    public void noMoreInput() {}

    @Override
    public void setSplit(Split split)
    {
        throw new UnsupportedOperationException("GpuLocalExchangeReader does not accept splits");
    }

    @Override
    public void close()
    {
        // Source-side termination (LIMIT, cancellation) reaches here when the driver closes the
        // upstream operators; closing the buffer fires its onFinish so checkAllSourcesFinished
        // can stop the sinks, and drops any GpuPages still queued.
        buffer.close();
    }
}
