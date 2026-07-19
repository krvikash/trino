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

import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.exchange.GpuLocalExchangeBuffer.BufferedPage;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuLocalExchangeBuffer
{
    @Test
    public void enqueueDequeueAccountsBytes()
    {
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1_000);
        try (GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> {})) {
            addPage(buffer, emptyGpuPage(), 200);
            assertThat(memory.getBufferedBytes()).isEqualTo(200);

            BufferedPage buffered = buffer.removePage();
            assertThat(buffered).isNotNull();
            buffered.memory().close();
            buffered.page().close();
            assertThat(memory.getBufferedBytes()).isEqualTo(0);
        }
    }

    @Test
    public void finishUnblocksReader()
    {
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1_000);
        try (GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> {})) {
            assertThat(buffer.waitForReading().isDone()).isFalse();
            buffer.finish();
            assertThat(buffer.waitForReading().isDone()).isTrue();
            assertThat(buffer.isFinished()).isTrue();
        }
    }

    @Test
    public void addAfterFinishClosesPageAndDoesNotCharge()
    {
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1_000);
        try (GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> {})) {
            buffer.finish();
            addPage(buffer, emptyGpuPage(), 200);
            assertThat(memory.getBufferedBytes()).isZero();
        }
    }

    @Test
    public void onFinishCalledWhenDrained()
    {
        AtomicInteger calls = new AtomicInteger();
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1_000);
        try (GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> calls.incrementAndGet())) {
            addPage(buffer, emptyGpuPage(), 50);
            buffer.finish();
            // drain
            BufferedPage buffered = buffer.removePage();
            buffered.memory().close();
            buffered.page().close();
            assertThat(calls.get()).isGreaterThanOrEqualTo(1);
        }
    }

    private static GpuPage emptyGpuPage()
    {
        return new GpuPage(0, new Column[0]);
    }

    private static void addPage(GpuLocalExchangeBuffer buffer, GpuPage page, long bytes)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        AllocatedMemory allocated = context.taskMemoryContext().allocate(TestGpuLocalExchangeBuffer.class.getSimpleName(), page.retainedMemory());
        buffer.add(allocated, page, bytes);
    }
}
