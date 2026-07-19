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
import io.trino.operator.gpu.GpuOperation;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuLocalExchangeReader
{
    @Test
    public void emptyBufferReportsBlocked()
    {
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> {});
        GpuLocalExchangeReader reader = new GpuLocalExchangeReader(buffer);

        GpuOperation.Result first = reader.execute();
        assertThat(first).isInstanceOf(GpuOperation.Blocked.class);
    }

    @Test
    public void readerEmitsQueuedPageThenFinish()
    {
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> {});
        GpuLocalExchangeReader reader = new GpuLocalExchangeReader(buffer);

        buffer.add(new GpuPage(0, new Column[0]), 64);
        GpuOperation.Result data = reader.execute();
        assertThat(data).isInstanceOf(GpuOperation.Data.class);
        GpuOperation.Data dataResult = (GpuOperation.Data) data;
        dataResult.page().close();
        dataResult.memory().close();

        buffer.finish();
        assertThat(reader.execute()).isInstanceOf(GpuOperation.Finished.class);
    }

    @Test
    public void closeFinishesBufferAndDropsQueuedPages()
    {
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        AtomicInteger onFinishCalls = new AtomicInteger();
        GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> onFinishCalls.incrementAndGet());
        GpuLocalExchangeReader reader = new GpuLocalExchangeReader(buffer);

        buffer.add(new GpuPage(0, new Column[0]), 256);
        assertThat(memory.getBufferedBytes()).isEqualTo(256);

        reader.close();

        assertThat(buffer.isFinished()).isTrue();
        assertThat(memory.getBufferedBytes()).isZero();
        assertThat(onFinishCalls.get()).isGreaterThanOrEqualTo(1);
    }
}
