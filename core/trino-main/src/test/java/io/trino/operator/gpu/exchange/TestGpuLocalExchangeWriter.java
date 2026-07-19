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

import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.exchange.GpuLocalExchange.GpuLocalExchangeSink;
import io.trino.operator.gpu.exchange.GpuLocalExchange.GpuLocalExchangeSinkFactory;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;

import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGpuLocalExchangeWriter
{
    private static final long MAX_BUFFER_BYTES = 64L * 1024 * 1024;

    /**
     * Creates a real {@link GpuLocalExchangeSink} using a real {@link GpuLocalExchange}.
     * The sink is fully owned by the caller; no reader is needed for writer tests.
     */
    private static GpuLocalExchangeSink newRealSink()
    {
        GpuLocalExchange exchange = new GpuLocalExchange(SINGLE_DISTRIBUTION, 1, new int[0], MAX_BUFFER_BYTES);
        GpuLocalExchangeSinkFactory factory = exchange.createSinkFactory();
        factory.noMoreSinkFactories();
        GpuLocalExchangeSink sink = factory.createSink(new TestingGpuOperationContext());
        factory.close();
        return sink;
    }

    @Test
    public void deliversDataToExchangerAndReportsYielded()
    {
        GpuLocalExchangeSink sink = newRealSink();
        FakeSource source = new FakeSource();
        source.results.add(new GpuOperation.Data(AllocatedMemory.untracked(), new GpuPage(0, new Column[0])));
        GpuLocalExchangeWriter writer = new GpuLocalExchangeWriter(source, sink);

        GpuOperation.Result result = writer.execute();

        assertThat(result).isInstanceOf(GpuOperation.Yielded.class);
        assertThat(sink.isFinished().isDone()).isFalse();
    }

    @Test
    public void propagatesUpstreamFinishedAndCallsSinkFinish()
    {
        GpuLocalExchangeSink sink = newRealSink();
        FakeSource source = new FakeSource();
        source.results.add(new GpuOperation.Finished());
        GpuLocalExchangeWriter writer = new GpuLocalExchangeWriter(source, sink);

        GpuOperation.Result result = writer.execute();
        assertThat(result).isInstanceOf(GpuOperation.Finished.class);
        assertThat(sink.isFinished().isDone()).as("sink.finish() must be called when upstream reports Finished").isTrue();
    }

    @Test
    public void propagatesExceptionFromSource()
    {
        GpuLocalExchangeSink sink = newRealSink();
        GpuLocalExchangeWriter writer = new GpuLocalExchangeWriter(new ThrowingSource(), sink);

        assertThatThrownBy(writer::execute)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("boom");
    }

    @Test
    public void finishedSinkExchangerIsUnblocked()
    {
        // Confirm that the writer proceeds (not Blocked) when the sink's exchanger is ready.
        // Use the package-visible finishedSink() which wraps GpuExchanger.FINISHED — its
        // waitForWriting() is always NOT_BLOCKED, and Finished source yields Finished result.
        GpuLocalExchangeSink finishedSink = GpuLocalExchangeSink.finishedSink();
        FakeSource source = new FakeSource();
        source.results.add(new GpuOperation.Finished());
        GpuLocalExchangeWriter writer = new GpuLocalExchangeWriter(source, finishedSink);

        // The finished sink's exchanger is NOT_BLOCKED, so execute() is not blocked.
        assertThat(writer.execute()).isInstanceOf(GpuOperation.Finished.class);
    }

    private static class FakeSource
            implements GpuOperation
    {
        final Deque<Result> results = new ArrayDeque<>();

        @Override
        public Result execute()
        {
            Result next = results.pollFirst();
            return next != null ? next : new Yielded();
        }

        @Override
        public void close() {}
    }

    private static class ThrowingSource
            implements GpuOperation
    {
        @Override
        public Result execute()
        {
            throw new RuntimeException("boom");
        }

        @Override
        public void close() {}
    }
}
