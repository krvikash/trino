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
import io.trino.operator.ReferenceCount;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.exchange.GpuLocalExchange.GpuLocalExchangeSink;
import io.trino.operator.gpu.exchange.GpuLocalExchange.GpuLocalExchangeSinkFactory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

/**
 * Terminal {@link GpuOperation} that drains its upstream into a {@link GpuLocalExchangeSink}.
 * Never returns {@link Data}; the trailing {@code CopyToBlocks} added by
 * {@link io.trino.operator.gpu.GpuOperator.Factory} becomes a free pass-through.
 */
public final class GpuLocalExchangeWriter
        implements GpuOperation
{
    /**
     * {@link GpuOperation.Factory} that owns a {@link GpuLocalExchangeSinkFactory} and a
     * {@link ReferenceCount} shared with siblings produced via {@link #duplicate()}. The original
     * factory installs a listener that fires {@link GpuLocalExchangeSinkFactory#noMoreSinkFactories()}
     * once every sibling has reported {@link #noMoreOperators()} — same pattern as the host
     * {@code LocalExchangeSinkOperatorFactory}.
     */
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuLocalExchangeSinkFactory sinkFactory;
        private final ReferenceCount factoryReferenceCount;

        public Factory(GpuLocalExchangeSinkFactory sinkFactory)
        {
            this(sinkFactory, new ReferenceCount(1));
            factoryReferenceCount.getFreeFuture().addListener(sinkFactory::noMoreSinkFactories, directExecutor());
        }

        private Factory(GpuLocalExchangeSinkFactory sinkFactory, ReferenceCount factoryReferenceCount)
        {
            this.sinkFactory = requireNonNull(sinkFactory, "sinkFactory is null");
            this.factoryReferenceCount = requireNonNull(factoryReferenceCount, "factoryReferenceCount is null");
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuLocalExchangeWriter(source, sinkFactory.createSink());
        }

        @Override
        public Factory duplicate()
        {
            factoryReferenceCount.retain();
            return new Factory(sinkFactory.duplicate(), factoryReferenceCount);
        }

        @Override
        public void noMoreOperators()
        {
            sinkFactory.close();
            factoryReferenceCount.release();
        }
    }

    private final GpuOperation source;
    private final GpuLocalExchangeSink sink;
    private final GpuExchanger exchanger;

    GpuLocalExchangeWriter(GpuOperation source, GpuLocalExchangeSink sink)
    {
        this.source = requireNonNull(source, "source is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.exchanger = sink.exchanger();
    }

    @Override
    public @Move Result execute()
    {
        // Source-side reader's close finished the sink; stop pulling pages the buffer would drop.
        if (sink.isFinished().isDone()) {
            return new Finished();
        }

        ListenableFuture<Void> writeBlocked = exchanger.waitForWriting();
        if (!writeBlocked.isDone()) {
            return new Blocked(writeBlocked);
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Data(GpuPage page) -> {
                try (page) {
                    exchanger.accept(page);
                }
                yield new Yielded();
            }
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Finished finished -> {
                sink.finish();
                yield finished;
            }
        };
    }

    @Override
    public void close()
    {
        try {
            sink.finish();
        }
        finally {
            source.close();
        }
    }
}
