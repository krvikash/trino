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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuSourceOperation;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.sql.planner.PartitioningHandle;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

/**
 * GPU-resident analog of {@link io.trino.operator.exchange.LocalExchange}: per-partition queues
 * of {@link io.trino.spi.gpu.GpuPage} for gather, hash, and round-robin partitioning.
 */
@ThreadSafe
public class GpuLocalExchange
{
    private final Function<GpuOperation.Context, GpuExchanger> exchangerSupplier;
    private final List<GpuLocalExchangeBuffer> buffers;

    // Writes guarded by `this`; volatile so the early-out in checkAllSourcesFinished avoids
    // re-walking every buffer once the cleanup has fired.
    private volatile boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final Set<GpuLocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<GpuLocalExchangeSink> sinks = new HashSet<>();

    @GuardedBy("this")
    private int nextSourceIndex;

    public GpuLocalExchange(
            PartitioningHandle partitioning,
            int defaultConcurrency,
            int[] partitionKeyChannels,
            long maxBufferedDeviceBytes)
    {
        requireNonNull(partitioning, "partitioning is null");
        requireNonNull(partitionKeyChannels, "partitionKeyChannels is null");

        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            checkArgument(partitionKeyChannels.length == 0, "Gather exchange must not have partition channels");
            LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedDeviceBytes);
            buffers = IntStream.range(0, 1)
                    .mapToObj(_ -> new GpuLocalExchangeBuffer(memoryManager, _ -> checkAllSourcesFinished()))
                    .collect(toImmutableList());
            exchangerSupplier = _ -> new GpuPassthroughExchanger(buffers.get(0), memoryManager);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            int bufferCount = defaultConcurrency;
            int[] keyChannels = partitionKeyChannels.clone();
            LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedDeviceBytes);
            buffers = IntStream.range(0, bufferCount)
                    .mapToObj(_ -> new GpuLocalExchangeBuffer(memoryManager, _ -> checkAllSourcesFinished()))
                    .collect(toImmutableList());
            exchangerSupplier = context -> new GpuHashPartitioningExchanger(buffers, memoryManager, keyChannels, context);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            checkArgument(partitionKeyChannels.length == 0, "Round-robin exchange must not have partition channels");
            int bufferCount = defaultConcurrency;
            LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedDeviceBytes);
            buffers = IntStream.range(0, bufferCount)
                    .mapToObj(_ -> new GpuLocalExchangeBuffer(memoryManager, _ -> checkAllSourcesFinished()))
                    .collect(toImmutableList());
            exchangerSupplier = _ -> new GpuRoundRobinExchanger(buffers, memoryManager);
        }
        else {
            throw new IllegalArgumentException("Unsupported partitioning for GpuLocalExchange: " + partitioning);
        }
    }

    public int getBufferCount()
    {
        return buffers.size();
    }

    public synchronized GpuLocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        GpuLocalExchangeSinkFactory newFactory = new GpuLocalExchangeSinkFactory(this);
        openSinkFactories.add(newFactory);
        return newFactory;
    }

    public synchronized GpuLocalExchangeBuffer getNextSource()
    {
        checkState(nextSourceIndex < buffers.size(), "All operators already created");
        GpuLocalExchangeBuffer result = buffers.get(nextSourceIndex);
        nextSourceIndex++;
        return result;
    }

    /**
     * Source factory for plugging this exchange into a {@link io.trino.operator.gpu.GpuOperator.Factory}.
     * Each {@code create()} claims one of the per-partition buffers via {@link #getNextSource()}
     * and returns a reader with a no-op control plane.
     */
    public GpuSourceOperation.Factory readerSourceFactory()
    {
        return () -> new GpuLocalExchangeReader(getNextSource());
    }

    private void checkAllSourcesFinished()
    {
        assertNotHoldsLock(this);

        if (allSourcesFinished) {
            return;
        }
        if (!buffers.stream().allMatch(GpuLocalExchangeBuffer::isFinished)) {
            return;
        }

        List<GpuLocalExchangeSink> openSinks;
        synchronized (this) {
            if (allSourcesFinished) {
                return;
            }
            allSourcesFinished = true;
            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // Limit queries can drain the source side before the sink side is done; finish the sinks
        // so the upstream writers stop producing.
        openSinks.forEach(GpuLocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private GpuLocalExchangeSink createSink(GpuLocalExchangeSinkFactory factory, GpuOperation.Context context)
    {
        assertNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                return GpuLocalExchangeSink.finishedSink();
            }

            // Exchanger may be stateful (hash partitioner caches per-call buffers), so each sink
            // gets its own.
            GpuExchanger exchanger = exchangerSupplier.apply(context);
            GpuLocalExchangeSink sink = new GpuLocalExchangeSink(exchanger, this::sinkFinished);
            sinks.add(sink);
            return sink;
        }
    }

    private void sinkFinished(GpuLocalExchangeSink sink)
    {
        assertNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    private void noMoreSinkFactories()
    {
        assertNotHoldsLock(this);

        synchronized (this) {
            noMoreSinkFactories = true;
        }
        checkAllSinksComplete();
    }

    private void sinkFactoryClosed(GpuLocalExchangeSinkFactory sinkFactory)
    {
        assertNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        assertNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        buffers.forEach(GpuLocalExchangeBuffer::finish);
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private static void assertNotHoldsLock(Object lock)
    {
        assert !Thread.holdsLock(lock) : "Cannot execute this method while holding a lock";
    }

    // Distinct identity per OperatorFactory so duplicate() can produce sibling sink factories
    // that close independently. The Set<GpuLocalExchangeSinkFactory> in the enclosing exchange
    // tracks all open siblings; the exchange's buffers finish only once every sibling and every
    // sink has been closed.
    @ThreadSafe
    public static final class GpuLocalExchangeSinkFactory
            implements Closeable
    {
        private final GpuLocalExchange exchange;

        private GpuLocalExchangeSinkFactory(GpuLocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public GpuLocalExchangeSink createSink(GpuOperation.Context context)
        {
            return exchange.createSink(this, context);
        }

        public GpuLocalExchangeSinkFactory duplicate()
        {
            return exchange.createSinkFactory();
        }

        public void noMoreSinkFactories()
        {
            exchange.noMoreSinkFactories();
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }
    }

    public static final class GpuLocalExchangeSink
    {
        private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

        static GpuLocalExchangeSink finishedSink()
        {
            GpuLocalExchangeSink sink = new GpuLocalExchangeSink(GpuExchanger.FINISHED, _ -> {});
            sink.finish();
            return sink;
        }

        private final GpuExchanger exchanger;
        private final Consumer<GpuLocalExchangeSink> onFinish;
        private final SettableFuture<Void> finished = SettableFuture.create();

        private GpuLocalExchangeSink(
                GpuExchanger exchanger,
                Consumer<GpuLocalExchangeSink> onFinish)
        {
            this.exchanger = requireNonNull(exchanger, "exchanger is null");
            this.onFinish = requireNonNull(onFinish, "onFinish is null");
        }

        GpuExchanger exchanger()
        {
            return exchanger;
        }

        public void addPage(@Move AllocatedMemory memory, @Move GpuPage page)
        {
            requireNonNull(page, "page is null");

            if (isFinished().isDone()) {
                try (var closer = UncheckedCloser.create()) {
                    closer.register(memory);
                    closer.register(page);
                }
                return;
            }

            // The isFinished check can race with finish(); even when it returns false the sink
            // may flip finished before exchanger.consume completes. The underlying
            // GpuLocalExchangeBuffer.addPage drops the page in that case (without charging
            // memory), so the race is benign — no rows are silently buffered after finish.
            exchanger.accept(memory, page);
        }

        public ListenableFuture<Void> waitForWriting()
        {
            if (isFinished().isDone()) {
                return NOT_BLOCKED;
            }
            return exchanger.waitForWriting();
        }

        public void finish()
        {
            if (finished.set(null)) {
                onFinish.accept(this);
            }
        }

        public ListenableFuture<Void> isFinished()
        {
            return finished;
        }
    }
}
