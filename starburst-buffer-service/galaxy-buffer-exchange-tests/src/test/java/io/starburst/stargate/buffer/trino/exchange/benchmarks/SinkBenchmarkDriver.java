/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.benchmarks;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManagerFactory;
import io.trino.exchange.ExchangeContextInstance;
import io.trino.exchange.ExchangeManagerContextInstance;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.allAsList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SinkBenchmarkDriver
{
    private static final Logger log = Logger.get(SinkBenchmarkDriver.class);

    private final SinkBenchmarkSetup setup;
    private final ListeningExecutorService executorService;

    public SinkBenchmarkDriver(SinkBenchmarkSetup setup, ListeningExecutorService executorService)
    {
        this.setup = requireNonNull(setup, "setup is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    public SinkBenchmarkResult benchmark()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            TestingBufferService bufferService = TestingBufferService.builder()
                    .setDataServersCount(1)
                    .withDiscoveryServerBuilder(builder ->
                            builder.setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                    .withDataServerBuilder(builder ->
                            builder.setConfigProperty("testing.drop-uploaded-pages", "true"))
                    .build();
            closer.register(bufferService);

            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .put("exchange.buffer-discovery.uri", bufferService.getDiscoveryServer().getBaseUri().toString())
                    .put("exchange.sink-blocked-memory-low", setup.sinkBlockedLow().toString())
                    .put("exchange.sink-blocked-memory-high", setup.sinkBlockedHigh().toString())
                    .buildOrThrow();

            ExchangeManager exchangeManager = BufferExchangeManagerFactory.forRealBufferService().create(
                    config,
                    new ExchangeManagerContextInstance(OpenTelemetry.noop(), Tracing.noopTracer()));

            ExchangeContext exchangeContext = new ExchangeContextInstance(new QueryId("dummy"), new ExchangeId("dummy"), Span.getInvalid());
            Exchange exchange = exchangeManager.createExchange(exchangeContext, setup.outputPartitionsCount(), false);
            ExchangeSinkHandle sinkHandle = exchange.addSink(0);
            ExchangeSinkInstanceHandle sinkeInstanceHandle = exchange.instantiateSink(sinkHandle, 0).get();
            ExchangeSink sink = exchangeManager.createSink(sinkeInstanceHandle);

            AtomicLong dataCounter = new AtomicLong();
            List<SinkWriter> writers = new ArrayList<>();
            for (int writerId = 0; writerId < setup.concurrency(); ++writerId) {
                writers.add(new SinkWriter(sink, writerId, setup.pageSize(), setup.dataSizePerWriter(), setup.outputPartitionsCount(), dataCounter));
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            List<? extends ListenableFuture<?>> futures = writers.stream()
                    .map(executorService::submit)
                    .collect(Collectors.toList());
            ListenableFuture<List<Object>> allWritersFuture = allAsList(futures);

            CompletableFuture<Void> finishFuture = null;
            do {
                try {
                    if (finishFuture != null) {
                        // finishing
                        verify(allWritersFuture.isDone());
                        finishFuture.get(1, TimeUnit.SECONDS);
                    }
                    else {
                        allWritersFuture.get(1, TimeUnit.SECONDS);
                        // all writers done - transition to finishing
                        verify(allWritersFuture.isDone());
                        finishFuture = sink.finish();
                    }
                }
                catch (TimeoutException e) {
                    // ignored
                }
                Duration elapsed = stopwatch.elapsed();
                long written = dataCounter.get();
                log.info("wrote %s in %.3f; throughput %s/s", DataSize.succinctBytes(written), elapsed.toMillis() / 1000.0, DataSize.succinctBytes((long) (written * 1000.0 / elapsed.toMillis())));
            }
            while (finishFuture == null || !finishFuture.isDone());

            Duration elapsed = stopwatch.elapsed();
            long written = dataCounter.get();
            SinkBenchmarkResult result = new SinkBenchmarkResult(
                    DataSize.succinctBytes(dataCounter.get()),
                    Duration.of(stopwatch.elapsed().toMillis(), ChronoUnit.MILLIS),
                    DataSize.succinctBytes((long) (written * 1000.0 / elapsed.toMillis())));

            exchange.close();
            exchangeManager.shutdown();
            return result;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Slice buildDataPage(DataSize pageSize)
    {
        final Slice page;
        page = Slices.allocate(toIntExact(pageSize.toBytes()));
        SliceOutput output = page.getOutput();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < pageSize.toBytes(); ++i) {
            output.writeByte((byte) random.nextInt());
        }
        try {
            output.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return page;
    }

    private static class SinkWriter
            implements Runnable
    {
        private final ExchangeSink sink;
        private final int writerId;
        private long dataSizeToWrite;
        private final Stopwatch stopwatch;
        private final int outputPartitionsCount;
        private final AtomicLong dataCounter;

        private final Slice page;

        public SinkWriter(ExchangeSink sink, int writerId, DataSize pageSize, DataSize dataSizePerWriter, int outputPartitionsCount, AtomicLong dataCounter)
        {
            this.sink = requireNonNull(sink, "sink is null");
            this.writerId = writerId;
            this.dataSizeToWrite = requireNonNull(dataSizePerWriter, "dataSizePerWriter is null").toBytes();
            this.stopwatch = Stopwatch.createUnstarted();
            this.outputPartitionsCount = outputPartitionsCount;
            this.dataCounter = requireNonNull(dataCounter, "dataCounter is null");
            requireNonNull(pageSize, "pageSize is null");
            this.page = buildDataPage(pageSize);
        }

        @Override
        public void run()
        {
            try {
                int partition = 0;
                while (dataSizeToWrite > 0) {
                    CompletableFuture<Void> blocked = sink.isBlocked();
                    if (!blocked.isDone()) {
                        blocked.get();
                        continue;
                    }
                    sink.add(partition, page);
                    dataSizeToWrite -= page.length();
                    dataCounter.addAndGet(page.length());
                    partition = (partition + 1) % outputPartitionsCount;
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
