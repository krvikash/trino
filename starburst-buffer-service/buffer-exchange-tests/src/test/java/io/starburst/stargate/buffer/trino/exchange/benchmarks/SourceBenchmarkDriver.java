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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManagerFactory;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeSourceHandle;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.getDone;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SourceBenchmarkDriver
{
    private static final Logger log = Logger.get(SourceBenchmarkDriver.class);

    private final SourceBenchmarkSetup setup;
    private final ListeningExecutorService executorService;

    public SourceBenchmarkDriver(SourceBenchmarkSetup setup, ListeningExecutorService executorService)
    {
        this.setup = requireNonNull(setup, "setup is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    public SourceBenchmarkResult benchmark()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            // use bing chunk size to be sure DataServer will allocate just one chunk for all the pages we pass it
            DataSize chunkMaxSize = DataSize.ofBytes(max(DataSize.of(16, MEGABYTE).toBytes(), 2 * setup.chunkSize().toBytes()));
            TestingBufferService bufferService = TestingBufferService.builder()
                    .setDataServersCount(1)
                    .withDiscoveryServerBuilder(builder ->
                            builder.setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                    .withDataServerBuilder(builder ->
                            builder.setConfigProperty("chunk.max-size", chunkMaxSize.toString()))
                    .build();
            closer.register(bufferService);

            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .put("exchange.buffer-discovery.uri", bufferService.getDiscoveryServer().getBaseUri().toString())
                    .put("exchange.source-blocked-memory-low", setup.sourceBlockedLow().toString())
                    .put("exchange.source-blocked-memory-high", setup.sourceBlockedHigh().toString())
                    .put("exchange.source-parallelism", String.valueOf(setup.parallelism()))
                    .buildOrThrow();
            ExchangeManager exchangeManager = BufferExchangeManagerFactory.forRealBufferService().create(config);
            closer.register(exchangeManager::shutdown);

            ExchangeId exchangeId = new ExchangeId("dummy");
            ExchangeContext exchangeContext = new ExchangeContext(new QueryId("dummy"), exchangeId);
            Exchange exchange = exchangeManager.createExchange(exchangeContext, 1, false);
            closer.register(exchange);
            ExchangeSinkHandle sinkHandle = exchange.addSink(0);
            exchange.noMoreSinks();
            ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle, 0);
            ExchangeSink sink = exchangeManager.createSink(sinkInstanceHandle);
            Slice dataPage = buildDataPage(setup.pageSize());

            long dataWritten = 0;
            while (dataWritten < setup.chunkSize().toBytes()) {
                sink.add(0, dataPage);
                dataWritten += dataPage.length();
            }
            sink.finish().get();

            exchange.allRequiredSinksFinished();

            ExchangeSourceHandleSource sourceHandlesSource = exchange.getSourceHandles();
            ExchangeSourceHandleSource.ExchangeSourceHandleBatch sourceHandlesBatch = sourceHandlesSource.getNextBatch().get();
            checkState(sourceHandlesBatch.lastBatch(), "expected only single source handles batch");
            checkState(sourceHandlesBatch.handles().size() == 1, "expected only single source handle; got" + sourceHandlesBatch.handles());
            BufferExchangeSourceHandle receivedSourceHandle = (BufferExchangeSourceHandle) sourceHandlesBatch.handles().get(0);
            sourceHandlesSource.close();
            checkState(receivedSourceHandle.getChunksCount() == 1, "expected single chunk in source handle; got " + receivedSourceHandle.getChunksCount());
            checkState(receivedSourceHandle.getPartitionId() == 0, "expected partition 0");

            ExchangeSource source = exchangeManager.createSource();
            AtomicLong dataCounter = new AtomicLong();
            List<SourceReader> readers = new ArrayList<>();
            for (int readerId = 0; readerId < setup.readersCount(); ++readerId) {
                readers.add(new SourceReader(source, readerId, dataCounter));
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            List<? extends ListenableFuture<Long>> futures = readers.stream()
                    .map(executorService::submit)
                    .collect(Collectors.toList());

            // create synthetic source handle based on received one to be able to put multiple chunks in single source handle
            BufferExchangeSourceHandle sourceHandle = duplicatedChunkSourceHandle(receivedSourceHandle, setup.chunksPerSourceHandle());
            int sourceHandlesCount = (int) (((double) setup.totalDataSize().toBytes()) / setup.chunkSize().toBytes() / setup.chunksPerSourceHandle());
            for (int i = 0; i < sourceHandlesCount; ++i) {
                source.addSourceHandles(ImmutableList.of(sourceHandle));
            }
            ExchangeSourceOutputSelector outputSelector = ExchangeSourceOutputSelector.builder(ImmutableSet.of(exchangeId))
                    .include(exchangeId, 0, 0)
                    .setPartitionCount(exchangeId, 1)
                    .setFinal()
                    .build();
            source.setOutputSelector(outputSelector);
            source.noMoreSourceHandles();

            // wait for readers
            ListenableFuture<List<Long>> allFuture = allAsList(futures);
            do {
                try {
                    allFuture.get(1, TimeUnit.SECONDS);
                }
                catch (TimeoutException e) {
                    // ignored
                }
                Duration elapsed = stopwatch.elapsed();
                long read = dataCounter.get();
                log.info("read %s in %.3f; throughput %s/s", DataSize.succinctBytes(read), elapsed.toMillis() / 1000.0, DataSize.succinctBytes((long) (read * 1000.0 / elapsed.toMillis())));
            }
            while (!allFuture.isDone());

            Duration elapsed = stopwatch.elapsed();
            long bytesRead = dataCounter.get();

            // get data from readers to make sure we are not optimized out
            long dummyHash = 0;
            for (Long dummyHashComponent : getDone(allFuture)) {
                dummyHash = dummyHashComponent + dummyHash;
            }

            SourceBenchmarkResult result = new SourceBenchmarkResult(
                    DataSize.succinctBytes(dataCounter.get()),
                    Duration.of(stopwatch.elapsed().toMillis(), ChronoUnit.MILLIS),
                    DataSize.succinctBytes((long) (bytesRead * 1000.0 / elapsed.toMillis())),
                    dummyHash);

            return result;
        }
        catch (Exception e) {
            log.error(e, "benchmark failure");
            throw new RuntimeException(e);
        }
    }

    private BufferExchangeSourceHandle duplicatedChunkSourceHandle(BufferExchangeSourceHandle receivedSourceHandle, int chunksPerSourceHandle)
    {
        verify(receivedSourceHandle.getChunksCount() == 1);
        if (chunksPerSourceHandle == 1) {
            return receivedSourceHandle;
        }

        long[] bufferIds = new long[chunksPerSourceHandle];
        long[] chunkIds = new long[chunksPerSourceHandle];

        Arrays.fill(bufferIds, receivedSourceHandle.getBufferNodeId(0));
        Arrays.fill(chunkIds, receivedSourceHandle.getChunkId(0));

        return new BufferExchangeSourceHandle(
                receivedSourceHandle.getExternalExchangeId(),
                receivedSourceHandle.getPartitionId(),
                bufferIds,
                chunkIds,
                receivedSourceHandle.getDataSizeInBytes() * chunksPerSourceHandle,
                receivedSourceHandle.isPreserveOrderWithinPartition());
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

    private static class SourceReader
            implements Callable<Long>
    {
        private final ExchangeSource source;
        private final int readerId;
        private final AtomicLong dataCounter;

        public SourceReader(ExchangeSource source, int readerId, AtomicLong dataCounter)
        {
            this.source = requireNonNull(source, "source is null");
            this.readerId = readerId;
            this.dataCounter = requireNonNull(dataCounter, "dataCounter is null");
        }

        @Override
        public Long call()
        {
            long dummyHash = 0;
            try {
                while (!source.isFinished()) {
                    CompletableFuture<Void> blocked = source.isBlocked();
                    if (!blocked.isDone()) {
                        blocked.get();
                        continue;
                    }
                    Slice readSlice = source.read();
                    if (readSlice == null) {
                        continue;
                    }
                    dataCounter.addAndGet(readSlice.length());
                    BasicSliceInput in = readSlice.getInput();
                    // read data so we are not optimized out
                    while (in.isReadable()) {
                        int someByte = in.read();
                        dummyHash = dummyHash + someByte % 13;
                    }
                }
            }
            catch (Exception e) {
                log.error(e, "Reader %s failed", readerId);
                throw new RuntimeException(e);
            }
            log.info("Reader %s finished", readerId);
            return dummyHash;
        }
    }
}
