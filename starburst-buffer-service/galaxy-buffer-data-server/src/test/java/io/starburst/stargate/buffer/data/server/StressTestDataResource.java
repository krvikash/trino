/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tracing.SpanSerialization;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.HttpDataClient;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;
import io.starburst.stargate.buffer.data.client.spooling.blackhole.BlackholeSpooledChunkReader;
import io.starburst.stargate.buffer.data.server.testing.TestingDataServer;
import io.starburst.stargate.buffer.data.server.testing.TestingDiscoveryApiModule;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TEN_SECONDS;

public final class StressTestDataResource
{
    private static final Logger log = Logger.get(StressTestDataResource.class);

    private static final long BUFFER_NODE_ID = 0;
    private static final DataSize DATA_SERVER_AVAILABLE_MEMORY = DataSize.of(2, GIGABYTE);

    private static final int MAX_REQUEST_SIZE = toIntExact(DataSize.of(32, MEGABYTE).toBytes());

    private static final byte[] randomBytes;

    public static final int WRITER_THREADS = 10;
    public static final int READER_THREADS = 10;
    public static final int REQUESTS_PER_THREAD = 10_000;
    public static final int MAX_PARTITIONS_PER_REQUEST = 10;

    private StressTestDataResource() {}

    static {
        randomBytes = new byte[toIntExact(DataSize.of(128, MEGABYTE).toBytes())];
        for (int i = 0; i < randomBytes.length; i++) {
            randomBytes[i] = (byte) (i % 128);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        boolean isChunkSpoolMergeEnabled = true;
        try (TestingDataServer dataServer = TestingDataServer.builder()
                .withDiscoveryApiModule(new TestingDiscoveryApiModule())
                .setConfigProperty("spooling.directory", System.getProperty("java.io.tmpdir") + "/spooling-storage-" + UUID.randomUUID())
                .setConfigProperty("chunk.spool-merge-enabled", String.valueOf(isChunkSpoolMergeEnabled))
                .setConfigProperty("discovery-broadcast-interval", "10ms")
                .setConfigProperty("memory.heap-headroom", succinctBytes(Runtime.getRuntime().maxMemory() - DATA_SERVER_AVAILABLE_MEMORY.toBytes()).toString())
                .setConfigProperty("memory.allocation-low-watermark", "0.75")
                .setConfigProperty("memory.allocation-high-watermark", "0.90")
                .setConfigProperty("draining.min-duration", "2s")
                .withBlackholeStorage()
                .build();
                ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool())) {
            {
                System.out.println("is it started yet?");
                try (JettyHttpClient httpClient = getJettyHttpClient()) {
                    var dataClient = getHttpDataClient(dataServer, httpClient);
                    // Wait for Node to become ready
                    await().atMost(TEN_SECONDS).until(
                            () -> dataClient.getInfo().state(),
                            BufferNodeState.ACTIVE::equals);
                }
                System.out.println("it is started now");
            }

            System.out.println("Data server started");

            String exchangeId = "test";

            var httpClient = getJettyHttpClient();
            var dataClient = getHttpDataClient(dataServer, httpClient);
            AtomicLong errorCount = new AtomicLong();

            List<ListenableFuture<?>> futures = new ArrayList<>();
            AtomicLong writersRemaining = new AtomicLong(WRITER_THREADS);
            for (int mutableThreadId = 0; mutableThreadId < WRITER_THREADS; ++mutableThreadId) {
                final int threadId = mutableThreadId;
                futures.add(executor.submit(() -> {
                    log.info("Starting WRITER %s", threadId);
                    try (SetThreadName ignored = new SetThreadName("WRITER-%s", threadId)) {
                        try {
                            ListMultimap<Integer, Slice> data = randomDataPages();
                            System.out.println("first request being sent with size " + new BigDecimal(data.values().stream().mapToInt(Slice::length).sum() / 1024d / 1024) + " mibibytes");
                            for (int requestNo = 0; requestNo < REQUESTS_PER_THREAD; requestNo++) {
                                Optional<RateLimitInfo> response = null;
                                try {
                                    response = dataClient.addDataPages(exchangeId, threadId, 0, requestNo, data)
                                            .get();
                                }
                                catch (ExecutionException e) {
                                    errorCount.incrementAndGet();
                                    log.warn(e, "looks like bum");
                                }
                                response.ifPresent(rateLimitInfo -> {
                                    System.out.println("Rate limit info: " + rateLimitInfo);
                                });
                                if (requestNo == 0) {
                                    System.out.println("first completed");
                                }
                                else if (requestNo % 100 == 0) {
                                    System.out.println("completed " + requestNo + " requests");
                                }
                            }
                        }
                        finally {
                            if (writersRemaining.addAndGet(-1) == 0) {
                                dataClient.finishExchange(exchangeId).get();
                            }
                            log.info("Stopping WRITER %s", threadId);
                        }
                    }
                    return null;
                }));
            }

            Queue<ChunkHandle> chunkQueue = new ConcurrentLinkedQueue<>();
            AtomicBoolean allChunksListed = new AtomicBoolean();

            futures.add(executor.submit(() -> {
                log.info("Starting LISTER");
                OptionalLong pangingId = OptionalLong.empty();
                try (SetThreadName ignored = new SetThreadName("LISTER")) {
                    dataClient.registerExchange(exchangeId, ChunkDeliveryMode.STANDARD, Span.getInvalid()).get();
                    while (true) {
                        ChunkList chunkList = null;
                        try {
                            chunkList = dataClient.listClosedChunks(exchangeId, pangingId).get();
                        }
                        catch (Exception e) {
                            log.error(e, "Error in LISTENER");
                            errorCount.incrementAndGet();
                        }
                        chunkQueue.addAll(chunkList.chunks());
                        pangingId = chunkList.nextPagingId();
                        if (pangingId.isEmpty()) {
                            allChunksListed.set(true);
                            log.info("Stopping LISTER");
                            return;
                        }
                    }
                }
                catch (Throwable e) {
                    log.error(e, "LISTENER DIED");
                }
            }));

            for (int mutableThreadId = 0; mutableThreadId < READER_THREADS; ++mutableThreadId) {
                final int threadId = mutableThreadId;
                futures.add(executor.submit(() -> {
                    log.info("Starting READER %s", threadId);
                    int readChunksCount = 0;
                    int notFoundChunksCount = 0;
                    try (SetThreadName ignored = new SetThreadName("READER-%s", threadId)) {
                        while (true) {
                            if (allChunksListed.get() && chunkQueue.isEmpty()) {
                                return null;
                            }
                            ChunkHandle chunkHandle = chunkQueue.poll();
                            if (chunkHandle != null) {
                                try {
                                    dataClient.getChunkData(chunkHandle.bufferNodeId(), exchangeId, chunkHandle.partitionId(), chunkHandle.chunkId()).get();
                                    readChunksCount++;
                                }
                                catch (Exception e) {
                                    if (e instanceof ExecutionException
                                            && e.getCause() instanceof DataApiException dataApiException
                                            && dataApiException.getErrorCode() == ErrorCode.CHUNK_NOT_FOUND) {
                                        notFoundChunksCount++;
                                        // shit happens - could be spoolled (dropped)
                                    }
                                    else {
                                        errorCount.incrementAndGet();
                                        log.error(e, "error reading");
                                    }
                                }
                                if (readChunksCount % 100 == 0) {
                                    log.info("readChunksCount=%s, notFoundChunksCount=%s", readChunksCount, notFoundChunksCount);
                                }
                            }
                        }
                    }
                    finally {
                        log.info("Stopping READER %s", threadId);
                    }
                }));
            }

            try {
                Futures.successfulAsList(futures).get();
            }
            finally {
                if (errorCount.get() > 0) {
                    log.error("Got %s errors", errorCount.get());
                    System.exit(1);
                }
            }
        }
    }

    private static HttpDataClient getHttpDataClient(TestingDataServer dataServer, JettyHttpClient httpClient)
    {
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(new ObjectMapperProvider()
                .withJsonSerializers(Map.of(Span.class, new SpanSerialization.SpanSerializer(OpenTelemetry.noop()))));
        JsonCodec<Span> spanJsonCodec = jsonCodecFactory.jsonCodec(Span.class);

        return new HttpDataClient(
                dataServer.getBaseUri(),
                BUFFER_NODE_ID,
                httpClient,
                succinctDuration(60, SECONDS),
                new BlackholeSpooledChunkReader(),
                true,
                Optional.empty(),
                spanJsonCodec);
    }

    private static JettyHttpClient getJettyHttpClient()
    {
        return new JettyHttpClient(new HttpClientConfig().setMaxContentLength(DataSize.of(64, MEGABYTE)));
    }

    private static ListMultimap<Integer, Slice> randomDataPages()
    {
        ImmutableListMultimap.Builder<Integer, Slice> builder = ImmutableListMultimap.builder();
        var partitions = ThreadLocalRandom.current().nextInt(1, MAX_PARTITIONS_PER_REQUEST);
        int maxSize = ThreadLocalRandom.current().nextInt(1000, MAX_REQUEST_SIZE / partitions);
        for (int i = 0; i < partitions; i++) {
            builder.put(i, randomSlice(maxSize));
        }
        return builder.build();
    }

    private static Slice randomSlice(int maxSize)
    {
        int length = ThreadLocalRandom.current().nextInt(0, maxSize);
        int offset = ThreadLocalRandom.current().nextInt(0, randomBytes.length - length);
        return Slices.wrappedBuffer(randomBytes, offset, length);
    }
}
