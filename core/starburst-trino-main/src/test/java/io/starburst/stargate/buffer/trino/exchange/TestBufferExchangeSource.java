/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacade.ChunkDataResponse;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacade.RetryExecutorConfig;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.trino.exchange.ExternalExchangeIds.externalExchangeId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBufferExchangeSource
{
    private static final long NODE_ID = 1;
    private static final String EXTERNAL_EXCHANGE_ID = externalExchangeId(new QueryId("query"), new ExchangeId("exchange"));
    private static final int CHUNK_SIZE = 1024 * 1024;
    private static final int PARALLELISM = 4;
    private static final ExchangeSourceOutputSelector OUTPUT_SELECTOR = new ExchangeSourceOutputSelector(0, ImmutableMap.of(), false);

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testAddSourceHandlesSchedulesReadAhead()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(2 * PARALLELISM)));

        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM);
        assertThat(source.getMemoryUsage()).isBetween(PARALLELISM * (long) CHUNK_SIZE, (PARALLELISM + 1) * (long) CHUNK_SIZE);
    }

    @Test
    public void testOutputSelectorSchedulesReadAhead()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.addSourceHandles(ImmutableList.of(sourceHandle(2 * PARALLELISM)));
        assertThat(dataApi.requestCount()).isEqualTo(0);

        source.setOutputSelector(OUTPUT_SELECTOR);
        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM);
    }

    @Test
    public void testReadSchedulesReadAhead()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(2 * PARALLELISM)));

        for (int request = 0; request < PARALLELISM; request++) {
            dataApi.respond(request, ImmutableList.of(dataPage()));
        }
        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM);

        assertThat(source.read()).isNotNull();
        assertThat(dataApi.requestCount()).isEqualTo(2 * PARALLELISM);
    }

    @Test
    public void testEmptyChunksStartNextReader()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(PARALLELISM + 2)));

        for (int request = 0; request < PARALLELISM - 1; request++) {
            dataApi.respond(request, ImmutableList.of());
        }
        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM);

        dataApi.respond(PARALLELISM - 1, ImmutableList.of());
        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM + 1);

        dataApi.respond(PARALLELISM, ImmutableList.of(dataPage()));
        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM + 1);
        assertThat(source.isBlocked()).isDone();
    }

    @Test
    public void testReaderStartedAfterEmptyChunkUnblocksConsumer()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(PARALLELISM + 1)));

        for (int request = 0; request < PARALLELISM - 1; request++) {
            dataApi.respond(request, ImmutableList.of());
        }
        CompletableFuture<Void> blocked = source.isBlocked();
        assertThat(blocked).isNotDone();

        dataApi.respond(PARALLELISM - 1, ImmutableList.of());
        assertThat(dataApi.requestCount()).isEqualTo(PARALLELISM + 1);
        assertThat(blocked).isDone();
        assertThat(source.read()).isNull();
        assertThat(source.getMemoryUsage()).isBetween((long) CHUNK_SIZE, 2L * CHUNK_SIZE);
        assertThat(source.isBlocked()).isNotDone();
    }

    @Test
    public void testHighWaterMarkPausesReadAhead()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi, DataSize.ofBytes(CHUNK_SIZE), DataSize.ofBytes(2L * CHUNK_SIZE));
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(2 * PARALLELISM)));
        assertThat(dataApi.requestCount())
                .as("the reader that crosses the high water mark is the last one started")
                .isEqualTo(3);

        dataApi.respond(0, ImmutableList.of());
        dataApi.respond(1, ImmutableList.of());
        assertThat(dataApi.requestCount())
                .as("read-ahead stays paused above the low water mark")
                .isEqualTo(3);

        dataApi.respond(2, ImmutableList.of());
        assertThat(dataApi.requestCount())
                .as("dropping below the low water mark resumes with one reader")
                .isEqualTo(4);
    }

    @Test
    public void testPreserveOrderingReadsOneChunkAtATime()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(2 * PARALLELISM, true)));
        assertThat(dataApi.requestCount()).isEqualTo(1);

        dataApi.respond(0, ImmutableList.of(dataPage()));
        assertThat(dataApi.requestCount()).isEqualTo(1);

        assertThat(source.read()).isNotNull();
        assertThat(dataApi.requestCount()).isEqualTo(2);
    }

    @Test
    public void testReadReleasesMemoryUsage()
    {
        TestingDataApiFacade dataApi = new TestingDataApiFacade(executor);
        BufferExchangeSource source = createSource(dataApi);
        source.setOutputSelector(OUTPUT_SELECTOR);
        source.addSourceHandles(ImmutableList.of(sourceHandle(PARALLELISM)));

        for (int request = 0; request < PARALLELISM; request++) {
            dataApi.respond(request, ImmutableList.of(dataPage()));
        }
        assertThat(source.getMemoryUsage()).isBetween(PARALLELISM * (long) CHUNK_SIZE, (PARALLELISM + 1) * (long) CHUNK_SIZE);

        for (int page = 0; page < PARALLELISM; page++) {
            assertThat(source.read()).isNotNull();
        }
        assertThat(source.getMemoryUsage()).isLessThan(CHUNK_SIZE);
    }

    private static BufferExchangeSource createSource(DataApiFacade dataApi)
    {
        return createSource(dataApi, DataSize.of(32, MEGABYTE), DataSize.of(64, MEGABYTE));
    }

    private static BufferExchangeSource createSource(DataApiFacade dataApi, DataSize memoryLowWaterMark, DataSize memoryHighWaterMark)
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(NODE_ID, BufferNodeState.ACTIVE));
        return new BufferExchangeSource(
                dataApi,
                discoveryManager,
                newDirectExecutorService(),
                memoryLowWaterMark,
                memoryHighWaterMark,
                PARALLELISM);
    }

    private static BufferExchangeSourceHandle sourceHandle(int chunkCount)
    {
        return sourceHandle(chunkCount, false);
    }

    private static BufferExchangeSourceHandle sourceHandle(int chunkCount, boolean preserveOrderWithinPartition)
    {
        List<ChunkHandle> chunkHandles = IntStream.range(0, chunkCount)
                .mapToObj(chunkId -> new ChunkHandle(NODE_ID, 0, chunkId, CHUNK_SIZE))
                .collect(toImmutableList());
        return BufferExchangeSourceHandle.fromChunkHandles(EXTERNAL_EXCHANGE_ID, 0, chunkHandles, preserveOrderWithinPartition);
    }

    private static DataPage dataPage()
    {
        return new DataPage(0, 0, Slices.allocate(CHUNK_SIZE));
    }

    private static class TestingDataApiFacade
            extends DataApiFacade
    {
        private static final RetryExecutorConfig RETRY_EXECUTOR_CONFIG = new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s"));

        private final List<SettableFuture<ChunkDataResponse>> requests = new ArrayList<>();

        private TestingDataApiFacade(ScheduledExecutorService executor)
        {
            super(new TestingBufferNodeDiscoveryManager(), new UnusedApiFactory(), RETRY_EXECUTOR_CONFIG, RETRY_EXECUTOR_CONFIG, 1, executor);
        }

        @Override
        public synchronized ListenableFuture<ChunkDataResponse> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId, long chunkBufferNodeId)
        {
            SettableFuture<ChunkDataResponse> request = SettableFuture.create();
            requests.add(request);
            return request;
        }

        private synchronized int requestCount()
        {
            return requests.size();
        }

        private void respond(int requestIndex, List<DataPage> pages)
        {
            SettableFuture<ChunkDataResponse> request;
            synchronized (this) {
                request = requests.get(requestIndex);
            }
            request.set(new ChunkDataResponse(pages, false, 0, 0, 0));
        }
    }

    private static class UnusedApiFactory
            implements ApiFactory
    {
        @Override
        public DiscoveryApi createDiscoveryApi()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataApi createDataApi(BufferNodeInfo nodeInfo)
        {
            throw new UnsupportedOperationException();
        }
    }
}
