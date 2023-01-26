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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDataApiFacade
{
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(4);

    @AfterAll
    public void teardown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testRetriesWithDelay()
            throws InterruptedException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("500ms"), Duration.valueOf("1000ms"), 2.0, 0.0, executor);

        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));

        ListenableFuture<ChunkList> future = dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, "exchange-1", OptionalLong.empty());

        Thread.sleep(100);
        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(1);
        Thread.sleep(550);
        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(2);
        Thread.sleep(1000);
        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(3);
        assertThat(future).isDone();
    }

    @Test
    public void testRetriesOnRuntimeException()
            throws ExecutionException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, "exchange-1", OptionalLong.empty()))
                .succeedsWithin(5, SECONDS)
                .isEqualTo(result);

        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty()))
                .isEqualTo(3);
    }

    @Test
    public void testRetriesOnInternalErrorException()
            throws ExecutionException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, "exchange-1", OptionalLong.empty()))
                .succeedsWithin(5, SECONDS)
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(3);
    }

    @ParameterizedTest
    @EnumSource(value = ErrorCode.class)
    public void testDoNotRetryOnMostDataApiExceptions(ErrorCode errorCode)
    {
        if (errorCode == ErrorCode.INTERNAL_ERROR || errorCode == ErrorCode.BUFFER_NODE_NOT_FOUND) {
            return; // skip
        }
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new DataApiException(errorCode, "blah")));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, "exchange-1", OptionalLong.empty()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(errorCode));

        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(1);
    }

    @Test
    public void testAddDataPagesRetries()
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);
        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        // OK after INTERNAL_ERROR
        dataApiDelegate.recordAddDataPages("exchange-1", 0, 0, 0, Futures.immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages("exchange-1", 0, 0, 0, Futures.immediateVoidFuture());
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-1", 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount("exchange-1", 0, 0, 0)).isEqualTo(2);

        // Immediate DRAINING
        dataApiDelegate.recordAddDataPages("exchange-2", 0, 0, 0, Futures.immediateFailedFuture(new DataApiException(ErrorCode.DRAINING, "blah")));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-2", 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINING));
        assertThat(dataApiDelegate.getAddDataPagesCallCount("exchange-2", 0, 0, 0)).isEqualTo(1);

        // DRAINING after INTERNAL_ERROR
        dataApiDelegate.recordAddDataPages("exchange-3", 0, 0, 0, Futures.immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages("exchange-3", 0, 0, 0, Futures.immediateFailedFuture(new DataApiException(ErrorCode.DRAINING, "blah")));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-3", 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINING_ON_RETRY))
                .withMessageContaining("Received DRAINING error code on retry");
        assertThat(dataApiDelegate.getAddDataPagesCallCount("exchange-3", 0, 0, 0)).isEqualTo(2);
    }

    @Test
    public void testDoNotRetryOnSuccess()
            throws ExecutionException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);
        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, executor);

        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, "exchange-1", OptionalLong.empty()))
                .succeedsWithin(100, MILLISECONDS) // returns immediately
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(1);
    }

    @Test
    public void testShortCircuitResponseIfNodeDrained()
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.DRAINED));
        TestingApiFactory apiFactory = new TestingApiFactory();
        TestingDataApi dataApiDelegate = new TestingDataApi();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);
        DataApiFacade dataApiFacade = new DataApiFacade(discoveryManager, apiFactory, 2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, executor);

        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, "exchange-1", OptionalLong.empty()));
        // we expect no communication with client
        assertThat(dataApiDelegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(0);

        // check other calls too
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.pingExchange(TestingDataApi.NODE_ID, "exchange-1"));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.finishExchange(TestingDataApi.NODE_ID, "exchange-1"));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.removeExchange(TestingDataApi.NODE_ID, "exchange-1"));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.getChunkData(TestingDataApi.NODE_ID, "exchange-1", 1, 1L, 1L));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.registerExchange(TestingDataApi.NODE_ID, "exchange-1"));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-1", 1, 1, 1L, ImmutableListMultimap.of()));
    }

    private static void assertShortCircuitResponseIfNodeDrained(Supplier<ListenableFuture<?>> call)
    {
        assertThat(call.get())
                .failsWithin(100, MILLISECONDS) // returns immediatelly
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINED))
                .withMessageContaining("Node already DRAINED");
    }

    private static class TestingDataApi
            implements DataApi
    {
        public static final int NODE_ID = 1;

        private record ListClosedChunksKey(String exchangeId, OptionalLong pagingId) {};
        private record AddDataPagesKey(String exchangeId, int taskId, int attemptId, long dataPagesId) {};

        private final ListMultimap<ListClosedChunksKey, ListenableFuture<ChunkList>> listClosedChunksResponses = ArrayListMultimap.create();
        private final Map<ListClosedChunksKey, AtomicLong> listClosedChunksCounters = new HashMap<>();
        private final ListMultimap<AddDataPagesKey, ListenableFuture<Void>> addDataPagesResponses = ArrayListMultimap.create();
        private final Map<AddDataPagesKey, AtomicLong> addDataPagesCounters = new HashMap<>();

        @Override
        public BufferNodeInfo getInfo()
        {
            return new BufferNodeInfo(NODE_ID, URI.create("http://testing"), Optional.empty(), BufferNodeState.ACTIVE);
        }

        @Override
        public synchronized ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
        {
            ListClosedChunksKey key = new ListClosedChunksKey(exchangeId, pagingId);
            List<ListenableFuture<ChunkList>> responses = listClosedChunksResponses.get(key);
            if (responses.isEmpty()) {
                throw new IllegalStateException("no response recorded for " + key);
            }
            listClosedChunksCounters.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
        }

        public synchronized void recordListClosedChunks(String exchangeId, OptionalLong pagingId, ListenableFuture<ChunkList> result)
        {
            listClosedChunksResponses.put(new ListClosedChunksKey(exchangeId, pagingId), result);
        }

        public synchronized long getListClosedChunksCallCount(String exchangeId, OptionalLong pagingId)
        {
            return listClosedChunksCounters.computeIfAbsent(new ListClosedChunksKey(exchangeId, pagingId), ignored -> new AtomicLong()).get();
        }

        @Override
        public ListenableFuture<Void> markAllClosedChunksReceived(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> registerExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> pingExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> removeExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
        {
            AddDataPagesKey key = new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId);
            List<ListenableFuture<Void>> responses = addDataPagesResponses.get(key);
            if (responses.isEmpty()) {
                throw new IllegalStateException("no response recorded for " + key);
            }
            addDataPagesCounters.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
        }

        public synchronized void recordAddDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListenableFuture<Void> result)
        {
            addDataPagesResponses.put(new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId), result);
        }

        public synchronized long getAddDataPagesCallCount(String exchangeId, int taskId, int attemptId, long dataPagesId)
        {
            return addDataPagesCounters.computeIfAbsent(new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId), ignored -> new AtomicLong()).get();
        }

        @Override
        public synchronized ListenableFuture<Void> finishExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
        {
            throw new RuntimeException("not implemented");
        }
    }

    private static class TestingApiFactory
            implements ApiFactory
    {
        private final Map<Long, DataApi> dataApis = new HashMap<>();

        @Override
        public DiscoveryApi createDiscoveryApi()
        {
            throw new RuntimeException("not implemented");
        }

        public void setDataApi(long nodeId, DataApi dataApi)
        {
            dataApis.put(nodeId, dataApi);
        }

        public void removeDataApi(long nodeId)
        {
            dataApis.remove(nodeId);
        }

        @Override
        public DataApi createDataApi(BufferNodeInfo nodeInfo)
        {
            DataApi dataApi = dataApis.get(nodeInfo.nodeId());
            if (dataApi == null) {
                throw new RuntimeException("cannot create dataApi for node " + nodeInfo.nodeId());
            }
            return dataApi;
        }
    }
}
