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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.HttpDataClient;
import io.starburst.stargate.buffer.data.client.spooling.local.LocalSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.noop.NoopSpooledChunkReader;
import io.starburst.stargate.buffer.data.server.testing.TestingDataServer;
import io.starburst.stargate.buffer.data.server.testing.TestingDiscoveryApiModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Closeables.closeAll;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.awaitility.Durations.TWO_SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestDataServer
{
    private static final String EXCHANGE_0 = "exchange-0";
    private static final String EXCHANGE_1 = "exchange-1";
    private static final long BUFFER_NODE_ID = 0;
    private static final DataSize DATA_SERVER_AVAILABLE_MEMORY = DataSize.of(70, MEGABYTE);

    private TestingDataServer dataServer;
    private HttpClient httpClient;
    private HttpDataClient dataClient;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @BeforeEach
    public void setup()
    {
        dataServer = TestingDataServer.builder()
                .withDiscoveryApiModule(new TestingDiscoveryApiModule())
                .setConfigProperty("spooling.directory", System.getProperty("java.io.tmpdir") + "/spooling-storage")
                .setConfigProperty("discovery-broadcast-interval", "10ms")
                .setConfigProperty("memory.heap-headroom", succinctBytes(Runtime.getRuntime().maxMemory() - DATA_SERVER_AVAILABLE_MEMORY.toBytes()).toString())
                .setConfigProperty("memory.allocation-low-watermark", "0.99")
                .setConfigProperty("memory.allocation-high-watermark", "0.99")
                .setConfigProperty("draining.min-duration", "2s")
                .build();
        httpClient = new JettyHttpClient(new HttpClientConfig().setMaxContentLength(DataSize.of(64, MEGABYTE)));
        dataClient = new HttpDataClient(dataServer.getBaseUri(), BUFFER_NODE_ID, httpClient, succinctDuration(60, SECONDS), new LocalSpooledChunkReader(new DataApiConfig()), true);

        // Wait for Node to become ready
        await().atMost(TEN_SECONDS).until(
                () -> dataClient.getInfo().state(),
                BufferNodeState.ACTIVE::equals);
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        executor.shutdownNow();
        closeAll(dataServer, httpClient);
    }

    @Test
    public void testBufferNodeInfo()
    {
        BufferNodeInfo bufferNodeInfo = dataClient.getInfo();
        assertThat(bufferNodeInfo.nodeId()).isEqualTo(0);
        assertThat(bufferNodeInfo.stats()).isNotEmpty();

        // Assure Node status is propagated to Discovery server
        assertThat(
                dataServer.getDiscovery().get()
                        .getBufferNodes()
                        .bufferNodeInfos()
                        .stream()
                        .filter(node -> node.nodeId() == bufferNodeInfo.nodeId())
                        .findFirst()
                        .map(BufferNodeInfo::state)
                        .orElse(null))
                .isEqualTo(dataClient.getInfo().state());
    }

    @Test
    public void testHappyPath()
    {
        Slice largePage1 = utf8Slice("1".repeat((int) DataSize.of(10, MEGABYTE).toBytes()));
        Slice largePage2 = utf8Slice("2".repeat((int) DataSize.of(20, MEGABYTE).toBytes()));

        addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("trino"));
        addDataPage(EXCHANGE_0, 0, 1, 0, 1L, utf8Slice("buffer"));

        assertNodeStats(1, 1, 0, 0);

        registerExchange(EXCHANGE_0);
        registerExchange(EXCHANGE_1);

        assertNodeStats(2, 1, 0, 0);

        addDataPage(EXCHANGE_0, 1, 0, 1, 2L, utf8Slice("service"));
        addDataPage(EXCHANGE_1, 0, 0, 0, 0L, utf8Slice("tardigrade"));
        addDataPage(EXCHANGE_1, 1, 2, 0, 1L, largePage1);
        addDataPage(EXCHANGE_1, 1, 2, 0, 2L, largePage2);

        pingExchange(EXCHANGE_0);
        pingExchange(EXCHANGE_1);

        assertNodeStats(2, 4, 0, 1);

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 11);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 7);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 10);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 1, 3L, largePage1.length());
        ChunkHandle chunkHandle4 = new ChunkHandle(BUFFER_NODE_ID, 1, 4L, largePage2.length());

        finishExchange(EXCHANGE_0);
        assertNodeStats(2, 2, 0, 3);

        ChunkList chunkList0 = listClosedChunks(EXCHANGE_0, OptionalLong.empty(), 2);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);
        assertTrue(chunkList0.nextPagingId().isEmpty());
        ChunkList chunkList1 = listClosedChunks(EXCHANGE_1, OptionalLong.empty(), 1);
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle3);
        assertTrue(chunkList1.nextPagingId().isPresent());

        finishExchange(EXCHANGE_1);
        assertNodeStats(2, 0, 0, 5);

        chunkList1 = listClosedChunks(EXCHANGE_1, chunkList1.nextPagingId(), 2);
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle2, chunkHandle4);
        assertTrue(chunkList1.nextPagingId().isEmpty());

        assertThat(getChunkData(EXCHANGE_0, chunkHandle0)).containsExactly(
                new DataPage(0, 0, utf8Slice("trino")),
                new DataPage(1, 0, utf8Slice("buffer")));
        assertThat(getChunkData(EXCHANGE_0, chunkHandle1)).containsExactly(
                new DataPage(0, 1, utf8Slice("service")));
        assertThat(getChunkData(EXCHANGE_1, chunkHandle2)).containsExactly(
                new DataPage(0, 0, utf8Slice("tardigrade")));
        assertThat(getChunkData(EXCHANGE_1, chunkHandle3)).containsExactly(
                new DataPage(2, 0, largePage1));
        assertThat(getChunkData(EXCHANGE_1, chunkHandle4)).containsExactly(
                new DataPage(2, 0, largePage2));

        removeExchange(EXCHANGE_0);
        removeExchange(EXCHANGE_1);
        assertNodeStats(0, 0, 0, 0);
    }

    @Test
    public void testAddPagesMultiplePartitions()
    {
        Slice largePage1 = utf8Slice("1".repeat((int) DataSize.of(10, MEGABYTE).toBytes()));
        Slice largePage2 = utf8Slice("2".repeat((int) DataSize.of(10, MEGABYTE).toBytes()));

        registerExchange(EXCHANGE_0);

        ImmutableListMultimap.Builder<Integer, Slice> dataPages = ImmutableListMultimap.<Integer, Slice>builder();
        dataPages.put(0, utf8Slice("a"));
        dataPages.put(0, utf8Slice("b"));
        dataPages.put(0, utf8Slice("c"));
        dataPages.put(0, largePage1);
        dataPages.put(0, utf8Slice("d"));
        dataPages.put(0, largePage2);
        dataPages.put(0, utf8Slice("e"));
        dataPages.put(1, utf8Slice("v"));
        dataPages.put(1, utf8Slice("x"));
        dataPages.put(1, largePage1);
        dataPages.put(1, utf8Slice("y"));
        dataPages.put(1, largePage2);
        dataPages.put(1, utf8Slice("z"));

        addDataPages(EXCHANGE_0, 0, 0, 0, dataPages.build());

        finishExchange(EXCHANGE_0);

        ChunkList chunkList0 = listClosedChunks(EXCHANGE_0, OptionalLong.empty(), 4);
        // Note: partition writes happen concurrently, so chunk ids assignment will be non-deterministic
        ChunkHandle chunkHandle0 = getChunkHandleOrThrow(chunkList0.chunks(), 0, 4 + largePage1.length()); // a, b, c, largePage1, d
        ChunkHandle chunkHandle1 = getChunkHandleOrThrow(chunkList0.chunks(), 0, 1 + largePage2.length()); // largePage2, e
        ChunkHandle chunkHandle2 = getChunkHandleOrThrow(chunkList0.chunks(), 1, 3 + largePage1.length()); // v, x, largePage1, y
        ChunkHandle chunkHandle3 = getChunkHandleOrThrow(chunkList0.chunks(), 1, 1 + largePage2.length()); // largePage2, z

        assertTrue(chunkList0.nextPagingId().isEmpty());
        assertThat(getChunkData(EXCHANGE_0, chunkHandle0)).containsExactly(
                new DataPage(0, 0, utf8Slice("a")),
                new DataPage(0, 0, utf8Slice("b")),
                new DataPage(0, 0, utf8Slice("c")),
                new DataPage(0, 0, largePage1),
                new DataPage(0, 0, utf8Slice("d")));
        assertThat(getChunkData(EXCHANGE_0, chunkHandle1)).containsExactly(
                new DataPage(0, 0, largePage2),
                new DataPage(0, 0, utf8Slice("e")));
        assertThat(getChunkData(EXCHANGE_0, chunkHandle2)).containsExactly(
                new DataPage(0, 0, utf8Slice("v")),
                new DataPage(0, 0, utf8Slice("x")),
                new DataPage(0, 0, largePage1),
                new DataPage(0, 0, utf8Slice("y")));
        assertThat(getChunkData(EXCHANGE_0, chunkHandle3)).containsExactly(
                new DataPage(0, 0, largePage2),
                new DataPage(0, 0, utf8Slice("z")));

        removeExchange(EXCHANGE_0);
    }

    @Test
    public void testDraining()
    {
        addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("dummy"));

        Request drainRequest = Request.builder()
                .setMethod("GET")
                .setUri(uriBuilderFrom(requireNonNull(dataServer.getBaseUri(), "baseUri is null"))
                        .replacePath("/api/v1/buffer/data/drain")
                        .build())
                .build();

        assertThat(httpClient.execute(drainRequest, createStatusResponseHandler()).getStatusCode())
                .isEqualTo(200);
        await().atMost(ONE_SECOND).until(
                this::getNodeState,
                BufferNodeState.DRAINING::equals);

        assertThatThrownBy(() -> addDataPage(EXCHANGE_0, 1, 1, 1, 1L, utf8Slice("dummy")))
                .isInstanceOf(DataApiException.class)
                .hasMessage("error on POST %s/api/v1/buffer/data/%s/addDataPages/1/1/1?targetBufferNodeId=0: Node %d is draining and not accepting any more data"
                        .formatted(dataServer.getBaseUri(), EXCHANGE_0, BUFFER_NODE_ID));

        Future<ChunkList> chunkListFuture = executor.submit(() -> {
            OptionalLong pagingId = OptionalLong.empty();
            for (int i = 0; i < 10; ++i) {
                ChunkList chunkList = getFutureValue(dataClient.listClosedChunks(EXCHANGE_0, pagingId));
                pagingId = chunkList.nextPagingId();
                if (pagingId.isEmpty()) {
                    markAllClosedChunksReceived(EXCHANGE_0);
                    return chunkList;
                }
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return fail();
        });

        await().atMost(ONE_SECOND).until(chunkListFuture::isDone);
        ChunkHandle chunkHandle = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5);
        assertThat(getFutureValue(chunkListFuture).chunks()).containsExactly(chunkHandle);

        await().atMost(FIVE_SECONDS).until(
                this::getNodeState,
                BufferNodeState.DRAINED::equals);
    }

    private BufferNodeState getNodeState()
    {
        return BufferNodeState.valueOf(httpClient.execute(
                Request.builder()
                        .setMethod("GET")
                        .setUri(uriBuilderFrom(requireNonNull(dataServer.getBaseUri(), "baseUri is null"))
                                .replacePath("/api/v1/buffer/data/state")
                                .build())
                        .build(), createStringResponseHandler()).getBody().trim());
    }

    @Test
    public void testExceptions()
    {
        addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("error"));

        assertThatThrownBy(() -> finishExchange(EXCHANGE_1))
                .isInstanceOf(DataApiException.class)
                .hasMessage("error on GET %s/api/v1/buffer/data/exchange-1/finish?targetBufferNodeId=0: exchange %s not found".formatted(dataServer.getBaseUri(), EXCHANGE_1));
        assertThatThrownBy(() -> getFutureValue(dataClient.listClosedChunks(EXCHANGE_0, OptionalLong.of(Long.MAX_VALUE))))
                .isInstanceOf(DataApiException.class)
                .hasMessageContaining("pagingId %s does not equal nextPagingId 0".formatted(Long.MAX_VALUE));
        assertThatThrownBy(() -> getChunkData(EXCHANGE_0, new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 0)))
                .isInstanceOf(DataApiException.class)
                .hasMessage("error on GET %s/api/v1/buffer/data/0/exchange-0/pages/0/3?targetBufferNodeId=0: No closed chunk found for bufferNodeId %d, exchange %s, chunk 3".formatted(dataServer.getBaseUri(), BUFFER_NODE_ID, EXCHANGE_0));

        finishExchange(EXCHANGE_0);

        assertThatThrownBy(() -> addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("exception")))
                .isInstanceOf(DataApiException.class)
                .hasMessage("error on POST %s/api/v1/buffer/data/exchange-0/addDataPages/0/0/0?targetBufferNodeId=0: exchange %s already finished".formatted(dataServer.getBaseUri(), EXCHANGE_0));

        removeExchange(EXCHANGE_0);
    }

    @Test
    public void testInvalidTargetDataNodeId()
    {
        HttpDataClient invalidDataClient = new HttpDataClient(dataServer.getBaseUri(), BUFFER_NODE_ID + 1, httpClient, succinctDuration(60, SECONDS), new NoopSpooledChunkReader(), true);
        Slice largePage = utf8Slice("1".repeat((int) DataSize.of(10, MEGABYTE).toBytes()));

        assertThatThrownBy(() -> getFutureValue(invalidDataClient.addDataPages(EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of(0, largePage))))
                .isInstanceOf(DataApiException.class)
                .matches(e -> ((DataApiException) e).getErrorCode() == USER_ERROR)
                .hasMessageContaining("target buffer node mismatch (1 vs 0)");
    }

    @Test
    public void testSpooling()
    {
        int pageSizeInBytes = (int) DataSize.of(10, MEGABYTE).toBytes() - DATA_PAGE_HEADER_SIZE;
        for (int index = 0; index < 10; ++index) {
            addDataPage(EXCHANGE_0, index, index, index, index, utf8Slice(String.valueOf(index).repeat(pageSizeInBytes)));
        }
        finishExchange(EXCHANGE_0);

        List<ChunkHandle> chunkHandles = listClosedChunks(EXCHANGE_0, OptionalLong.empty(), 10).chunks();
        assertNodeStats(1, 0, 5, 5);

        for (ChunkHandle chunkHandle : chunkHandles) {
            int index = chunkHandle.partitionId();
            assertThat(getChunkData(EXCHANGE_0, chunkHandle)).containsExactly(new DataPage(index, index, utf8Slice(String.valueOf(index).repeat(pageSizeInBytes))));
        }

        removeExchange(EXCHANGE_0);
    }

    private void addDataPage(String exchangeId, int partitionId, int taskId, int attemptId, long dataPagesId, Slice data)
    {
        getFutureValue(dataClient.addDataPages(exchangeId, taskId, attemptId, dataPagesId, ImmutableListMultimap.of(partitionId, data)));
    }

    private void addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        getFutureValue(dataClient.addDataPages(exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition));
    }

    private void finishExchange(String exchangeId)
    {
        getFutureValue(dataClient.finishExchange(exchangeId));
    }

    private ChunkList listClosedChunks(String exchangeId, OptionalLong pagingId, int expectedChunkListSize)
    {
        List<ChunkHandle> chunkHandles = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            ChunkList chunkList = getFutureValue(dataClient.listClosedChunks(exchangeId, pagingId));
            chunkHandles.addAll(chunkList.chunks());
            pagingId = chunkList.nextPagingId();

            if (chunkHandles.size() == expectedChunkListSize) {
                return new ChunkList(chunkHandles, pagingId);
            }
            if (chunkHandles.size() > expectedChunkListSize) {
                return fail();
            }

            sleepUninterruptibly(100, MILLISECONDS);
        }
        return fail();
    }

    private void markAllClosedChunksReceived(String exchangeId)
    {
        getFutureValue(dataClient.markAllClosedChunksReceived(exchangeId));
    }

    private List<DataPage> getChunkData(String exchangeId, ChunkHandle chunkHandle)
    {
        return getChunkData(chunkHandle.bufferNodeId(), exchangeId, chunkHandle.partitionId(), chunkHandle.chunkId());
    }

    private List<DataPage> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        return getFutureValue(dataClient.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId));
    }

    private void registerExchange(String exchangeId)
    {
        getFutureValue(dataClient.registerExchange(exchangeId));
    }

    private void removeExchange(String exchangeId)
    {
        getFutureValue(dataClient.removeExchange(exchangeId));
    }

    private void pingExchange(String exchangeId)
    {
        getFutureValue(dataClient.pingExchange(exchangeId));
    }

    private ChunkHandle getChunkHandleOrThrow(List<ChunkHandle> chunkHandles, int partitionId, int dataSizeInBytes)
    {
        for (ChunkHandle chunkHandle : chunkHandles) {
            if (chunkHandle.partitionId() == partitionId && chunkHandle.dataSizeInBytes() == dataSizeInBytes) {
                return chunkHandle;
            }
        }
        return fail();
    }

    private void assertNodeStats(long trackedExchanges, int openChunks, int spooledChunks, int closedChunks)
    {
        await().atMost(TWO_SECONDS)
                .until(() -> nodeStatsEqual(trackedExchanges, openChunks, spooledChunks, closedChunks));
    }

    private boolean nodeStatsEqual(long trackedExchanges, int openChunks, int spooledChunks, int closedChunks)
    {
        BufferNodeStats bufferNodeStats = dataClient.getInfo().stats().get();
        return bufferNodeStats.trackedExchanges() == trackedExchanges &&
                bufferNodeStats.openChunks() == openChunks &&
                bufferNodeStats.spooledChunks() == spooledChunks &&
                bufferNodeStats.closedChunks() == closedChunks;
    }
}
