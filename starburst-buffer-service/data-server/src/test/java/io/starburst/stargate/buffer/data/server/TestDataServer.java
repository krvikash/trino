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
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.HttpDataClient;
import io.starburst.stargate.buffer.data.server.testing.TestingDataServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Closeables.closeAll;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestDataServer
{
    private static final String EXCHANGE_0 = "exchange-0";
    private static final String EXCHANGE_1 = "exchange-1";
    private static final long BUFFER_NODE_ID = 0;

    private TestingDataServer dataServer;
    private HttpClient httpClient;
    private HttpDataClient dataClient;

    @BeforeEach
    public void setup()
    {
        dataServer = TestingDataServer.builder()
                .setDiscoveryBroadcastEnabled(false)
                .build();
        httpClient = new JettyHttpClient(new HttpClientConfig());
        dataClient = new HttpDataClient(dataServer.getBaseUri(), httpClient, true);
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        closeAll(dataServer, httpClient);
    }

    @Test
    public void testHappyPath()
    {
        Slice largePage1 = utf8Slice("1".repeat((int) DataSize.of(10, MEGABYTE).toBytes()));
        Slice largePage2 = utf8Slice("2".repeat((int) DataSize.of(10, MEGABYTE).toBytes()));

        addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("trino"));
        addDataPage(EXCHANGE_0, 0, 1, 0, 1L, utf8Slice("buffer"));

        registerExchange(EXCHANGE_0);
        registerExchange(EXCHANGE_1);

        addDataPage(EXCHANGE_0, 1, 0, 1, 2L, utf8Slice("service"));
        addDataPage(EXCHANGE_1, 0, 0, 0, 0L, utf8Slice("tardigrade"));
        addDataPage(EXCHANGE_1, 1, 2, 0, 1L, largePage1);
        addDataPage(EXCHANGE_1, 1, 2, 0, 2L, largePage2);

        pingExchange(EXCHANGE_0);
        pingExchange(EXCHANGE_1);

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 11);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 7);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 10);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 1, 3L, (int) DataSize.of(10, MEGABYTE).toBytes());
        ChunkHandle chunkHandle4 = new ChunkHandle(BUFFER_NODE_ID, 1, 4L, (int) DataSize.of(10, MEGABYTE).toBytes());

        finishExchange(EXCHANGE_0);

        ChunkList chunkList0 = listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);
        assertTrue(chunkList0.nextPagingId().isEmpty());
        ChunkList chunkList1 = listClosedChunks(EXCHANGE_1, OptionalLong.empty());
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle3);
        assertTrue(chunkList1.nextPagingId().isPresent());
        assertEquals(chunkList1.nextPagingId().getAsLong(), 1L);

        finishExchange(EXCHANGE_1);

        chunkList1 = listClosedChunks(EXCHANGE_1, OptionalLong.of(1L));
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

        // Note: this is not a strict contract that we will get chunks for partition 0 before chunks for partition 1, but
        // it is an implication of use of ImmutableListMultimap above which returns keys in order in which they were added to the map.
        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 4 + largePage1.length()); // a, b, c, largePage1, d
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 1 + largePage2.length()); // largePage2, e
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 3 + largePage1.length()); // v, x, largePage1, y
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 1, 3L, 1 + largePage2.length()); // largePage2, z

        finishExchange(EXCHANGE_0);

        ChunkList chunkList0 = listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2, chunkHandle3);
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
    public void testExceptions()
    {
        addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("error"));

        assertThatThrownBy(() -> finishExchange(EXCHANGE_1))
                .isInstanceOf(DataApiException.class).hasMessage("exchange %s not found".formatted(EXCHANGE_1));
        assertThatThrownBy(() -> listClosedChunks(EXCHANGE_0, OptionalLong.of(Long.MAX_VALUE)))
                .isInstanceOf(DataApiException.class).hasMessageContaining("Expected pagingId to equal next pagingId");
        assertThatThrownBy(() -> getChunkData(EXCHANGE_0, new ChunkHandle(-1L, 0, 3L, 0)))
                .isInstanceOf(DataApiException.class).hasMessage("No closed chunk found for exchange %s, partition 0, chunk 3".formatted(EXCHANGE_0));

        finishExchange(EXCHANGE_0);

        assertThatThrownBy(() -> addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("exception")))
                .isInstanceOf(DataApiException.class).hasMessage("exchange %s already finished".formatted(EXCHANGE_0));

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

    private ChunkList listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        return getFutureValue(dataClient.listClosedChunks(exchangeId, pagingId));
    }

    private List<DataPage> getChunkData(String exchangeId, ChunkHandle chunkHandle)
    {
        return getChunkData(exchangeId, chunkHandle.partitionId(), chunkHandle.chunkId(), chunkHandle.bufferNodeId());
    }

    private List<DataPage> getChunkData(String exchangeId, int partitionId, long chunkId, long bufferNodeId)
    {
        return getFutureValue(dataClient.getChunkData(exchangeId, partitionId, chunkId, bufferNodeId));
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
}
