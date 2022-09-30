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

import com.google.common.collect.ImmutableList;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Closeables.closeAll;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.server.testing.TestingDataServer.BUFFER_NODE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDataServer
{
    private static final String EXCHANGE_0 = "exchange-0";
    private static final String EXCHANGE_1 = "exchange-1";

    private TestingDataServer dataServer;
    private HttpClient httpClient;
    private HttpDataClient dataClient;

    @BeforeAll
    public void setup()
    {
        dataServer = TestingDataServer.builder()
                .setDiscoveryBroadcastEnabled(false)
                .build();
        httpClient = new JettyHttpClient(new HttpClientConfig());
        dataClient = new HttpDataClient(dataServer.getBaseUri(), httpClient, true);
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        closeAll(dataServer, httpClient);
    }

    @Test
    @Order(1)
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

        assertThat(getChunkData(EXCHANGE_0, chunkHandle0)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("trino")),
                new DataPage(1, 0, utf8Slice("buffer")));
        assertThat(getChunkData(EXCHANGE_0, chunkHandle1)).containsExactlyInAnyOrder(
                new DataPage(0, 1, utf8Slice("service")));
        assertThat(getChunkData(EXCHANGE_1, chunkHandle2)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("tardigrade")));
        assertThat(getChunkData(EXCHANGE_1, chunkHandle3)).containsExactlyInAnyOrder(
                new DataPage(2, 0, largePage1));
        assertThat(getChunkData(EXCHANGE_1, chunkHandle4)).containsExactlyInAnyOrder(
                new DataPage(2, 0, largePage2));

        removeExchange(EXCHANGE_0);
        removeExchange(EXCHANGE_1);
    }

    @Test
    @Order(2)
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

    private void addDataPage(String exchangeId, int partitionId, int taskId, int attemptId, long dataPageId, Slice data)
    {
        getFutureValue(dataClient.addDataPages(exchangeId, partitionId, taskId, attemptId, dataPageId, ImmutableList.of(data)));
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
        return getChunkData(exchangeId, chunkHandle.bufferNodeId(), chunkHandle.partitionId(), chunkHandle.chunkId());
    }

    private List<DataPage> getChunkData(String exchangeId, long bufferNodeId, int partitionId, long chunkId)
    {
        return getFutureValue(dataClient.getChunkData(exchangeId, bufferNodeId, partitionId, chunkId));
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
