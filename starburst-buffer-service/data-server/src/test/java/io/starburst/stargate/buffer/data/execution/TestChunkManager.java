/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.execution.ChunkManagerConfig.DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.data.server.testing.TestingDataServer.BUFFER_NODE_ID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestChunkManager
{
    private static final String EXCHANGE_0 = "exchange-0";
    private static final String EXCHANGE_1 = "exchange-1";

    private final MemoryAllocator memoryAllocator = new MemoryAllocator(new MemoryAllocatorConfig(), new DataServerStats());
    private final TestingTicker ticker = new TestingTicker();

    @Test
    public void testSingleChunkPerPartition()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(16, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.registerExchange(EXCHANGE_1);

        chunkManager.addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("000_0"));
        chunkManager.addDataPage(EXCHANGE_0, 0, 1, 0, 1L, utf8Slice("001_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 0, 2L, utf8Slice("010_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 1, 0, 3L, utf8Slice("011_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 1, 4L, utf8Slice("010_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 1, 5L, utf8Slice("010_1"));
        chunkManager.addDataPage(EXCHANGE_1, 0, 0, 0, 6L, utf8Slice("100_0"));

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 10);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 20);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 5);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(3, chunkManager.getOpenChunks());
        assertEquals(0, chunkManager.getClosedChunks());

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertTrue(chunkList0.chunks().isEmpty());
        assertEquals(chunkList0.nextPagingId(), OptionalLong.of(1L));

        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty());
        assertTrue(chunkList1.chunks().isEmpty());
        assertEquals(chunkList1.nextPagingId(), OptionalLong.of(1L));

        chunkManager.finishExchange(EXCHANGE_0);
        chunkManager.finishExchange(EXCHANGE_1);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(0, chunkManager.getOpenChunks());
        assertEquals(3, chunkManager.getClosedChunks());

        chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.of(1L));
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.of(1L));
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle2);
        assertTrue(chunkList1.nextPagingId().isEmpty());

        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("000_0")),
                new DataPage(1, 0, utf8Slice("001_0")));
        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("010_0")),
                new DataPage(1, 0, utf8Slice("011_0")),
                new DataPage(0, 1, utf8Slice("010_0")),
                new DataPage(0, 1, utf8Slice("010_1")));
        assertThat(chunkManager.getChunkData(EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("100_0")));

        assertEquals(memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory(), DataSize.of(48, MEGABYTE).toBytes());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testMultipleChunksPerPartition()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(32, BYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.registerExchange(EXCHANGE_1);

        chunkManager.addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("000_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 0, 1L, utf8Slice("010_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 1, 0, 2L, utf8Slice("011_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 1, 3L, utf8Slice("010_0"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 1, 4L, utf8Slice("010_1"));
        chunkManager.addDataPage(EXCHANGE_1, 0, 0, 0, 0L, utf8Slice("100_0"));

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 10);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 10);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 5);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(3, chunkManager.getOpenChunks());
        assertEquals(1, chunkManager.getClosedChunks());

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle1);
        assertEquals(chunkList0.nextPagingId(), OptionalLong.of(1L));

        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty());
        assertTrue(chunkList1.chunks().isEmpty());
        assertEquals(chunkList0.nextPagingId(), OptionalLong.of(1L));

        chunkManager.finishExchange(EXCHANGE_0);
        chunkManager.finishExchange(EXCHANGE_1);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(0, chunkManager.getOpenChunks());
        assertEquals(4, chunkManager.getClosedChunks());

        chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.of(1L));
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle2);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.of(1L));
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle3);
        assertTrue(chunkList1.nextPagingId().isEmpty());

        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("000_0")));
        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("010_0")),
                new DataPage(1, 0, utf8Slice("011_0")));
        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 1, utf8Slice("010_0")),
                new DataPage(0, 1, utf8Slice("010_1")));
        assertThat(chunkManager.getChunkData(EXCHANGE_1, chunkHandle3.partitionId(), chunkHandle3.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("100_0")));

        assertEquals(memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory(), DataSize.of(128, BYTE).toBytes());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testPingExchange()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(16, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);

        ticker.increment(1000, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).isEmpty();
        assertEquals(chunkList0.nextPagingId(), OptionalLong.of(1L));
        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList1.chunks()).isEmpty();
        assertEquals(chunkList1.nextPagingId(), OptionalLong.of(1L));

        chunkManager.pingExchange(EXCHANGE_0);

        ticker.increment(DEFAULT_EXCHANGE_STALENESS_THRESHOLD.toMillis() - 500, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();

        chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.of(1L));
        assertThat(chunkList0.chunks()).isEmpty();
        assertEquals(chunkList0.nextPagingId(), OptionalLong.of(2L));
        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_1));
    }

    @Test
    public void testDataPageIdDeduplication()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(30, BYTE));

        chunkManager.addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("chunk"));
        chunkManager.addDataPage(EXCHANGE_0, 0, 0, 0, 1L, utf8Slice("manager"));
        chunkManager.addDataPage(EXCHANGE_0, 0, 0, 0, 1L, utf8Slice("manager"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 0, 2L, utf8Slice("data"));
        chunkManager.addDataPage(EXCHANGE_0, 1, 0, 0, 3L, utf8Slice("page"));
        assertThatThrownBy(() -> chunkManager.addDataPage(EXCHANGE_0, 0, 0, 0, 0L, utf8Slice("chunk")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dataPageId should not decrease for the same writer: taskId 0, attemptId 0, dataPageId 0, lastDataPageId 1");
        chunkManager.addDataPage(EXCHANGE_0, 1, 1, 0, 0L, utf8Slice("deduplication"));
        chunkManager.finishExchange(EXCHANGE_0);

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 12);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 8);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 13);

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("chunk")), new DataPage(0, 0, utf8Slice("manager")));
        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(0, 0, utf8Slice("data")), new DataPage(0, 0, utf8Slice("page")));
        assertThat(chunkManager.getChunkData(EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId(), BUFFER_NODE_ID)).containsExactlyInAnyOrder(
                new DataPage(1, 0, utf8Slice("deduplication")));

        chunkManager.removeExchange(EXCHANGE_0);
    }

    @Test
    public void testRemoveExchange()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(16, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_0);

        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.getChunkData(EXCHANGE_0, 1, 0, BUFFER_NODE_ID))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));
    }

    private ChunkManager createChunkManager(DataSize chunkSize)
    {
        ChunkManagerConfig config = new ChunkManagerConfig().setChunkSize(chunkSize);
        return new ChunkManager(BUFFER_NODE_ID, config, memoryAllocator, ticker, new DataServerStats());
    }
}
