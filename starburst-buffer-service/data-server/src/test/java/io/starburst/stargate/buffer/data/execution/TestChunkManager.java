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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.execution.ChunkManagerConfig.DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.verifyChunkData;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestChunkManager
{
    private static final String EXCHANGE_0 = "exchange-0";
    private static final String EXCHANGE_1 = "exchange-1";
    private static final long BUFFER_NODE_ID = 0;

    private final MemoryAllocator memoryAllocator = new MemoryAllocator(new MemoryAllocatorConfig(), new DataServerStats());
    private final TestingTicker ticker = new TestingTicker();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testSingleChunkPerPartition()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(16, MEGABYTE), DataSize.of(128, KILOBYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.registerExchange(EXCHANGE_1);

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 1, 0, 1L, ImmutableList.of(utf8Slice("001_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 2L, ImmutableList.of(utf8Slice("010_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 3L, ImmutableList.of(utf8Slice("011_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 1, 4L, ImmutableList.of(utf8Slice("010_0"), utf8Slice("010_1"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("100_0"))));

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 10);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 20);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 5);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(3, chunkManager.getOpenChunks());
        assertEquals(0, chunkManager.getClosedChunks());

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertTrue(chunkList0.chunks().isEmpty());
        assertEquals(OptionalLong.of(1L), chunkList0.nextPagingId());

        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty());
        assertTrue(chunkList1.chunks().isEmpty());
        assertEquals(OptionalLong.of(1L), chunkList1.nextPagingId());

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

        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("000_0")),
                new DataPage(1, 0, utf8Slice("001_0")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("010_0")),
                new DataPage(1, 0, utf8Slice("011_0")),
                new DataPage(0, 1, utf8Slice("010_0")),
                new DataPage(0, 1, utf8Slice("010_1")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("100_0")));

        assertEquals(DataSize.of(384, KILOBYTE).toBytes(), memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testMultipleChunksPerPartition()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(32, BYTE), DataSize.of(16, BYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.registerExchange(EXCHANGE_1);

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 1L, ImmutableList.of(utf8Slice("010_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 2L, ImmutableList.of(utf8Slice("011_0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 1, 3L, ImmutableList.of(utf8Slice("010_0"), utf8Slice("010_1"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("100_0"))));

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 10);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 10);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 5);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(3, chunkManager.getOpenChunks());
        assertEquals(1, chunkManager.getClosedChunks());

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle1);
        assertEquals(OptionalLong.of(1L), chunkList0.nextPagingId());

        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty());
        assertTrue(chunkList1.chunks().isEmpty());
        assertEquals(OptionalLong.of(1L), chunkList0.nextPagingId());

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

        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("000_0")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("010_0")),
                new DataPage(1, 0, utf8Slice("011_0")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 1, utf8Slice("010_0")),
                new DataPage(0, 1, utf8Slice("010_1")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_1, chunkHandle3.partitionId(), chunkHandle3.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("100_0")));

        assertEquals(DataSize.of(96, BYTE).toBytes(), memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testPingExchange()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(16, MEGABYTE), DataSize.of(1, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);

        ticker.increment(1000, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).isEmpty();
        assertEquals(OptionalLong.of(1L), chunkList0.nextPagingId());
        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList1.chunks()).isEmpty();
        assertEquals(OptionalLong.of(1L), chunkList1.nextPagingId());

        chunkManager.pingExchange(EXCHANGE_0);

        ticker.increment(DEFAULT_EXCHANGE_STALENESS_THRESHOLD.toMillis() - 500, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();

        chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.of(1L));
        assertThat(chunkList0.chunks()).isEmpty();
        assertEquals(OptionalLong.of(2L), chunkList0.nextPagingId());
        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_1));

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testDataPagesIdDeduplication()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(30, BYTE), DataSize.of(10, BYTE));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("chunk"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("manager"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("manager"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 2L, ImmutableList.of(utf8Slice("data"), utf8Slice("page"))));
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("chunk")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dataPagesId should not decrease for the same writer: taskId 0, attemptId 0, dataPagesId 0, lastDataPagesId 1");
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 0L, ImmutableList.of(utf8Slice("deduplication"))));
        chunkManager.finishExchange(EXCHANGE_0);

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 12);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 8);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 13);

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("chunk")), new DataPage(0, 0, utf8Slice("manager")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId(), BUFFER_NODE_ID),
                new DataPage(0, 0, utf8Slice("data")), new DataPage(0, 0, utf8Slice("page")));
        verifyChunkData(chunkManager.getChunkData(EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId(), BUFFER_NODE_ID),
                new DataPage(1, 0, utf8Slice("deduplication")));

        chunkManager.removeExchange(EXCHANGE_0);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testRemoveExchange()
    {
        ChunkManager chunkManager = createChunkManager(DataSize.of(16, MEGABYTE), DataSize.of(4, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_0);

        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.getChunkData(EXCHANGE_0, 1, 0, BUFFER_NODE_ID))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testAddDataPagesFailure()
            throws InterruptedException
    {
        DataSize chunkMaxSize = DataSize.of(12, BYTE);
        ChunkManager chunkManager = createChunkManager(chunkMaxSize, chunkMaxSize);
        Slice largePage = utf8Slice("8".repeat((int) DataSize.of(8, MEGABYTE).toBytes()));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy"))));
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("dummy"), largePage))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("requiredStorageSize %d larger than chunkMaxSizeInBytes %d".formatted(DATA_PAGE_HEADER_SIZE + largePage.length(), chunkMaxSize.toBytes()));
        Thread.sleep(10); // make sure exception callback has executed and failure has been set
        // all future operations (except releaseChunks) to the exchange will fail
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 2, 2, 2, 2L, ImmutableList.of(utf8Slice("dummy")))))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s is in inconsistent state".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s is in inconsistent state".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.finishExchange(EXCHANGE_0))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s is in inconsistent state".formatted(EXCHANGE_0));
    }

    private ChunkManager createChunkManager(DataSize chunkMaxSize, DataSize chunkSliceSize)
    {
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig().setChunkMaxSize(chunkMaxSize).setChunkSliceSize(chunkSliceSize);
        DataServerConfig dataServerConfig = new DataServerConfig().setIncludeChecksumInDataResponse(true);
        return new ChunkManager(
                new BufferNodeId(BUFFER_NODE_ID),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                ticker,
                new DataServerStats(),
                executor);
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
    }
}
