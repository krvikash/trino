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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.MinioStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.execution.ChunkManagerConfig.DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.toChunkDataLease;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.verifyChunkData;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpoolingStorage;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestChunkManager
{
    private static final Logger log = Logger.get(TestChunkManager.class);

    private static final String EXCHANGE_0 = "exchange-0";
    private static final String EXCHANGE_1 = "exchange-1";
    private static final long BUFFER_NODE_ID = 0;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final TestingTicker ticker = new TestingTicker();
    private MinioStorage minioStorage;
    private SpoolingStorage spoolingStorage;
    private SpooledChunkReader spooledChunkReader;

    @BeforeAll
    public void init()
    {
        this.minioStorage = new MinioStorage("spooling-storage-" + randomUUID());
        minioStorage.start();

        this.spoolingStorage = createS3SpoolingStorage(minioStorage);
        this.spooledChunkReader = createS3SpooledChunkReader(minioStorage, executor);
    }

    @Test
    public void testSingleChunkPerPartition()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);
        chunkManager.registerExchange(EXCHANGE_1, STANDARD);

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 1, 0, 1L, ImmutableList.of(utf8Slice("001_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 2L, ImmutableList.of(utf8Slice("010_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 3L, ImmutableList.of(utf8Slice("011_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 1, 4L, ImmutableList.of(utf8Slice("010_0"), utf8Slice("010_1"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("100_0"))).addDataPagesFuture());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 10);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 20);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 5);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(3, chunkManager.getOpenChunks());
        assertEquals(0, chunkManager.getClosedChunks());

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 0);
        assertTrue(chunkList0.chunks().isEmpty());
        assertTrue(chunkList0.nextPagingId().isPresent());

        ChunkList chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 0);
        assertTrue(chunkList1.chunks().isEmpty());
        assertTrue(chunkList1.nextPagingId().isPresent());

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        getFutureValue(chunkManager.finishExchange(EXCHANGE_1));

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(0, chunkManager.getOpenChunks());
        assertEquals(3, chunkManager.getClosedChunks());

        chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, chunkList0.nextPagingId(), 2);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, chunkList1.nextPagingId(), 1);
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle2);
        assertTrue(chunkList1.nextPagingId().isEmpty());

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("000_0")),
                new DataPage(1, 0, utf8Slice("001_0")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("010_0")),
                new DataPage(1, 0, utf8Slice("011_0")),
                new DataPage(0, 1, utf8Slice("010_0")),
                new DataPage(0, 1, utf8Slice("010_1")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(0, 0, utf8Slice("100_0")));

        assertEquals(DataSize.of(384, KILOBYTE).toBytes(), memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testMultipleChunksPerPartition()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(32, BYTE),
                DataSize.of(128, BYTE),
                DataSize.of(16, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);
        chunkManager.registerExchange(EXCHANGE_1, STANDARD);

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 1L, ImmutableList.of(utf8Slice("010_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 2L, ImmutableList.of(utf8Slice("011_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 1, 3L, ImmutableList.of(utf8Slice("010_0"), utf8Slice("010_1"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("100_0"))).addDataPagesFuture());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 10);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 10);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 5);

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(3, chunkManager.getOpenChunks());
        assertEquals(1, chunkManager.getClosedChunks());

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 1);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle1);
        assertTrue(chunkList0.nextPagingId().isPresent());

        ChunkList chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 0);
        assertTrue(chunkList1.chunks().isEmpty());
        assertTrue(chunkList1.nextPagingId().isPresent());

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        getFutureValue(chunkManager.finishExchange(EXCHANGE_1));

        assertEquals(2, chunkManager.getTrackedExchanges());
        assertEquals(0, chunkManager.getOpenChunks());
        assertEquals(4, chunkManager.getClosedChunks());

        chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, chunkList0.nextPagingId(), 2);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle2);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, chunkList1.nextPagingId(), 1);
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle3);
        assertTrue(chunkList1.nextPagingId().isEmpty());

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("000_0")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("010_0")),
                new DataPage(1, 0, utf8Slice("011_0")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(0, 1, utf8Slice("010_0")),
                new DataPage(0, 1, utf8Slice("010_1")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle3.partitionId(), chunkHandle3.chunkId()),
                new DataPage(0, 0, utf8Slice("100_0")));

        assertEquals(DataSize.of(96, BYTE).toBytes(), memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testPingExchange()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(1, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);
        chunkManager.registerExchange(EXCHANGE_1, STANDARD);
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture());

        ticker.increment(1000, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();
        assertEquals(DataSize.of(1, MEGABYTE).toBytes(), memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 0);
        assertThat(chunkList0.chunks()).isEmpty();
        assertTrue(chunkList0.nextPagingId().isPresent());

        ChunkList chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 0);
        assertThat(chunkList1.chunks()).isEmpty();
        assertTrue(chunkList1.nextPagingId().isPresent());

        ticker.increment(1000, MILLISECONDS);
        chunkManager.pingExchange(EXCHANGE_0);

        ticker.increment(DEFAULT_EXCHANGE_STALENESS_THRESHOLD.toMillis() - 500, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();

        chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, chunkList0.nextPagingId(), 0);
        assertThat(chunkList0.chunks()).isEmpty();
        assertTrue(chunkList0.nextPagingId().isPresent());
        assertThatThrownBy(() -> getFutureValue(chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty())))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_1));

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testDataPagesIdDeduplication()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(30, BYTE),
                DataSize.of(120, BYTE),
                DataSize.of(10, BYTE));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("chunk"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("manager"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("manager"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 2L, ImmutableList.of(utf8Slice("data"), utf8Slice("page"))).addDataPagesFuture());
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("chunk"))).addDataPagesFuture()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dataPagesId should not decrease for the same writer: taskId 0, attemptId 0, dataPagesId 0, lastDataPagesId 1");
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 0L, ImmutableList.of(utf8Slice("deduplication"))).addDataPagesFuture());
        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 12);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 8);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 13);

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 3);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2);
        assertTrue(chunkList0.nextPagingId().isEmpty());

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("chunk")), new DataPage(0, 0, utf8Slice("manager")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("data")), new DataPage(0, 0, utf8Slice("page")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(1, 0, utf8Slice("deduplication")));

        chunkManager.removeExchange(EXCHANGE_0);

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testRemoveExchange()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(4, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);
        chunkManager.removeExchange(EXCHANGE_0);

        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, 1, 0))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));

        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @Test
    public void testAddDataPagesFailure()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        DataSize chunkTargetSize = DataSize.of(12, BYTE);
        DataSize chunkMaxSize = DataSize.of(48, BYTE);
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                chunkTargetSize,
                chunkMaxSize,
                chunkTargetSize);
        Slice largePage = utf8Slice("8".repeat((int) DataSize.of(8, MEGABYTE).toBytes()));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture());
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("dummy"), largePage)).addDataPagesFuture()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("requiredStorageSize %d larger than chunkMaxSizeInBytes %d".formatted(DATA_PAGE_HEADER_SIZE + largePage.length(), chunkMaxSize.toBytes()));
        sleepUninterruptibly(100, MILLISECONDS); // make sure exception callback has executed and failure has been set
        // all future operations (except releaseChunks) to the exchange will fail
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 2, 2, 2, 2L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s is in inconsistent state".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s is in inconsistent state".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> getFutureValue(chunkManager.finishExchange(EXCHANGE_0)))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s is in inconsistent state".formatted(EXCHANGE_0));
    }

    @Test
    public void testSpoolChunks()
    {
        long maxBytes = 96L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(0.8)
                        .setAllocationRatioLowWatermark(0.5),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(64, BYTE),
                DataSize.of(256, BYTE),
                DataSize.of(32, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);
        chunkManager.registerExchange(EXCHANGE_1, STANDARD);

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("test"), utf8Slice("spool"), utf8Slice("chunks"))).addDataPagesFuture();
        await().atMost(ONE_SECOND).until(addDataPagesFuture1::isDone);
        assertEquals(32, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("add"), utf8Slice("data"), utf8Slice("pages"))).addDataPagesFuture();
        await().atMost(ONE_SECOND).until(addDataPagesFuture2::isDone);
        assertEquals(0, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_1, 1, 1, 1, 2L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture();
        assertFalse(addDataPagesFuture3.isDone()); // no memory available yet

        chunkManager.spoolIfNecessary();
        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(2, chunkManager.getSpooledChunks()); // the open chunk should have spooled too

        await().atMost(ONE_SECOND).until(addDataPagesFuture3::isDone);
        assertEquals(64, memoryAllocator.getFreeMemory());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 22);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 5);
        assertThat(listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 2).chunks())
                .containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 0, utf8Slice("spool")),
                new DataPage(0, 0, utf8Slice("chunks")),
                new DataPage(0, 0, utf8Slice("add")),
                new DataPage(0, 0, utf8Slice("data")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("pages")));

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        getFutureValue(chunkManager.finishExchange(EXCHANGE_1));

        ListenableFuture<Slice> sliceFuture = memoryAllocator.allocate(64);
        await().atMost(ONE_SECOND).until(sliceFuture::isDone);

        chunkManager.spoolIfNecessary(); // only one closed chunk can be spooled at this point
        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(3, chunkManager.getSpooledChunks());
        assertEquals(32, memoryAllocator.getFreeMemory());
        memoryAllocator.release(getFutureValue(sliceFuture));

        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 5);
        assertThat(listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 1).chunks())
                .containsExactlyInAnyOrder(chunkHandle2);
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(1, 1, utf8Slice("dummy")));

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(0, chunkManager.getSpooledChunks());
        assertEquals(96, memoryAllocator.getFreeMemory());
    }

    @Test
    public void testDrainAllChunks()
    {
        long maxBytes = 64L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(0.8)
                        .setAllocationRatioLowWatermark(0.5),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(8, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("a"), utf8Slice("b"), utf8Slice("c"))).addDataPagesFuture();
        await().atMost(ONE_SECOND).until(addDataPagesFuture1::isDone);
        assertEquals(40, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("d"), utf8Slice("e"), utf8Slice("f"))).addDataPagesFuture();
        await().atMost(ONE_SECOND).until(addDataPagesFuture2::isDone);
        assertEquals(16, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_0, 2, 2, 2, 2L, ImmutableList.of(utf8Slice("g"), utf8Slice("h"), utf8Slice("i"))).addDataPagesFuture();
        assertFalse(addDataPagesFuture3.isDone()); // not enough memory available yet

        // wait for all addDataPagesFutures to finish
        chunkManager.spoolIfNecessary();
        await().atMost(ONE_SECOND).until(addDataPagesFuture3::isDone);

        Future<Integer> numClosedChunksFuture = executor.submit(() -> {
            OptionalLong pagingId = OptionalLong.empty();
            int numChunks = 0;
            for (int i = 0; i < 10; ++i) {
                ChunkList chunkList = getFutureValue(chunkManager.listClosedChunks(EXCHANGE_0, pagingId));
                pagingId = chunkList.nextPagingId();
                numChunks += chunkList.chunks().size();
                if (pagingId.isEmpty()) {
                    chunkManager.markAllClosedChunksReceived(EXCHANGE_0);
                    return numChunks;
                }

                sleepUninterruptibly(100, MILLISECONDS);
            }
            return fail();
        });

        chunkManager.drainAllChunks();

        assertTrue(numClosedChunksFuture.isDone());
        assertEquals(6, getFutureValue(numClosedChunksFuture));
        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(6, chunkManager.getSpooledChunks());
        assertEquals(maxBytes, memoryAllocator.getFreeMemory());

        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 3, 3, 3, 3L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("addDataPages called in ChunkManager after we started draining");
    }

    @Test
    public void testRegisterExchangeWhileDraining()
    {
        long maxBytes = 64L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(0.8)
                        .setAllocationRatioLowWatermark(0.5),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(8, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD);
        assertFalse(chunkManager.getExchangeAndHeartbeat(EXCHANGE_0).isFinished());

        Future<?> drainAllChunksFuture = executor.submit(chunkManager::drainAllChunks);

        // wait until chunkManager::drainAllChunks finishes all existing exchanges
        waitAtMost(1, SECONDS).until(() -> chunkManager.getExchangeAndHeartbeat(EXCHANGE_0).isFinished());
        // it is waiting for markAllClosedChunksReceived on all exchanges now

        // register one more exchange
        chunkManager.registerExchange(EXCHANGE_1, STANDARD);

        // finish should be triggered on new exchange too
        waitAtMost(1, SECONDS).until(() -> chunkManager.getExchangeAndHeartbeat(EXCHANGE_1).isFinished());

        // we should get information that there are no more chunks for both exchanges
        listClosedChunkUntilNoMore(chunkManager, EXCHANGE_0, OptionalLong.empty());
        listClosedChunkUntilNoMore(chunkManager, EXCHANGE_1, OptionalLong.empty());

        // mark all chunks received (simulate Trino behavior)
        chunkManager.markAllClosedChunksReceived(EXCHANGE_0);
        chunkManager.markAllClosedChunksReceived(EXCHANGE_1);

        // drainAllChunk should complete timely
        assertThat(drainAllChunksFuture).succeedsWithin(5, SECONDS);
    }

    @Test
    public void testAddToRemovedExchange()
    {
        ChunkManager chunkManager = createChunkManager(
                defaultMemoryAllocator(),
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))).addDataPagesFuture());
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(1);

        chunkManager.removeExchange(EXCHANGE_0);
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))).addDataPagesFuture()))
                .isInstanceOf(DataServerException.class)
                .matches(t -> (((DataServerException) t).getErrorCode()) == ErrorCode.EXCHANGE_NOT_FOUND)
                .hasMessage("exchange %s already removed".formatted(EXCHANGE_0));
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
    }

    @Test
    public void testListClosedChunks()
    {
        ChunkManager chunkManager = createChunkManager(
                defaultMemoryAllocator(),
                DataSize.of(12, BYTE),
                DataSize.of(48, BYTE),
                DataSize.of(12, BYTE));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("page0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("page1"))).addDataPagesFuture());
        OptionalLong pagingId = OptionalLong.empty();
        ChunkList chunkList = listClosedChunks(chunkManager, EXCHANGE_0, pagingId, 1); // only chunk 0 closed at this point
        pagingId = chunkList.nextPagingId();
        assertTrue(pagingId.isPresent());
        assertThat(chunkList.chunks()).containsExactly(new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(utf8Slice("page2"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 3L, ImmutableList.of(utf8Slice("page3"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 4L, ImmutableList.of(utf8Slice("page4"))).addDataPagesFuture());
        chunkList = listClosedChunks(chunkManager, EXCHANGE_0, pagingId, 3); // chunk 1, 2, 3 are newly closed
        pagingId = chunkList.nextPagingId();
        assertTrue(pagingId.isPresent());
        assertThat(chunkList.chunks()).containsExactly(
                new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 5));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 5L, ImmutableList.of(utf8Slice("page5"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 6L, ImmutableList.of(utf8Slice("page6"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 7L, ImmutableList.of(utf8Slice("page7"))).addDataPagesFuture());
        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        chunkList = listClosedChunks(chunkManager, EXCHANGE_0, pagingId, 4); // chunk 4, 5, 6, 7 are newly closed
        pagingId = chunkList.nextPagingId();
        assertTrue(pagingId.isEmpty());
        assertThat(chunkList.chunks()).containsExactly(
                new ChunkHandle(BUFFER_NODE_ID, 0, 4L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 5L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 6L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 7L, 5));
    }

    @Test
    public void testWaitForInProgressAddDataPages()
    {
        long maxBytes = 16L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(1.0)
                        .setAllocationRatioLowWatermark(1.0),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(8, BYTE),
                DataSize.of(32, BYTE),
                DataSize.of(8, BYTE));
        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("1"))).addDataPagesFuture();
        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(utf8Slice("2"))).addDataPagesFuture();
        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 3L, ImmutableList.of(utf8Slice("3"))).addDataPagesFuture();
        ListenableFuture<Void> addDataPagesFuture4 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 4L, ImmutableList.of(utf8Slice("4"))).addDataPagesFuture();
        await().atMost(ONE_SECOND).until(addDataPagesFuture1::isDone);
        await().atMost(ONE_SECOND).until(addDataPagesFuture2::isDone);
        assertFalse(addDataPagesFuture3.isDone());
        assertFalse(addDataPagesFuture4.isDone());

        ListenableFuture<Void> exchangeFinishFuture = chunkManager.finishExchange(EXCHANGE_0);
        assertFalse(exchangeFinishFuture.isDone());
        assertFalse(addDataPagesFuture3.isDone()); // only wait for the in-progress addDataPagesFuture
        assertTrue(addDataPagesFuture4.isDone()); // rest of the addDataPagesFutures should complete early

        chunkManager.spoolIfNecessary();
        await().atMost(ONE_SECOND).until(addDataPagesFuture3::isDone);
        await().atMost(ONE_SECOND).until(exchangeFinishFuture::isDone);

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 1);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 1);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 1);

        ChunkList chunkList = getFutureValue(chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()));
        assertThat(chunkList.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2);

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("1")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("2")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(0, 0, utf8Slice("3")));
    }

    @Test
    public void testGetDrainedChunkDataOnAnotherNode()
    {
        long drainedBufferNodeId = BUFFER_NODE_ID + 1;
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 1, utf8Slice("Spooling")),
                new DataPage(1, 0, utf8Slice("Storage")));
        getFutureValue(spoolingStorage.writeChunk(drainedBufferNodeId, EXCHANGE_0, 0L, toChunkDataLease(dataPages)));

        ChunkManager chunkManager = createChunkManager(
                defaultMemoryAllocator(),
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        // exchange missing
        assertDrainedChunkDataResult(chunkManager, drainedBufferNodeId);

        // exchange exists, but partition missing
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("dummy1"))).addDataPagesFuture());
        assertDrainedChunkDataResult(chunkManager, drainedBufferNodeId);

        // exchange exists, partition exists, but chunk missing
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy0"))).addDataPagesFuture());
        assertDrainedChunkDataResult(chunkManager, drainedBufferNodeId);

        chunkManager.removeExchange(EXCHANGE_0);
    }

    @Test
    public void testAddDataPagesShouldRetainMemory()
    {
        long maxBytes = 16L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(1.0)
                        .setAllocationRatioLowWatermark(1.0),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        AddDataPagesResult addDataPagesResult = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy")));
        assertFalse(addDataPagesResult.addDataPagesFuture().isDone()); // addDataPagesResult will block because of memory not enough
        assertTrue(addDataPagesResult.shouldRetainMemory());

        AddDataPagesResult retriedAddDataPagesResult = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy")));
        assertFalse(retriedAddDataPagesResult.addDataPagesFuture().isDone());
        assertEquals(addDataPagesResult.addDataPagesFuture(), retriedAddDataPagesResult.addDataPagesFuture()); // retry should get the same future
        assertFalse(retriedAddDataPagesResult.shouldRetainMemory()); // retry shouldn't retain memory for input data

        addDataPagesResult.addDataPagesFuture().cancel(true);
        assertTrue(retriedAddDataPagesResult.addDataPagesFuture().isCancelled());
    }

    @Test
    public void testWritePagesOfDifferentSizes()
    {
        Slice tinyPage = utf8Slice("0");
        Slice largePage = utf8Slice("2".repeat(40));
        Slice hugePage = utf8Slice("3".repeat(100));

        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator,
                DataSize.of(16, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(8, BYTE));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(tinyPage)).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(tinyPage)).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(largePage)).addDataPagesFuture());

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 0L, ImmutableList.of(largePage)).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(tinyPage)).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 2L, ImmutableList.of(tinyPage)).addDataPagesFuture());

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 2, 2, 2, 0L, ImmutableList.of(tinyPage)).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 2, 2, 2, 1L, ImmutableList.of(largePage)).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 2, 2, 2, 2L, ImmutableList.of(tinyPage)).addDataPagesFuture());

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 2);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 40);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 3L, 40);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 1, 4L, 2);
        ChunkHandle chunkHandle4 = new ChunkHandle(BUFFER_NODE_ID, 2, 5L, 1);
        ChunkHandle chunkHandle5 = new ChunkHandle(BUFFER_NODE_ID, 2, 6L, 40);
        ChunkHandle chunkHandle6 = new ChunkHandle(BUFFER_NODE_ID, 2, 7L, 1);

        ChunkList chunkList = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 7);
        assertThat(chunkList.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2, chunkHandle3, chunkHandle4, chunkHandle5, chunkHandle6);

        assertEquals(192, memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 3, 3, 3, 0L, ImmutableList.of(hugePage)).addDataPagesFuture()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("requiredStorageSize 107 larger than chunkMaxSizeInBytes 64");

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);
        assertEquals(memoryAllocator.getTotalMemory(), memoryAllocator.getFreeMemory());
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
        if (spoolingStorage != null) {
            try {
                spoolingStorage.close();
            }
            catch (Exception e) {
                log.error(e, "Error closing spoolingStorage");
            }
            spoolingStorage = null;
        }
        if (minioStorage != null) {
            try {
                minioStorage.close();
            }
            catch (Exception e) {
                log.error(e, "Error closing minioStorage");
            }
            minioStorage = null;
        }
    }

    private void verifyChunkDataResult(ChunkDataResult chunkDataResult, DataPage... values)
    {
        if (chunkDataResult.chunkDataLease().isPresent()) {
            verifyChunkData(chunkDataResult.chunkDataLease().get(), values);
        }
        else {
            assertTrue(chunkDataResult.spooledChunk().isPresent());
            List<DataPage> dataPages = getFutureValue(spooledChunkReader.getDataPages(chunkDataResult.spooledChunk().get()));
            assertThat(dataPages).containsExactlyInAnyOrder(values);
        }
    }

    private MemoryAllocator defaultMemoryAllocator()
    {
        return new MemoryAllocator(new MemoryAllocatorConfig(), new ChunkManagerConfig(), new DataServerStats());
    }

    private ChunkManager createChunkManager(
            MemoryAllocator memoryAllocator,
            DataSize chunkTargetSize,
            DataSize chunkMaxSize,
            DataSize chunkSliceSize)
    {
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig()
                .setChunkTargetSize(chunkTargetSize)
                .setChunkMaxSize(chunkMaxSize)
                .setChunkSliceSize(chunkSliceSize)
                .setSpoolingDirectory("s3://" + minioStorage.getBucketName())
                .setChunkSpoolInterval(succinctDuration(100, SECONDS)); // only manual triggering in tests
        DataServerConfig dataServerConfig = new DataServerConfig()
                .setDataIntegrityVerificationEnabled(true)
                .setMinDrainingDuration(succinctDuration(0, SECONDS)) // don't wait for extra time in tests
                // Reduce timeout here for calls when we expect zero results - we want those to return ASAP to reduce test duration
                .setChunkListPollTimeout(Duration.succinctDuration(5, MILLISECONDS));
        return new ChunkManager(
                new BufferNodeId(BUFFER_NODE_ID),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                spoolingStorage,
                ticker,
                new SpooledChunkMapByExchange(),
                new DataServerStats(),
                executor);
    }

    private List<ChunkHandle> listClosedChunkUntilNoMore(ChunkManager chunkManager, String exchangeId, OptionalLong pagingId)
    {
        List<ChunkHandle> chunkHandles = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            ChunkList chunkList = getFutureValue(chunkManager.listClosedChunks(exchangeId, pagingId));
            chunkHandles.addAll(chunkList.chunks());
            pagingId = chunkList.nextPagingId();

            if (pagingId.isEmpty()) {
                return chunkHandles;
            }

            sleepUninterruptibly(100, MILLISECONDS);
        }
        return fail();
    }

    private ChunkList listClosedChunks(ChunkManager chunkManager, String exchangeId, OptionalLong pagingId, int expectedChunkListSize)
    {
        List<ChunkHandle> chunkHandles = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            ChunkList chunkList = getFutureValue(chunkManager.listClosedChunks(exchangeId, pagingId));
            chunkHandles.addAll(chunkList.chunks());
            pagingId = chunkList.nextPagingId();

            if (chunkHandles.size() == expectedChunkListSize) {
                return new ChunkList(chunkHandles, pagingId);
            }
            if (chunkHandles.size() > expectedChunkListSize) {
                return fail(String.format("Expected chunkHandles.size()=%s to be less than or equal to %s; chunkHandles=%s", chunkHandles.size(), expectedChunkListSize, chunkHandles));
            }

            sleepUninterruptibly(100, MILLISECONDS);
        }
        return fail();
    }

    private void assertDrainedChunkDataResult(ChunkManager chunkManager, long drainedBufferNodeId)
    {
        ChunkDataResult chunkDataResult = chunkManager.getChunkData(drainedBufferNodeId, EXCHANGE_0, 0, 0L);
        assertTrue(chunkDataResult.spooledChunk().isPresent());
        assertEquals(52, chunkDataResult.spooledChunk().get().length());
        assertEquals("s3://" + minioStorage.getBucketName() + "/0a.exchange-0.1/0", chunkDataResult.spooledChunk().get().location());
    }
}
