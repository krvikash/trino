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

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.execution.ChunkManagerConfig.DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.verifyChunkData;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpoolingStorage;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.awaitility.Durations.ONE_SECOND;

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
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(16, MEGABYTE), DataSize.of(128, KILOBYTE));

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
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(32, BYTE), DataSize.of(16, BYTE));

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
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(16, MEGABYTE), DataSize.of(1, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.registerExchange(EXCHANGE_1);
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy"))));

        ticker.increment(1000, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();
        assertEquals(DataSize.of(1, MEGABYTE).toBytes(), memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory());

        ChunkList chunkList0 = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList0.chunks()).isEmpty();
        assertEquals(OptionalLong.of(1L), chunkList0.nextPagingId());
        ChunkList chunkList1 = chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty());
        assertThat(chunkList1.chunks()).isEmpty();
        assertEquals(OptionalLong.of(1L), chunkList1.nextPagingId());

        ticker.increment(1000, MILLISECONDS);
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
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(30, BYTE), DataSize.of(10, BYTE));

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
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(16, MEGABYTE), DataSize.of(4, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0);
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
            throws InterruptedException
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        DataSize chunkMaxSize = DataSize.of(12, BYTE);
        ChunkManager chunkManager = createChunkManager(memoryAllocator, chunkMaxSize, chunkMaxSize);
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

    @Test
    public void testSpoolChunks()
    {
        long maxBytes = 96L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(0.8)
                        .setAllocationRatioLowWatermark(0.5),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(64, BYTE), DataSize.of(32, BYTE));

        chunkManager.registerExchange(EXCHANGE_0);
        chunkManager.registerExchange(EXCHANGE_1);

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("test"), utf8Slice("spool"), utf8Slice("chunks")));
        await().atMost(ONE_SECOND).until(addDataPagesFuture1::isDone);
        assertEquals(32, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("add"), utf8Slice("data"), utf8Slice("pages")));
        await().atMost(ONE_SECOND).until(addDataPagesFuture2::isDone);
        assertEquals(0, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_1, 1, 1, 1, 2L, ImmutableList.of(utf8Slice("dummy")));
        assertFalse(addDataPagesFuture3.isDone()); // no memory available yet

        chunkManager.spoolIfNecessary();
        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(2, chunkManager.getSpooledChunks()); // the open chunk should have spooled too

        await().atMost(ONE_SECOND).until(addDataPagesFuture3::isDone);
        assertEquals(64, memoryAllocator.getFreeMemory());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 22);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 5);
        assertThat(chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()).chunks())
                .containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 0, utf8Slice("spool")),
                new DataPage(0, 0, utf8Slice("chunks")),
                new DataPage(0, 0, utf8Slice("add")),
                new DataPage(0, 0, utf8Slice("data")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("pages")));

        chunkManager.finishExchange(EXCHANGE_0);
        chunkManager.finishExchange(EXCHANGE_1);

        ListenableFuture<Slice> sliceFuture = memoryAllocator.allocate(64);
        await().atMost(ONE_SECOND).until(sliceFuture::isDone);

        chunkManager.spoolIfNecessary(); // only one closed chunk can be spooled at this point
        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(3, chunkManager.getSpooledChunks());
        assertEquals(32, memoryAllocator.getFreeMemory());
        memoryAllocator.release(getFutureValue(sliceFuture));

        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 5);
        assertThat(chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty()).chunks())
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
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(16, BYTE), DataSize.of(8, BYTE));

        chunkManager.registerExchange(EXCHANGE_0);

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("a"), utf8Slice("b"), utf8Slice("c")));
        await().atMost(ONE_SECOND).until(addDataPagesFuture1::isDone);
        assertEquals(40, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("d"), utf8Slice("e"), utf8Slice("f")));
        await().atMost(ONE_SECOND).until(addDataPagesFuture2::isDone);
        assertEquals(16, memoryAllocator.getFreeMemory());

        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_0, 2, 2, 2, 2L, ImmutableList.of(utf8Slice("g"), utf8Slice("h"), utf8Slice("i")));
        assertFalse(addDataPagesFuture3.isDone()); // not enough memory available yet

        // wait for all addDataPagesFutures to finish
        chunkManager.spoolIfNecessary();
        await().atMost(ONE_SECOND).until(addDataPagesFuture3::isDone);

        Future<Integer> numClosedChunksFuture = executor.submit(() -> {
            OptionalLong pagingId = OptionalLong.empty();
            int numChunks = 0;
            for (int i = 0; i < 10; ++i) {
                ChunkList chunkList = chunkManager.listClosedChunks(EXCHANGE_0, pagingId);
                pagingId = chunkList.nextPagingId();
                numChunks += chunkList.chunks().size();
                if (pagingId.isEmpty()) {
                    chunkManager.markAllClosedChunksReceived(EXCHANGE_0);
                    return numChunks;
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

        chunkManager.drainAllChunks();

        assertTrue(numClosedChunksFuture.isDone());
        assertEquals(6, getFutureValue(numClosedChunksFuture));
        assertEquals(0, chunkManager.getClosedChunks());
        assertEquals(6, chunkManager.getSpooledChunks());
        assertEquals(maxBytes, memoryAllocator.getFreeMemory());

        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 3, 3, 3, 3L, ImmutableList.of(utf8Slice("dummy")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("addDataPages called in ChunkManager after we started draining");
    }

    @Test
    public void testAddToRemovedExchange()
    {
        ChunkManager chunkManager = createChunkManager(defaultMemoryAllocator(), DataSize.of(16, MEGABYTE), DataSize.of(128, KILOBYTE));

        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))));
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(1);

        chunkManager.removeExchange(EXCHANGE_0);
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0")))))
                .isInstanceOf(DataServerException.class)
                .matches(t -> (((DataServerException) t).getErrorCode()) == ErrorCode.EXCHANGE_NOT_FOUND)
                .hasMessage("exchange %s already removed".formatted(EXCHANGE_0));
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
    }

    @Test
    public void testListClosedChunks()
    {
        ChunkManager chunkManager = createChunkManager(defaultMemoryAllocator(), DataSize.of(12, BYTE), DataSize.of(12, BYTE));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("page0"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("page1"))));
        OptionalLong pagingId = OptionalLong.empty();
        ChunkList chunkList = chunkManager.listClosedChunks(EXCHANGE_0, pagingId); // only chunk 0 closed at this point
        pagingId = chunkList.nextPagingId();
        assertTrue(pagingId.isPresent());
        assertEquals(1L, pagingId.getAsLong());
        assertThat(chunkList.chunks()).containsExactly(new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(utf8Slice("page2"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 3L, ImmutableList.of(utf8Slice("page3"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 4L, ImmutableList.of(utf8Slice("page4"))));
        chunkList = chunkManager.listClosedChunks(EXCHANGE_0, pagingId); // chunk 1, 2, 3 are newly closed
        pagingId = chunkList.nextPagingId();
        assertTrue(pagingId.isPresent());
        assertEquals(2L, pagingId.getAsLong());
        assertThat(chunkList.chunks()).containsExactly(
                new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 5));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 5L, ImmutableList.of(utf8Slice("page5"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 6L, ImmutableList.of(utf8Slice("page6"))));
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 7L, ImmutableList.of(utf8Slice("page7"))));
        chunkManager.finishExchange(EXCHANGE_0);
        chunkList = chunkManager.listClosedChunks(EXCHANGE_0, pagingId); // chunk 4, 5, 6, 7 are newly closed
        pagingId = chunkList.nextPagingId();
        assertTrue(pagingId.isEmpty());
        assertThat(chunkList.chunks()).containsExactly(
                new ChunkHandle(BUFFER_NODE_ID, 0, 4L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 5L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 6L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 7L, 5));
    }

    @Test
    public void testIgnoreCancellationException()
    {
        long maxBytes = 16L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setAllocationRatioHighWatermark(1.0)
                        .setAllocationRatioLowWatermark(1.0),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(memoryAllocator, DataSize.of(8, BYTE), DataSize.of(8, BYTE));

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("1")));
        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(utf8Slice("2")));
        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 3L, ImmutableList.of(utf8Slice("3")));

        await().atMost(ONE_SECOND).until(addDataPagesFuture1::isDone);
        await().atMost(ONE_SECOND).until(addDataPagesFuture2::isDone);
        assertFalse(addDataPagesFuture3.isDone());

        chunkManager.finishExchange(EXCHANGE_0);
        assertTrue(addDataPagesFuture3.isCancelled());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 1);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 1);

        ChunkList chunkList = chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty());
        assertThat(chunkList.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("1")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("2")));
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
        if (chunkDataResult.chunkDataHolder().isPresent()) {
            verifyChunkData(chunkDataResult.chunkDataHolder().get(), values);
        }
        else {
            assertTrue(chunkDataResult.spoolingFile().isPresent());
            List<DataPage> dataPages = getFutureValue(spooledChunkReader.getDataPages(chunkDataResult.spoolingFile().get()));
            assertThat(dataPages).containsExactlyInAnyOrder(values);
        }
    }

    private MemoryAllocator defaultMemoryAllocator()
    {
        return new MemoryAllocator(new MemoryAllocatorConfig(), new DataServerStats());
    }

    private ChunkManager createChunkManager(
            MemoryAllocator memoryAllocator,
            DataSize chunkMaxSize,
            DataSize chunkSliceSize)
    {
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig()
                .setChunkMaxSize(chunkMaxSize)
                .setChunkSliceSize(chunkSliceSize)
                .setSpoolingDirectories("s3://" + minioStorage.getBucketName())
                .setChunkSpoolInterval(succinctDuration(100, SECONDS)); // only manual triggering in tests
        DataServerConfig dataServerConfig = new DataServerConfig().setDataIntegrityVerificationEnabled(true);
        return new ChunkManager(
                new BufferNodeId(BUFFER_NODE_ID),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                spoolingStorage,
                ticker,
                new DataServerStats(),
                executor);
    }
}
