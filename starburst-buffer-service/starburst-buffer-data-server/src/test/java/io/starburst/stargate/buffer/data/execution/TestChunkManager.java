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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.buffer.data.client.BufferNodeExchangeMetrics;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.TestingMemoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.gcs.GcsClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.MinioStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.execution.ChunkManagerConfig.DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.verifyChunkData;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpoolingStorage;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestChunkManager
{
    private static final Logger log = Logger.get(TestChunkManager.class);

    protected static final String EXCHANGE_0 = "exchange-0";
    protected static final String EXCHANGE_1 = "exchange-1";
    protected static final long BUFFER_NODE_ID = 0;

    protected final ExecutorService executor = Executors.newCachedThreadPool();
    protected final TestingTicker ticker = new TestingTicker();
    protected MinioStorage minioStorage;
    protected SpoolingStorage spoolingStorage;
    protected SpooledChunkReader spooledChunkReader;

    private static class FailureInjectingS3SpoolingStorage
            extends S3SpoolingStorage
    {
        private final Set<String> failureExchanges;

        public FailureInjectingS3SpoolingStorage(
                BufferNodeId bufferNodeId,
                SpoolingDirectoryConfig spoolingDirectoryConfig,
                S3AsyncClient s3AsyncClient,
                MergedFileNameGenerator mergedFileNameGenerator,
                DataServerStats dataServerStats,
                CompatibilityMode compatibilityMode,
                GcsClientConfig gcsClientConfig,
                Set<String> failureExchanges)
                throws IOException
        {
            super(bufferNodeId, spoolingDirectoryConfig, s3AsyncClient, mergedFileNameGenerator, dataServerStats, compatibilityMode, gcsClientConfig);
            this.failureExchanges = failureExchanges;
        }

        @Override
        protected ListenableFuture<Map<Long, SpooledChunk>> putStorageObject(String fileName, Map<Chunk, ChunkDataLease> chunkDataLeaseMap, long contentLength)
        {
            String exchangeId = chunkDataLeaseMap.keySet().stream().findFirst().get().getExchangeId();
            if (failureExchanges.contains(exchangeId)) {
                return Futures.immediateFailedFuture(new ExecutionException("Task did not complete", new IOException("Write failed")));
            }

            return super.putStorageObject(fileName, chunkDataLeaseMap, contentLength);
        }
    }

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
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 1, 0, 1L, ImmutableList.of(utf8Slice("001_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 2L, ImmutableList.of(utf8Slice("010_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 3L, ImmutableList.of(utf8Slice("011_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 1, 4L, ImmutableList.of(utf8Slice("010_0"), utf8Slice("010_1"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("100_0"))).addDataPagesFuture());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 10);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 20);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 5);

        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(2);
        assertThat(chunkManager.getOpenChunks()).isEqualTo(3);
        assertThat(chunkManager.getClosedChunks()).isEqualTo(0);

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 0);
        assertThat(chunkList0.chunks()).isEmpty();
        assertThat(chunkList0.nextPagingId()).isPresent();

        ChunkList chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 0);
        assertThat(chunkList1.chunks()).isEmpty();
        assertThat(chunkList1.nextPagingId()).isPresent();

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        getFutureValue(chunkManager.finishExchange(EXCHANGE_1));

        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(2);
        assertThat(chunkManager.getOpenChunks()).isEqualTo(0);
        assertThat(chunkManager.getClosedChunks()).isEqualTo(3);

        chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, chunkList0.nextPagingId(), 2);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1);
        assertThat(chunkList0.nextPagingId()).isEmpty();

        BufferNodeExchangeMetrics exchangeMetrics0 = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics0).isEqualTo(new BufferNodeExchangeMetrics(2, 2, 30, 0, 0, 2, 30));

        chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, chunkList1.nextPagingId(), 1);
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle2);
        assertThat(chunkList1.nextPagingId()).isEmpty();
        BufferNodeExchangeMetrics exchangeMetrics1 = chunkManager.pingExchange(EXCHANGE_1);
        assertThat(exchangeMetrics1).isEqualTo(new BufferNodeExchangeMetrics(1, 1, 5, 0, 0, 1, 5));

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

        assertThat(memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory()).isEqualTo(DataSize.of(384, KILOBYTE).toBytes());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(memoryAllocator.getTotalMemory());
    }

    @Test
    public void testMultipleChunksPerPartition()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(32, BYTE),
                DataSize.of(128, BYTE),
                DataSize.of(16, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("000_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 0, 1L, ImmutableList.of(utf8Slice("010_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 0, 2L, ImmutableList.of(utf8Slice("011_0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 0, 1, 3L, ImmutableList.of(utf8Slice("010_0"), utf8Slice("010_1"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("100_0"))).addDataPagesFuture());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 1, 1L, 10);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 10);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 0, 3L, 5);

        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(2);
        assertThat(chunkManager.getOpenChunks()).isEqualTo(3);
        assertThat(chunkManager.getClosedChunks()).isEqualTo(1);

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 1);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle1);
        assertThat(chunkList0.nextPagingId()).isPresent();

        ChunkList chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 0);
        assertThat(chunkList1.chunks()).isEmpty();
        assertThat(chunkList1.nextPagingId()).isPresent();

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        getFutureValue(chunkManager.finishExchange(EXCHANGE_1));

        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(2);
        assertThat(chunkManager.getOpenChunks()).isEqualTo(0);
        assertThat(chunkManager.getClosedChunks()).isEqualTo(4);

        chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, chunkList0.nextPagingId(), 2);
        assertThat(chunkList0.chunks()).containsExactlyInAnyOrder(chunkHandle0, chunkHandle2);
        assertThat(chunkList0.nextPagingId()).isEmpty();

        BufferNodeExchangeMetrics exchangeMetrics0 = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics0).isEqualTo(new BufferNodeExchangeMetrics(2, 3, 25, 0, 0, 3, 25));

        chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, chunkList1.nextPagingId(), 1);
        assertThat(chunkList1.chunks()).containsExactlyInAnyOrder(chunkHandle3);
        assertThat(chunkList1.nextPagingId()).isEmpty();

        BufferNodeExchangeMetrics exchangeMetrics1 = chunkManager.pingExchange(EXCHANGE_1);
        assertThat(exchangeMetrics1).isEqualTo(new BufferNodeExchangeMetrics(1, 1, 5, 0, 0, 1, 5));

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

        assertThat(memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory()).isEqualTo(DataSize.of(96, BYTE).toBytes());

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(memoryAllocator.getTotalMemory());
    }

    @Test
    public void testPingExchange()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(1, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture());

        ticker.increment(1000, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();
        assertThat((memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory())).isEqualTo(DataSize.of(1, MEGABYTE).toBytes());

        ChunkList chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, OptionalLong.empty(), 0);
        assertThat(chunkList0.chunks()).isEmpty();
        assertThat(chunkList0.nextPagingId()).isPresent();

        ChunkList chunkList1 = listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 0);
        assertThat(chunkList1.chunks()).isEmpty();
        assertThat(chunkList1.nextPagingId()).isPresent();

        ticker.increment(1000, MILLISECONDS);
        BufferNodeExchangeMetrics exchangeMetrics0 = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics0).isEqualTo(emptyMetrics());

        ticker.increment(DEFAULT_EXCHANGE_STALENESS_THRESHOLD.toMillis() - 500, MILLISECONDS);
        chunkManager.cleanupStaleExchanges();

        chunkList0 = listClosedChunks(chunkManager, EXCHANGE_0, chunkList0.nextPagingId(), 0);
        assertThat(chunkList0.chunks()).isEmpty();
        assertThat(chunkList0.nextPagingId()).isPresent();
        assertThatThrownBy(() -> getFutureValue(chunkManager.listClosedChunks(EXCHANGE_1, OptionalLong.empty())))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_1));

        exchangeMetrics0 = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics0).isEqualTo(emptyMetrics());

        Object expected = memoryAllocator.getTotalMemory();
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(expected);
    }

    @Test
    public void testDataPagesIdDeduplication()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
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
        assertThat(chunkList0.nextPagingId()).isEmpty();

        BufferNodeExchangeMetrics exchangeMetrics0 = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics0).isEqualTo(new BufferNodeExchangeMetrics(2, 3, 33, 0, 0, 3, 33));

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("chunk")), new DataPage(0, 0, utf8Slice("manager")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("data")), new DataPage(0, 0, utf8Slice("page")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(1, 0, utf8Slice("deduplication")));

        chunkManager.removeExchange(EXCHANGE_0);

        Object expected = memoryAllocator.getTotalMemory();
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(expected);
    }

    @Test
    public void testRemoveExchange()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(4, MEGABYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.removeExchange(EXCHANGE_0);

        assertThatThrownBy(() -> chunkManager.listClosedChunks(EXCHANGE_0, OptionalLong.empty()))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));
        assertThatThrownBy(() -> chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, 1, 0))
                .isInstanceOf(DataServerException.class)
                .hasMessage("exchange %s not found".formatted(EXCHANGE_0));

        Object expected = memoryAllocator.getTotalMemory();
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(expected);
    }

    @Test
    public void testAddDataPagesFailure()
    {
        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        DataSize chunkTargetSize = DataSize.of(12, BYTE);
        DataSize chunkMaxSize = DataSize.of(48, BYTE);
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
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
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(0.8)
                        .setSpoolingRatioLowWatermark(0.5),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(64, BYTE),
                DataSize.of(256, BYTE),
                DataSize.of(32, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("test"), utf8Slice("spool"), utf8Slice("chunks"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture1.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(32);

        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("add"), utf8Slice("data"), utf8Slice("pages"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture2.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(0);

        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_1, 1, 1, 1, 2L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture();
        assertThat(addDataPagesFuture3.isDone()).isFalse(); // no memory available yet

        chunkManager.spoolIfNecessary();
        assertThat(chunkManager.getClosedChunks()).isEqualTo(0);
        // the open chunk should have spooled too
        assertThat(chunkManager.getSpooledChunksCount()).isEqualTo(2);

        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture3.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(64);

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
        assertEventually(new Duration(1, SECONDS), () -> assertThat(sliceFuture.isDone()).isTrue());

        chunkManager.spoolIfNecessary(); // only one closed chunk can be spooled at this point
        assertThat(chunkManager.getClosedChunks()).isEqualTo(0);
        assertThat(chunkManager.getSpooledChunksCount()).isEqualTo(3);
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(32);
        memoryAllocator.release(getFutureValue(sliceFuture));

        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 5);
        assertThat(listClosedChunks(chunkManager, EXCHANGE_1, OptionalLong.empty(), 1).chunks())
                .containsExactlyInAnyOrder(chunkHandle2);

        BufferNodeExchangeMetrics exchangeMetrics1 = chunkManager.pingExchange(EXCHANGE_1);
        assertThat(exchangeMetrics1).isEqualTo(new BufferNodeExchangeMetrics(1, 0, 0, 1, 5, 1, 5));

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(1, 1, utf8Slice("dummy")));

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);

        assertThat(chunkManager.getClosedChunks()).isEqualTo(0);
        assertThat(chunkManager.getSpooledChunksCount()).isEqualTo(0);
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(96);
    }

    @Test
    public void testDrainAllChunks()
    {
        long maxBytes = 64L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(0.8)
                        .setSpoolingRatioLowWatermark(0.5),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(16, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(8, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());

        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("a"), utf8Slice("b"), utf8Slice("c"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture1.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(40);

        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("d"), utf8Slice("e"), utf8Slice("f"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture2.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(16);

        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_0, 2, 2, 2, 2L, ImmutableList.of(utf8Slice("g"), utf8Slice("h"), utf8Slice("i"))).addDataPagesFuture();
        assertThat(addDataPagesFuture3.isDone()).isFalse(); // not enough memory available yet

        // wait for all addDataPagesFutures to finish
        chunkManager.spoolIfNecessary();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture3.isDone()).isTrue());

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
            return abort();
        });

        chunkManager.drainAllChunks();

        assertThat(numClosedChunksFuture.isDone()).isTrue();
        assertThat(getFutureValue(numClosedChunksFuture)).isEqualTo(6);
        assertThat(chunkManager.getClosedChunks()).isEqualTo(0);
        assertThat(chunkManager.getSpooledChunksCount()).isEqualTo(6);
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(maxBytes);

        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 3, 3, 3, 3L, ImmutableList.of(utf8Slice("dummy"))).addDataPagesFuture()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("addDataPages called in ChunkManager after we started draining");
    }

    @Test
    public void testDrainAllChunksWithFailure()
            throws IOException
    {
        long maxBytes = 128L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(0.9)
                        .setSpoolingRatioLowWatermark(0.6),
                new ChunkManagerConfig(),
                new DataServerStats());
        SpoolingStorage failureInjectingSpoolingStorage = new FailureInjectingS3SpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory("s3://" + minioStorage.getBucketName()),
                S3Utils.createS3Client(new S3ClientConfig()
                        .setS3AwsAccessKey(MinioStorage.ACCESS_KEY)
                        .setS3AwsSecretKey(MinioStorage.SECRET_KEY)
                        .setRegion("us-east-1")
                        .setS3Endpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint())),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                S3SpoolingStorage.CompatibilityMode.AWS,
                new GcsClientConfig(),
                Set.of(EXCHANGE_0));
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(32, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(32, BYTE),
                failureInjectingSpoolingStorage,
                1);  // Ensure no concurrency to ensure that the exception is handled before the next exchange begins writing.

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());

        // 1 closed chunk and 1 partially filled open chunk
        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("data for chunk 0"), utf8Slice("partial1"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture1.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(64);

        // 1 closed chunk and 1 partially filled open chunk
        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(
                EXCHANGE_1, 1, 1, 1, 2L, ImmutableList.of(utf8Slice("data for chunk 2"), utf8Slice("partial3"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture2.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(0);

        chunkManager.spoolIfNecessary();

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 16);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 8);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 16);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 1, 3L, 8);

        verifySpooledChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(1, 1, utf8Slice("data for chunk 2")));
        assertThatThrownBy(() -> chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle3.partitionId(), chunkHandle3.chunkId()))
                .isInstanceOf(DataServerException.class)
                .hasMessageStartingWith("No closed chunk found");

        verifyInMemoryChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("data for chunk 0")));
        assertThatThrownBy(() -> chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()))
                .isInstanceOf(DataServerException.class)
                .hasMessageStartingWith("No closed chunk found");

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        verifyInMemoryChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("partial1")));
    }

    @Test
    @Disabled
    // Only use this when verifying the multi-failure logging in ChunkManager.spoolIfNecessary()
    public void testDrainAllChunksWithMultipleExchangeFailure()
            throws IOException
    {
        long maxBytes = 128L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(0.9)
                        .setSpoolingRatioLowWatermark(0.6),
                new ChunkManagerConfig(),
                new DataServerStats());
        SpoolingStorage failureInjectingSpoolingStorage = new FailureInjectingS3SpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory("s3://" + minioStorage.getBucketName()),
                S3Utils.createS3Client(new S3ClientConfig()
                        .setS3AwsAccessKey(MinioStorage.ACCESS_KEY)
                        .setS3AwsSecretKey(MinioStorage.SECRET_KEY)
                        .setRegion("us-east-1")
                        .setS3Endpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint())),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                S3SpoolingStorage.CompatibilityMode.AWS,
                new GcsClientConfig(),
                Set.of(EXCHANGE_0, EXCHANGE_1));
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(32, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(32, BYTE),
                failureInjectingSpoolingStorage,
                1);  // Ensure no concurrency to ensure that the exception is handled before the next exchange begins writing.

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());

        // 1 closed chunk and 1 partially filled open chunk
        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(
                EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("data for chunk 0"), utf8Slice("partial1"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture1.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(64);

        // 1 closed chunk and 1 partially filled open chunk
        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(
                EXCHANGE_1, 1, 1, 1, 2L, ImmutableList.of(utf8Slice("data for chunk 2"), utf8Slice("partial3"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture3.isDone()).isTrue());
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(0);

        chunkManager.spoolIfNecessary();

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 16);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 8);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 1, 2L, 16);
        ChunkHandle chunkHandle3 = new ChunkHandle(BUFFER_NODE_ID, 1, 3L, 8);

        verifyInMemoryChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("data for chunk 0")));
        assertThatThrownBy(() -> chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()))
                .isInstanceOf(DataServerException.class)
                .hasMessageStartingWith("No closed chunk found");

        verifyInMemoryChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(1, 1, utf8Slice("data for chunk 2")));
        assertThatThrownBy(() -> chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle3.partitionId(), chunkHandle3.chunkId()))
                .isInstanceOf(DataServerException.class)
                .hasMessageStartingWith("No closed chunk found");

        getFutureValue(chunkManager.finishExchange(EXCHANGE_0));
        getFutureValue(chunkManager.finishExchange(EXCHANGE_1));
        verifyInMemoryChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("partial1")));
        verifyInMemoryChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_1, chunkHandle3.partitionId(), chunkHandle3.chunkId()),
                new DataPage(1, 1, utf8Slice("partial3")));
    }

    @RepeatedTest(5) // test probabilistic race
    public void testRegisterExchangeWhileDraining()
            throws InterruptedException
    {
        long maxBytes = 64L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(0.8)
                        .setSpoolingRatioLowWatermark(0.5),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(16, BYTE),
                DataSize.of(64, BYTE),
                DataSize.of(8, BYTE));

        chunkManager.registerExchange(EXCHANGE_0, STANDARD, Optional.empty());
        assertThat(chunkManager.getExchangeAndHeartbeat(EXCHANGE_0).isFinished()).isFalse();

        Future<?> drainAllChunksFuture = executor.submit(chunkManager::drainAllChunks);

        // wait until chunkManager::drainAllChunks finishes all existing exchanges
        assertEventually(new Duration(1, SECONDS), () -> assertThat(chunkManager.getExchangeAndHeartbeat(EXCHANGE_0).isFinished()).isTrue());
        Thread.sleep(200);
        // it is waiting for markAllClosedChunksReceived on all exchanges now

        // register one more exchange and immediatelly close the other
        chunkManager.registerExchange(EXCHANGE_1, STANDARD, Optional.empty());
        chunkManager.markAllClosedChunksReceived(EXCHANGE_0);

        // finish should be triggered on new exchange too
        assertEventually(new Duration(1, SECONDS), () -> chunkManager.getExchangeAndHeartbeat(EXCHANGE_1).isFinished());

        // we should get information that there are no more chunks for both exchanges
        listClosedChunkUntilNoMore(chunkManager, EXCHANGE_0, OptionalLong.empty());
        listClosedChunkUntilNoMore(chunkManager, EXCHANGE_1, OptionalLong.empty());

        // mark all chunks received (simulate Trino behavior)
        chunkManager.markAllClosedChunksReceived(EXCHANGE_1);

        // drainAllChunk should complete timely
        assertThat(drainAllChunksFuture).succeedsWithin(5, SECONDS);
    }

    @Test
    public void testAddToRemovedExchange()
    {
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
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
                .matches(t -> ((DataServerException) t).getErrorCode() == ErrorCode.EXCHANGE_NOT_FOUND)
                .hasMessage("exchange %s already removed (EXPLICIT)".formatted(EXCHANGE_0));
        assertThat(chunkManager.getTrackedExchanges()).isEqualTo(0);
    }

    @Test
    public void testListClosedChunks()
    {
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                defaultMemoryAllocator(),
                DataSize.of(12, BYTE),
                DataSize.of(48, BYTE),
                DataSize.of(12, BYTE));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("page0"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("page1"))).addDataPagesFuture());
        OptionalLong pagingId = OptionalLong.empty();
        ChunkList chunkList = listClosedChunks(chunkManager, EXCHANGE_0, pagingId, 1); // only chunk 0 closed at this point
        pagingId = chunkList.nextPagingId();
        assertThat(pagingId).isPresent();
        assertThat(chunkList.chunks()).containsExactly(new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 5));

        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(utf8Slice("page2"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 3L, ImmutableList.of(utf8Slice("page3"))).addDataPagesFuture());
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 4L, ImmutableList.of(utf8Slice("page4"))).addDataPagesFuture());
        chunkList = listClosedChunks(chunkManager, EXCHANGE_0, pagingId, 3); // chunk 1, 2, 3 are newly closed
        pagingId = chunkList.nextPagingId();
        assertThat(pagingId).isPresent();
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
        assertThat(pagingId).isEmpty();
        assertThat(chunkList.chunks()).containsExactly(
                new ChunkHandle(BUFFER_NODE_ID, 0, 4L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 5L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 6L, 5),
                new ChunkHandle(BUFFER_NODE_ID, 0, 7L, 5));

        BufferNodeExchangeMetrics exchangeMetrics = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics).isEqualTo(new BufferNodeExchangeMetrics(1, 8, 40, 0, 0, 8, 40));
    }

    @Test
    public void testWaitForInProgressAddDataPages()
            throws InterruptedException
    {
        long maxBytes = 16L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(1.0)
                        .setSpoolingRatioLowWatermark(1.0),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(8, BYTE),
                DataSize.of(32, BYTE),
                DataSize.of(8, BYTE));
        ListenableFuture<Void> addDataPagesFuture1 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 1L, ImmutableList.of(utf8Slice("1"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture1.isDone()).isTrue());
        ListenableFuture<Void> addDataPagesFuture2 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 2L, ImmutableList.of(utf8Slice("2"))).addDataPagesFuture();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture2.isDone()).isTrue());
        ListenableFuture<Void> addDataPagesFuture3 = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 3L, ImmutableList.of(utf8Slice("3"))).addDataPagesFuture();
        assertThat(addDataPagesFuture3.isDone()).isFalse();

        ListenableFuture<Void> exchangeFinishFuture = chunkManager.finishExchange(EXCHANGE_0);
        // for finish and addDataPagesFuture3 we need more memory
        assertThat(exchangeFinishFuture.isDone()).isFalse();
        assertThat(addDataPagesFuture3.isDone()).isFalse();

        // spool to release memory pressure
        chunkManager.spoolIfNecessary();
        assertEventually(new Duration(1, SECONDS), () -> assertThat(addDataPagesFuture3.isDone()).isTrue());
        assertEventually(new Duration(1, SECONDS), () -> assertThat(exchangeFinishFuture.isDone()).isTrue());

        ChunkHandle chunkHandle0 = new ChunkHandle(BUFFER_NODE_ID, 0, 0L, 1);
        ChunkHandle chunkHandle1 = new ChunkHandle(BUFFER_NODE_ID, 0, 1L, 1);
        ChunkHandle chunkHandle2 = new ChunkHandle(BUFFER_NODE_ID, 0, 2L, 1);

        // closedChunkConsumer schedules a separate task to move chunks from recentlyClosedChunks to pendingChunkList,
        // so a single listClosedChunks call may race with that task and miss just-closed chunks. Page through.
        List<ChunkHandle> chunks = listClosedChunkUntilNoMore(chunkManager, EXCHANGE_0, OptionalLong.empty());
        assertThat(chunks).containsExactlyInAnyOrder(chunkHandle0, chunkHandle1, chunkHandle2);

        BufferNodeExchangeMetrics exchangeMetrics = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics).isEqualTo(new BufferNodeExchangeMetrics(1, 2, 2, 1, 1, 3, 3));

        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle0.partitionId(), chunkHandle0.chunkId()),
                new DataPage(0, 0, utf8Slice("1")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle1.partitionId(), chunkHandle1.chunkId()),
                new DataPage(0, 0, utf8Slice("2")));
        verifyChunkDataResult(chunkManager.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, chunkHandle2.partitionId(), chunkHandle2.chunkId()),
                new DataPage(0, 0, utf8Slice("3")));
    }

    @Test
    public void testAddDataPagesShouldRetainMemory()
    {
        long maxBytes = 16L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setSpoolingRatioHighWatermark(1.0)
                        .setSpoolingRatioLowWatermark(1.0),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
                memoryAllocator,
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        AddDataPagesResult addDataPagesResult = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy")));
        assertThat(addDataPagesResult.addDataPagesFuture().isDone()).isFalse(); // addDataPagesResult will block because of memory not enough
        assertThat(addDataPagesResult.shouldRetainMemory()).isTrue();

        AddDataPagesResult retriedAddDataPagesResult = chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy")));
        assertThat(retriedAddDataPagesResult.addDataPagesFuture().isDone()).isFalse();
        // retry should get the same future
        assertThat(retriedAddDataPagesResult.addDataPagesFuture()).isEqualTo(addDataPagesResult.addDataPagesFuture());
        assertThat(retriedAddDataPagesResult.shouldRetainMemory()).isFalse(); // retry shouldn't retain memory for input data

        addDataPagesResult.addDataPagesFuture().cancel(true);
        assertThat(retriedAddDataPagesResult.addDataPagesFuture().isCancelled()).isTrue();
    }

    @Test
    public void testWritePagesOfDifferentSizes()
    {
        Slice tinyPage = utf8Slice("0");
        Slice largePage = utf8Slice("2".repeat(40));
        Slice hugePage = utf8Slice("3".repeat(100));

        MemoryAllocator memoryAllocator = defaultMemoryAllocator();
        ChunkManager chunkManager = createChunkManager(
                BUFFER_NODE_ID,
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
        assertThat(chunkList.nextPagingId()).isEmpty();

        BufferNodeExchangeMetrics exchangeMetrics = chunkManager.pingExchange(EXCHANGE_0);
        assertThat(exchangeMetrics).isEqualTo(new BufferNodeExchangeMetrics(3, 7, 126, 0, 0, 7, 126));

        assertThat((memoryAllocator.getTotalMemory() - memoryAllocator.getFreeMemory())).isEqualTo(192);

        assertThatThrownBy(() -> getFutureValue(chunkManager.addDataPages(EXCHANGE_1, 3, 3, 3, 0L, ImmutableList.of(hugePage)).addDataPagesFuture()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("requiredStorageSize 107 larger than chunkMaxSizeInBytes 64");

        chunkManager.removeExchange(EXCHANGE_0);
        chunkManager.removeExchange(EXCHANGE_1);
        Object expected = memoryAllocator.getTotalMemory();
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(expected);
    }

    @Test
    public void testGetDrainedChunkDataOnAfterDrainingAllChunks()
    {
        long drainedBufferNodeId = BUFFER_NODE_ID + 1;
        ChunkManager drainedChunkManager = createChunkManager(
                drainedBufferNodeId,
                defaultMemoryAllocator(),
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));
        getFutureValue(drainedChunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("test"))).addDataPagesFuture());
        getFutureValue(drainedChunkManager.addDataPages(EXCHANGE_0, 0, 0, 1, 0L, ImmutableList.of(utf8Slice("Spooling"))).addDataPagesFuture());
        getFutureValue(drainedChunkManager.addDataPages(EXCHANGE_0, 0, 1, 0, 0L, ImmutableList.of(utf8Slice("Storage"))).addDataPagesFuture());

        Future<Integer> numClosedChunksFuture = executor.submit(() -> {
            OptionalLong pagingId = OptionalLong.empty();
            int numChunks = 0;
            for (int i = 0; i < 10; ++i) {
                ChunkList chunkList = getFutureValue(drainedChunkManager.listClosedChunks(EXCHANGE_0, pagingId));
                pagingId = chunkList.nextPagingId();
                numChunks += chunkList.chunks().size();
                if (pagingId.isEmpty()) {
                    drainedChunkManager.markAllClosedChunksReceived(EXCHANGE_0);
                    return numChunks;
                }

                sleepUninterruptibly(100, MILLISECONDS);
            }
            return abort();
        });

        drainedChunkManager.drainAllChunks();
        drainedChunkManager.clearSpooledChunkByExchange();

        assertThat(numClosedChunksFuture.isDone()).isTrue();

        ChunkManager newChunkManager = createChunkManager(
                BUFFER_NODE_ID,
                defaultMemoryAllocator(),
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));
        // exchange missing
        assertDrainedChunkDataResult(newChunkManager, drainedBufferNodeId);

        // exchange exists, but partition missing
        getFutureValue(newChunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("dummy1"))).addDataPagesFuture());
        assertDrainedChunkDataResult(newChunkManager, drainedBufferNodeId);

        // exchange exists, partition exists, but chunk missing
        getFutureValue(newChunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy0"))).addDataPagesFuture());
        assertDrainedChunkDataResult(newChunkManager, drainedBufferNodeId);

        newChunkManager.removeExchange(EXCHANGE_0);
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

    private void verifySpooledChunkDataResult(ChunkDataResult chunkDataResult, DataPage... values)
    {
        assertThat(chunkDataResult).isInstanceOf(SpooledChunkResult.class);
        List<DataPage> dataPages = getFutureValue(spooledChunkReader.getDataPages(((SpooledChunkResult) chunkDataResult).spooledChunk()));
        assertThat(dataPages).containsExactlyInAnyOrder(values);
    }

    private void verifyInMemoryChunkDataResult(ChunkDataResult chunkDataResult, DataPage... values)
    {
        assertThat(chunkDataResult).isInstanceOf(ChunkContentResult.class);
        verifyChunkData(((ChunkContentResult) chunkDataResult).lease(), values);
    }

    private void verifyChunkDataResult(ChunkDataResult chunkDataResult, DataPage... values)
    {
        switch (chunkDataResult) {
            case ChunkContentResult(ChunkDataLease lease) -> verifyChunkData(lease, values);
            case SpooledChunkResult(SpooledChunk spooledChunk) -> {
                List<DataPage> dataPages = getFutureValue(spooledChunkReader.getDataPages(spooledChunk));
                assertThat(dataPages).containsExactlyInAnyOrder(values);
            }
        }
    }

    protected MemoryAllocator defaultMemoryAllocator()
    {
        return new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, MEGABYTE)),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());
    }

    protected ChunkManager createChunkManager(
            long bufferNodeId,
            MemoryAllocator memoryAllocator,
            DataSize chunkTargetSize,
            DataSize chunkMaxSize,
            DataSize chunkSliceSize)
    {
        return this.createChunkManager(bufferNodeId, memoryAllocator, chunkTargetSize, chunkMaxSize, chunkSliceSize, spoolingStorage, 8);
    }

    protected ChunkManager createChunkManager(
            long bufferNodeId,
            MemoryAllocator memoryAllocator,
            DataSize chunkTargetSize,
            DataSize chunkMaxSize,
            DataSize chunkSliceSize,
            SpoolingStorage spoolingStorage,
            int chunkSpoolConcurrency)
    {
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig()
                .setChunkTargetSize(chunkTargetSize)
                .setChunkMaxSize(chunkMaxSize)
                .setChunkSliceSize(chunkSliceSize)
                .setChunkSpoolInterval(succinctDuration(100, SECONDS))
                .setChunkSpoolConcurrency(chunkSpoolConcurrency); // only manual triggering in tests
        DataServerConfig dataServerConfig = new DataServerConfig()
                .setTraceResourceReportingEnabled(false)
                .setDataIntegrityVerificationEnabled(true)
                .setMinDrainingDuration(succinctDuration(0, SECONDS)) // don't wait for extra time in tests
                // Reduce timeout here for calls when we expect zero results - we want those to return ASAP to reduce test duration
                .setChunkListPollTimeout(Duration.succinctDuration(5, MILLISECONDS));
        ChunkDataFactory chunkDataFactory = new ChunkDataFactory(Optional.empty(), memoryAllocator, executor, chunkManagerConfig, dataServerConfig);
        return new ChunkManager(
                new BufferNodeId(bufferNodeId),
                new BufferNodeStateManager(),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                spoolingStorage,
                ticker,
                new SpooledChunksByExchange(),
                chunkDataFactory,
                new DataServerStats(),
                new Tracer() {
                    @Override
                    public SpanBuilder spanBuilder(String spanName)
                    {
                        return null;
                    }
                },
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
        return abort();
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
                return abort(String.format("Expected chunkHandles.size()=%s to be less than or equal to %s; chunkHandles=%s", chunkHandles.size(), expectedChunkListSize, chunkHandles));
            }

            sleepUninterruptibly(100, MILLISECONDS);
        }
        return abort();
    }

    protected void assertDrainedChunkDataResult(ChunkManager chunkManager, long drainedBufferNodeId)
    {
        ChunkDataResult chunkDataResult = chunkManager.getChunkData(drainedBufferNodeId, EXCHANGE_0, 0, 0L);
        assertThat(chunkDataResult).isInstanceOf(SpooledChunkResult.class);
        SpooledChunk spooledChunk = ((SpooledChunkResult) chunkDataResult).spooledChunk();
        assertThat(spooledChunk.length()).isEqualTo(52);
        assertThat(spooledChunk.location()).startsWith("s3://" + minioStorage.getBucketName());
        assertThat(spooledChunk.location()).contains("exchange-0." + drainedBufferNodeId);
    }

    private static BufferNodeExchangeMetrics emptyMetrics()
    {
        return new BufferNodeExchangeMetrics(0, 0, 0, 0, 0, 0, 0);
    }
}
