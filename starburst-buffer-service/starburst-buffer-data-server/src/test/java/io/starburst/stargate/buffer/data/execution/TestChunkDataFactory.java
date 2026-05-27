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

import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.disk.LocalDiskAllocator;
import io.starburst.stargate.buffer.data.disk.LocalDiskTier;
import io.starburst.stargate.buffer.data.disk.LocalDiskTierConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.TestingMemoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestChunkDataFactory
{
    private static final String EXCHANGE_ID = "exchange-1";
    private static final int PARTITION_ID = 3;
    private static final long CHUNK_ID = 5;
    private static final int CHUNK_SLICE_SIZE_BYTES = 1024;
    private static final DataSize MEMORY_CAPACITY = DataSize.of(64, MEGABYTE);
    private static final DataSize DEFAULT_CHUNK_SIZE = DataSize.of(64, KILOBYTE);
    private static final DataSize DISK_CAPACITY = DataSize.of(10, MEGABYTE);

    private ExecutorService executor;
    private ChunkManagerConfig chunkManagerConfig;
    private MemoryAllocator memoryAllocator;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp()
    {
        executor = Executors.newSingleThreadExecutor();
        chunkManagerConfig = new ChunkManagerConfig().setChunkSliceSize(DataSize.ofBytes(CHUNK_SLICE_SIZE_BYTES));
        memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(MEMORY_CAPACITY),
                new MemoryAllocatorConfig(),
                chunkManagerConfig,
                new DataServerStats());
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testReturnsMemoryWhenDiskTierAbsent()
    {
        ChunkDataFactory factory = createFactory(Optional.empty());

        ChunkData chunkData = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, CHUNK_SLICE_SIZE_BYTES);

        assertThat(chunkData).isInstanceOf(MemoryChunkData.class);
    }

    @Test
    public void testChunkUsesMemoryWhenBelowWatermark()
    {
        // Watermark=1.0 (100%) means routing never fires unless allocator is fully saturated.
        LocalDiskTier diskTier = createDiskTier(tempDir, 1.0);
        ChunkDataFactory factory = createFactory(Optional.of(diskTier));

        int largeChunk = toInt(DEFAULT_CHUNK_SIZE) + CHUNK_SLICE_SIZE_BYTES;
        ChunkData chunkData = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, largeChunk);

        assertThat(chunkData).isInstanceOf(MemoryChunkData.class);
    }

    @Test
    public void testChunkRoutesToDiskWhenAboveWatermark()
    {
        // Watermark=0.0 routes any chunk at or above the threshold to disk immediately.
        LocalDiskTier diskTier = createDiskTier(tempDir, 0.0);
        ChunkDataFactory factory = createFactory(Optional.of(diskTier));

        int largeChunk = toInt(DEFAULT_CHUNK_SIZE);
        ChunkData chunkData = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, largeChunk);

        assertThat(chunkData).isInstanceOf(DiskChunkData.class);
    }

    @Test
    public void testFallsBackToMemoryWhenDiskCapacityExhausted()
    {
        // Disk capacity is just one chunk; the second allocation must fall back to memory.
        DataSize tinyDisk = DataSize.ofBytes(toInt(DEFAULT_CHUNK_SIZE));
        LocalDiskTier diskTier = createDiskTier(tempDir, 0.0, tinyDisk);
        ChunkDataFactory factory = createFactory(Optional.of(diskTier));

        int chunkSize = toInt(DEFAULT_CHUNK_SIZE);
        ChunkData first = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, chunkSize);
        ChunkData second = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID + 1, chunkSize);

        assertThat(first).isInstanceOf(DiskChunkData.class);
        assertThat(second).isInstanceOf(MemoryChunkData.class);
    }

    @Test
    public void testChunkStaysInMemoryWhenExchangeUnreadBelowFraction()
    {
        // exchangeAllocatedBytesFraction=1.0: threshold = 100% of memory capacity; single chunk never reaches that.
        LocalDiskTier diskTier = createDiskTierWithUnreadFraction(tempDir, 1.0, 1.0);
        ChunkDataFactory factory = createFactory(Optional.of(diskTier));

        int chunkSize = toInt(DEFAULT_CHUNK_SIZE);
        ChunkData first = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, chunkSize);
        assertThat(first).isInstanceOf(MemoryChunkData.class);
    }

    @Test
    public void testChunkRoutesToDiskOnceExchangeUnreadExceedsFraction()
    {
        // exchangeAllocatedBytesFraction tiny so the first chunk crosses the threshold immediately.
        double tinyFraction = 1.0 / MEMORY_CAPACITY.toBytes(); // threshold = 1 byte
        LocalDiskTier diskTier = createDiskTierWithUnreadFraction(tempDir, 1.0, tinyFraction);
        ChunkDataFactory factory = createFactory(Optional.of(diskTier));

        int chunkSize = toInt(DEFAULT_CHUNK_SIZE);
        ChunkData first = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, chunkSize);
        assertThat(first).isInstanceOf(DiskChunkData.class);
    }

    private static LocalDiskTier createDiskTier(Path rootDirectory, double memoryHighWatermark)
    {
        return createDiskTier(rootDirectory, memoryHighWatermark, DISK_CAPACITY);
    }

    private static LocalDiskTier createDiskTier(Path rootDirectory, double memoryHighWatermark, DataSize capacity)
    {
        // low watermark == high watermark means no hysteresis zone; routing activates and deactivates at the same threshold.
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(rootDirectory)
                .setCapacity(capacity)
                .setAllowDirectoryCreation(true)
                .setMemoryHighWatermark(memoryHighWatermark)
                .setMemoryLowWatermark(memoryHighWatermark);
        return new LocalDiskTier(new BufferNodeId(1L), config, new LocalDiskAllocator(config));
    }

    private static LocalDiskTier createDiskTierWithUnreadFraction(Path rootDirectory, double memoryHighWatermark, double exchangeMemoryFraction)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(rootDirectory)
                .setCapacity(DISK_CAPACITY)
                .setAllowDirectoryCreation(true)
                .setMemoryHighWatermark(memoryHighWatermark)
                .setMemoryLowWatermark(memoryHighWatermark)
                .setExchangeMemoryFraction(exchangeMemoryFraction);
        return new LocalDiskTier(new BufferNodeId(1L), config, new LocalDiskAllocator(config));
    }

    private ChunkDataFactory createFactory(Optional<LocalDiskTier> localDiskTier)
    {
        return new ChunkDataFactory(localDiskTier, memoryAllocator, executor, Optional.empty(), new ExchangeChunkBytes(), new DataServerStats(), chunkManagerConfig, new DataServerConfig());
    }

    private static int toInt(DataSize size)
    {
        return toIntExact(size.toBytes());
    }
}
