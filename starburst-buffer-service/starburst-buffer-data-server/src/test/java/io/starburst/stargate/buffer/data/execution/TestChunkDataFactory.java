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

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestChunkDataFactory
{
    private static final long BUFFER_NODE_ID = 7;
    private static final String EXCHANGE_ID = "exchange-1";
    private static final int PARTITION_ID = 3;
    private static final long CHUNK_ID = 5;
    private static final int CHUNK_SIZE_IN_BYTES = 1024;
    private static final DataSize THRESHOLD = DataSize.of(1, MEGABYTE);
    private static final DataSize DISK_CAPACITY = DataSize.of(10, MEGABYTE);

    @TempDir
    Path tempDir;

    private ExecutorService executor;
    private ChunkManagerConfig chunkManagerConfig;
    private MemoryAllocator memoryAllocator;

    @BeforeEach
    public void setUp()
    {
        executor = Executors.newSingleThreadExecutor();
        chunkManagerConfig = new ChunkManagerConfig().setChunkSliceSize(DataSize.ofBytes(CHUNK_SIZE_IN_BYTES));
        memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, MEGABYTE)),
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

        ChunkData chunkData = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, CHUNK_SIZE_IN_BYTES, Long.MAX_VALUE);

        assertThat(chunkData).isInstanceOf(MemoryChunkData.class);
    }

    @Test
    public void testReturnsMemoryWhenThresholdNotConfigured()
    {
        ChunkDataFactory factory = createFactory(Optional.of(createDiskTier(Optional.empty(), DISK_CAPACITY)));

        ChunkData chunkData = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, CHUNK_SIZE_IN_BYTES, Long.MAX_VALUE);

        assertThat(chunkData).isInstanceOf(MemoryChunkData.class);
    }

    @Test
    public void testReturnsMemoryWhenCumulativeBelowThreshold()
    {
        ChunkDataFactory factory = createFactory(Optional.of(createDiskTier(Optional.of(THRESHOLD), DISK_CAPACITY)));

        ChunkData chunkData = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes() - 1);

        assertThat(chunkData).isInstanceOf(MemoryChunkData.class);
        // factory must not materialize the partition directory when staying on memory
        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve(EXCHANGE_ID).resolve(String.valueOf(PARTITION_ID))).doesNotExist();
    }

    @Test
    public void testReturnsDiskWhenCumulativeAtOrAboveThreshold()
    {
        ChunkDataFactory factory = createFactory(Optional.of(createDiskTier(Optional.of(THRESHOLD), DISK_CAPACITY)));

        ChunkData atThreshold = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes());
        ChunkData aboveThreshold = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID + 1, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes() + 1);

        assertThat(atThreshold).isInstanceOf(DiskChunkData.class);
        assertThat(aboveThreshold).isInstanceOf(DiskChunkData.class);
        // factory creates the partition directory lazily on the first disk-backed chunk
        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve(EXCHANGE_ID).resolve(String.valueOf(PARTITION_ID))).isDirectory();
    }

    @Test
    public void testFallsBackToMemoryWhenCapacityExhausted()
    {
        // Capacity holds exactly two chunks; the third must fall back to memory,
        // and releasing one disk chunk must free space for a subsequent disk-eligible request.
        DataSize tightCapacity = DataSize.ofBytes(2L * CHUNK_SIZE_IN_BYTES);
        ChunkDataFactory factory = createFactory(Optional.of(createDiskTier(Optional.of(THRESHOLD), tightCapacity)));

        ChunkData first = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes());
        ChunkData second = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID + 1, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes());
        ChunkData third = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID + 2, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes());

        assertThat(first).isInstanceOf(DiskChunkData.class);
        assertThat(second).isInstanceOf(DiskChunkData.class);
        assertThat(third).isInstanceOf(MemoryChunkData.class);

        first.release();

        ChunkData fourth = factory.create(EXCHANGE_ID, PARTITION_ID, CHUNK_ID + 3, CHUNK_SIZE_IN_BYTES, THRESHOLD.toBytes());
        assertThat(fourth).isInstanceOf(DiskChunkData.class);
    }

    private ChunkDataFactory createFactory(Optional<LocalDiskTier> localDiskTier)
    {
        return new ChunkDataFactory(localDiskTier, memoryAllocator, executor, chunkManagerConfig, new DataServerConfig());
    }

    private LocalDiskTier createDiskTier(Optional<DataSize> memorySkipThreshold, DataSize capacity)
    {
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), createDiskTierConfig(memorySkipThreshold, capacity));
    }

    private LocalDiskTierConfig createDiskTierConfig(Optional<DataSize> memorySkipThreshold, DataSize capacity)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(capacity);
        memorySkipThreshold.ifPresent(config::setMemorySkipThreshold);
        return config;
    }
}
