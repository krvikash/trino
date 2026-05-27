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
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.TestingMemoryConfig;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestChunkDataFactory
{
    private static final String EXCHANGE_ID = "exchange-1";
    private static final int PARTITION_ID = 3;
    private static final long CHUNK_ID = 5;
    private static final int CHUNK_SLICE_SIZE_BYTES = 1024;
    private static final DataSize MEMORY_CAPACITY = DataSize.of(64, MEGABYTE);

    private ExecutorService executor;
    private ChunkManagerConfig chunkManagerConfig;
    private MemoryAllocator memoryAllocator;

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

    private ChunkDataFactory createFactory(Optional<LocalDiskTier> localDiskTier)
    {
        return new ChunkDataFactory(localDiskTier, memoryAllocator, executor, Optional.empty(), chunkManagerConfig, new DataServerConfig());
    }
}
