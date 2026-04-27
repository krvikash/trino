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
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartition
{
    private static final long BUFFER_NODE_ID = 0;
    private static final String EXCHANGE_ID = "exchange-1";
    private static final int PARTITION_ID = 3;
    private static final int CHUNK_TARGET_SIZE = (int) DataSize.of(1, MEGABYTE).toBytes();
    private static final int CHUNK_MAX_SIZE = (int) DataSize.of(1, MEGABYTE).toBytes();
    private static final int CHUNK_SLICE_SIZE = (int) DataSize.of(128, KILOBYTE).toBytes();

    @TempDir
    Path tempDir;

    private ExecutorService executor;

    @BeforeEach
    public void setUp()
    {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testCreatesDirectoryOnConstruction()
    {
        LocalDiskTier diskTier = createDiskTier();
        createPartition(Optional.of(diskTier));

        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve(EXCHANGE_ID).resolve(String.valueOf(PARTITION_ID))).isDirectory();
    }

    @Test
    public void testDeletesDirectoryWithContentsOnRelease()
            throws IOException, ExecutionException, InterruptedException
    {
        LocalDiskTier diskTier = createDiskTier();
        Partition partition = createPartition(Optional.of(diskTier));

        Path partitionDir = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve(EXCHANGE_ID).resolve(String.valueOf(PARTITION_ID));
        Files.writeString(partitionDir.resolve("chunk-0.data"), "payload");
        assertThat(partitionDir).isDirectory();

        partition.releaseChunks();
        diskTier.awaitPendingTasks();

        assertThat(partitionDir).doesNotExist();
    }

    @Test
    public void testDisabledTierLeavesNoFilesystemTrace()
    {
        createPartition(Optional.empty());

        assertThat(tempDir.toFile().list()).isEmpty();
    }

    private LocalDiskTier createDiskTier()
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(DataSize.of(10, MEGABYTE));
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config);
    }

    private Partition createPartition(Optional<LocalDiskTier> localDiskTier)
    {
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, MEGABYTE)),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig().setChunkSliceSize(DataSize.ofBytes(CHUNK_SLICE_SIZE));
        ChunkDataFactory chunkDataFactory = new ChunkDataFactory(memoryAllocator, executor, chunkManagerConfig, new DataServerConfig());
        return new Partition(
                BUFFER_NODE_ID,
                EXCHANGE_ID,
                PARTITION_ID,
                new SpooledChunksByExchange(),
                CHUNK_TARGET_SIZE,
                CHUNK_MAX_SIZE,
                CHUNK_SLICE_SIZE,
                new ChunkIdGenerator(),
                ChunkDeliveryMode.STANDARD,
                executor,
                localDiskTier,
                chunkDataFactory,
                _ -> {});
    }
}
