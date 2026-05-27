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

import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.disk.DiskChunkSlot;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testDoesNotCreateDirectoryOnConstructionWhenNoDiskChunks()
    {
        LocalDiskTier diskTier = createDiskTier();
        createPartition(Optional.of(diskTier));

        // ChunkDataFactory creates the partition directory only when it hands out a DiskChunkData;
        // a partition that never crosses the memory-skip threshold pays no filesystem cost.
        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve(EXCHANGE_ID).resolve(String.valueOf(PARTITION_ID))).doesNotExist();
    }

    @Test
    public void testReleaseChunksDoesNotDeletePartitionDirectoryDirectly()
            throws ExecutionException, InterruptedException
    {
        LocalDiskTier diskTier = createDiskTier();
        Partition partition = createPartition(Optional.of(diskTier));

        diskTier.createPartitionDirectory(EXCHANGE_ID, PARTITION_ID);
        Path partitionDir = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve(EXCHANGE_ID).resolve(String.valueOf(PARTITION_ID));
        assertThat(partitionDir).isDirectory();

        partition.releaseChunks();
        diskTier.getDirectoryTracker().awaitPendingTasks();

        assertThat(partitionDir).isDirectory();
    }

    @Test
    public void testDisabledTierLeavesNoFilesystemTrace()
    {
        createPartition(Optional.empty());

        assertThat(tempDir.toFile().list()).isEmpty();
    }

    @Test
    public void testWriteFailurePropagatesToAddDataPagesFuture()
    {
        LocalDiskTier diskTier = createDiskTier();
        IOException injected = new IOException("disk full");

        Partition partition = buildPartition(failingDiskFactory(diskTier, injected));
        AddDataPagesResult result = partition.addDataPages(1, 0, 0L, List.of(Slices.utf8Slice("page")));

        assertThatThrownBy(() -> getFutureValue(result.addDataPagesFuture()))
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("disk full");
    }

    private LocalDiskTier createDiskTier()
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(DataSize.of(10, MEGABYTE));
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config, new LocalDiskAllocator(config));
    }

    private Partition createPartition(Optional<LocalDiskTier> localDiskTier)
    {
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, MEGABYTE)),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig().setChunkSliceSize(DataSize.ofBytes(CHUNK_SLICE_SIZE));
        ChunkDataFactory chunkDataFactory = new ChunkDataFactory(localDiskTier, memoryAllocator, executor, Optional.empty(), new ExchangeChunkBytes(), new DataServerStats(), chunkManagerConfig, new DataServerConfig());
        return buildPartition(chunkDataFactory);
    }

    private ChunkDataFactory failingDiskFactory(LocalDiskTier diskTier, IOException cause)
    {
        return new ChunkDataFactory(
                Optional.of(diskTier),
                new MemoryAllocator(new TestingMemoryConfig(DataSize.of(64, MEGABYTE)), new MemoryAllocatorConfig(), new ChunkManagerConfig(), new DataServerStats()),
                executor,
                Optional.empty(),
                new ExchangeChunkBytes(),
                new DataServerStats(),
                new ChunkManagerConfig().setChunkSliceSize(DataSize.ofBytes(CHUNK_SLICE_SIZE)),
                new DataServerConfig())
        {
            @Override
            public ChunkData create(String exchangeId, int partitionId, long chunkId, int sizeBytes)
            {
                DiskChunkSlot slot = diskTier
                        .tryReserveChunkSlot(exchangeId, partitionId, chunkId, sizeBytes)
                        .orElseThrow();
                return new DiskChunkData(
                        executor,
                        slot.file(),
                        chunkId,
                        sizeBytes,
                        false,
                        slot.lease(),
                        slot.diskRelease(),
                        () -> {
                            throw new UncheckedIOException(cause);
                        },
                        new DataServerStats());
            }
        };
    }

    private Partition buildPartition(ChunkDataFactory chunkDataFactory)
    {
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
                chunkDataFactory,
                _ -> {});
    }
}
