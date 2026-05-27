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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.disk.DiskChunkSlot;
import io.starburst.stargate.buffer.data.disk.ForLocalDiskIo;
import io.starburst.stargate.buffer.data.disk.LocalDiskTier;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.server.DataServerConfig;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ChunkDataFactory
{
    private static final Logger log = Logger.get(ChunkDataFactory.class);

    private final Optional<LocalDiskTier> localDiskTier;
    private final MemoryAllocator memoryAllocator;
    private final ExecutorService executor;
    private final Executor diskIoExecutor;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final ChunkAllocationStats chunkAllocationStats = new ChunkAllocationStats();

    @Inject
    public ChunkDataFactory(
            Optional<LocalDiskTier> localDiskTier,
            MemoryAllocator memoryAllocator,
            ExecutorService executor,
            @ForLocalDiskIo Optional<ExecutorService> diskIoExecutor,
            ChunkManagerConfig chunkManagerConfig,
            DataServerConfig dataServerConfig)
    {
        this.localDiskTier = requireNonNull(localDiskTier, "localDiskTier is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.executor = requireNonNull(executor, "executor is null");
        // Dedicated bounded pool for disk-chunk writes
        this.diskIoExecutor = requireNonNull(diskIoExecutor, "diskIoExecutor is null").map(Executor.class::cast).orElse(executor);
        requireNonNull(chunkManagerConfig, "chunkManagerConfig is null");
        requireNonNull(dataServerConfig, "dataServerConfig is null");
        this.chunkSliceSizeInBytes = toIntExact(chunkManagerConfig.getChunkSliceSize().toBytes());
        this.calculateDataPagesChecksum = dataServerConfig.isDataIntegrityVerificationEnabled();
    }

    public ChunkData create(String exchangeId, int partitionId, long chunkId, int chunkSizeInBytes)
    {
        if (localDiskTier.isPresent()) {
            LocalDiskTier diskTier = localDiskTier.get();
            Optional<DiskChunkSlot> slot = diskTier.tryReserveChunkSlot(exchangeId, partitionId, chunkId, chunkSizeInBytes);
            if (slot.isPresent()) {
                DiskChunkSlot diskChunkSlot = slot.get();
                log.debug("Chunk %s for exchange %s partition %s allocated to disk: %s", chunkId, exchangeId, partitionId, diskChunkSlot.file());
                DiskChunkData diskChunkData = new DiskChunkData(diskIoExecutor, chunkId, chunkSizeInBytes, calculateDataPagesChecksum, diskChunkSlot);
                chunkAllocationStats.recordDiskChunk();
                return diskChunkData;
            }
            log.debug("Chunk %s for exchange %s partition %s falling back to memory (disk allocation failed)", chunkId, exchangeId, partitionId);
        }
        MemoryChunkData memoryChunkData = new MemoryChunkData(memoryAllocator, executor, chunkSizeInBytes, chunkSliceSizeInBytes, calculateDataPagesChecksum);
        chunkAllocationStats.recordMemoryChunk();
        return memoryChunkData;
    }

    public ChunkAllocationStats getChunkAllocationStats()
    {
        return chunkAllocationStats;
    }
}
