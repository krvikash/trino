/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.server.BufferNodeId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.initializeDirectories;
import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalDiskTier
{
    private static final Logger log = Logger.get(LocalDiskTier.class);

    private final Path directory;
    private final LocalDiskAllocator diskAllocator;
    private final DiskDirectoryTracker directoryTracker;
    private final double memoryHighWatermarkPercent;
    private final double memoryLowWatermarkPercent;
    private final int maxOpenDiskChunks;
    private final double exchangeMemoryFraction;
    private volatile boolean diskRoutingActive;

    @Inject
    public LocalDiskTier(BufferNodeId bufferNodeId, LocalDiskTierConfig config, LocalDiskAllocator allocator)
    {
        requireNonNull(bufferNodeId, "bufferNodeId is null");
        requireNonNull(config, "config is null");
        Path rootDirectory = requireNonNull(config.getDirectory(), "directory is null").normalize();
        this.directory = rootDirectory.resolve(String.valueOf(bufferNodeId.getLongValue()));
        this.diskAllocator = requireNonNull(allocator, "allocator is null");
        this.directoryTracker = new DiskDirectoryTracker();
        initializeDirectories(rootDirectory, this.directory, directoryTracker, config.isAllowDirectoryCreation());
        this.memoryHighWatermarkPercent = config.getMemoryHighWatermark() * 100.0;
        this.memoryLowWatermarkPercent = config.getMemoryLowWatermark() * 100.0;
        checkState(memoryLowWatermarkPercent <= memoryHighWatermarkPercent,
                "memory-low-watermark (%s) must not exceed memory-high-watermark (%s)",
                config.getMemoryLowWatermark(),
                config.getMemoryHighWatermark());
        this.maxOpenDiskChunks = config.getMaxOpenDiskChunks();
        this.exchangeMemoryFraction = config.getExchangeMemoryFraction();
    }

    public Optional<DiskChunkSlot> tryReserveChunkSlot(
            String exchangeId,
            int partitionId,
            long chunkId,
            int chunkSizeInBytes)
    {
        validateExchangeIdAsPathSegment(directory, exchangeId);
        Optional<DiskSpaceLease> lease = diskAllocator.allocate(chunkSizeInBytes);
        if (lease.isEmpty()) {
            log.debug("Disk allocation skipped for exchange %s chunk %s size=%s", exchangeId, chunkId, chunkSizeInBytes);
            return Optional.empty();
        }
        DiskSpaceLease spaceLease = lease.get();
        try {
            Path file = chunkFile(exchangeId, partitionId, chunkId);
            Runnable directoryRelease = directoryTracker.registerChunkRelease(exchangeDirectory(exchangeId));
            Runnable releaseCallback = directoryRelease;
            Runnable materializeDirectory = () -> createPartitionDirectory(exchangeId, partitionId);
            return Optional.of(new DiskChunkSlot(file, spaceLease, releaseCallback, materializeDirectory));
        }
        catch (RuntimeException e) {
            spaceLease.release();
            throw e;
        }
    }

    public boolean shouldRouteToDisk(RoutingDecisionInputs inputs)
    {
        if (inputs.openChunks() >= maxOpenDiskChunks) {
            return false;
        }
        if (exchangeMemoryFraction > 0.0 && inputs.exchangeAllocatedBytes() > exchangeMemoryFraction * inputs.memoryCapacityBytes()) {
            return true;
        }
        if (inputs.memoryAllocationPercent() >= memoryHighWatermarkPercent) {
            diskRoutingActive = true;
        }
        else if (inputs.memoryAllocationPercent() < memoryLowWatermarkPercent) {
            diskRoutingActive = false;
        }
        return diskRoutingActive;
    }

    @VisibleForTesting
    public Optional<DiskSpaceLease> allocate(long bytes)
    {
        return diskAllocator.allocate(bytes);
    }

    public Path partitionDirectory(String exchangeId, int partitionId)
    {
        return exchangeDirectory(exchangeId).resolve(String.valueOf(partitionId));
    }

    public Path exchangeDirectory(String exchangeId)
    {
        return directory.resolve(exchangeId);
    }

    public Path createPartitionDirectory(String exchangeId, int partitionId)
    {
        Path partitionDirectory = partitionDirectory(exchangeId, partitionId);
        try {
            createDirectories(partitionDirectory);
            return partitionDirectory;
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to create partition directory " + partitionDirectory, e);
        }
    }

    private Path chunkFile(String exchangeId, int partitionId, long chunkId)
    {
        return partitionDirectory(exchangeId, partitionId).resolve("chunk-" + chunkId + ".data");
    }

    @VisibleForTesting
    static void validateExchangeIdAsPathSegment(Path bufferNodeDirectory, String exchangeId)
    {
        Path resolved = bufferNodeDirectory.resolve(exchangeId).normalize();
        checkArgument(
                resolved.startsWith(bufferNodeDirectory)
                        && !resolved.equals(bufferNodeDirectory)
                        && bufferNodeDirectory.equals(resolved.getParent()),
                "exchangeId %s does not resolve to a direct subdirectory of %s",
                exchangeId,
                bufferNodeDirectory);
    }

    @VisibleForTesting
    public DiskDirectoryTracker getDirectoryTracker()
    {
        return directoryTracker;
    }

    public record RoutingDecisionInputs(
            double memoryAllocationPercent,
            int openChunks,
            long exchangeAllocatedBytes,
            long memoryCapacityBytes) {}
}
