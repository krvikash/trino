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
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.initializeDirectories;
import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public class LocalDiskTier
{
    private static final Logger log = Logger.get(LocalDiskTier.class);

    private final Path directory;
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("local-disk-cleanup-%s"));

    @Inject
    public LocalDiskTier(BufferNodeId bufferNodeId, LocalDiskTierConfig config)
    {
        requireNonNull(bufferNodeId, "bufferNodeId is null");
        requireNonNull(config, "config is null");
        Path rootDirectory = requireNonNull(config.getDirectory(), "directory is null").normalize();
        this.directory = rootDirectory.resolve(String.valueOf(bufferNodeId.getLongValue()));
        initializeDirectories(rootDirectory, this.directory, cleanupExecutor);
    }

    public void validateExchangeId(String exchangeId)
    {
        validateExchangeIdAsPathSegment(directory, exchangeId);
    }

    public void createPartitionDirectory(String exchangeId, int partitionId)
    {
        Path partitionDirectory = directory.resolve(exchangeId).resolve(String.valueOf(partitionId));
        try {
            createDirectories(partitionDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to create partition directory " + partitionDirectory, e);
        }
    }

    public void releasePartitionDirectory(String exchangeId, int partitionId)
    {
        Path partitionDirectory = directory.resolve(exchangeId).resolve(String.valueOf(partitionId));
        cleanupExecutor.execute(() -> deleteDirectoryQuietly(partitionDirectory, "partition"));
    }

    public void releaseExchangeDirectory(String exchangeId)
    {
        Path exchangeDirectory = directory.resolve(exchangeId);
        // single-thread FIFO executor serializes with releasePartitionDirectory; whichever runs first
        // sees the directory present and the second silently no-ops on the missing path.
        cleanupExecutor.execute(() -> deleteDirectoryQuietly(exchangeDirectory, "exchange"));
    }

    private static void deleteDirectoryQuietly(Path path, String directoryKind)
    {
        try {
            deleteRecursively(path, ALLOW_INSECURE);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete %s directory: %s", directoryKind, path);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        cleanupExecutor.shutdownNow();
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
    public void awaitPendingTasks()
            throws InterruptedException, ExecutionException
    {
        cleanupExecutor.submit(() -> null).get();
    }
}
