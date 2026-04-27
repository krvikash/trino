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
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import jakarta.annotation.PreDestroy;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.initializeDirectories;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public class LocalDiskTier
{
    private final Path directory;
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("local-disk-cleanup-%s"));
    private final Future<?> startupCleanupFuture;

    @Inject
    public LocalDiskTier(BufferNodeId bufferNodeId, LocalDiskTierConfig config)
    {
        requireNonNull(bufferNodeId, "bufferNodeId is null");
        requireNonNull(config, "config is null");
        Path rootDirectory = requireNonNull(config.getDirectory(), "directory is null");
        this.directory = rootDirectory.resolve(String.valueOf(bufferNodeId.getLongValue()));
        this.startupCleanupFuture = initializeDirectories(rootDirectory, this.directory, cleanupExecutor);
    }

    @PreDestroy
    public void shutdown()
    {
        cleanupExecutor.shutdownNow();
    }

    @VisibleForTesting
    void awaitStartupCleanup()
            throws InterruptedException, ExecutionException
    {
        startupCleanupFuture.get();
    }
}
