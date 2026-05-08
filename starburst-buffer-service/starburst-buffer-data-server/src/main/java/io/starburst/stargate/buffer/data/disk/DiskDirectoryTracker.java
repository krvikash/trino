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
import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public class DiskDirectoryTracker
{
    private static final Logger log = Logger.get(DiskDirectoryTracker.class);

    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("local-disk-cleanup-%s"));
    private final ConcurrentHashMap<Path, AtomicInteger> exchangeChunkCounts = new ConcurrentHashMap<>();

    public Runnable registerChunkRelease(Path exchangeDirectory)
    {
        exchangeChunkCounts.compute(exchangeDirectory, (_, count) -> {
            if (count == null) {
                return new AtomicInteger(1);
            }
            count.incrementAndGet();
            return count;
        });
        return () -> onChunkReleased(exchangeDirectory);
    }

    Future<?> submitCleanup(Runnable task)
    {
        return cleanupExecutor.submit(task);
    }

    private void onChunkReleased(Path exchangeDirectory)
    {
        exchangeChunkCounts.compute(exchangeDirectory, (dir, count) -> {
            if (count == null || count.decrementAndGet() == 0) {
                cleanupExecutor.execute(() -> deleteDirectoryQuietly(dir));
                return null;
            }
            return count;
        });
    }

    private static void deleteDirectoryQuietly(Path path)
    {
        if (!Files.exists(path)) {
            return;
        }
        try {
            deleteRecursively(path, ALLOW_INSECURE);
            log.debug("Released directory %s", path);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete directory: %s", path);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        cleanupExecutor.shutdownNow();
    }

    @VisibleForTesting
    public void awaitPendingTasks()
            throws InterruptedException, ExecutionException
    {
        // cleanupExecutor is single-threaded; submitting a no-op and waiting on it acts as a barrier
        // that flushes previously-submitted cleanup tasks.
        CompletableFuture.runAsync(() -> {}, cleanupExecutor).get();
    }
}
