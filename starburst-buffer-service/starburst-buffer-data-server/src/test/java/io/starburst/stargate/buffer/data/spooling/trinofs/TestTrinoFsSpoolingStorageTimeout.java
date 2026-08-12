/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.trinofs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.trino.filesystem.TrinoFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.toChunkDataLease;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTrinoFsSpoolingStorageTimeout
{
    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach
    void tearDown()
    {
        executors.forEach(ExecutorService::shutdownNow);
        executors.clear();
    }

    @Test
    void testWriteTimesOutAndInterruptsWorker()
            throws Exception
    {
        HangingFileSystem fileSystem = new HangingFileSystem();
        TrinoFsSpoolingStorage storage = createStorage(fileSystem, new Duration(500, MILLISECONDS));

        Map<Chunk, ChunkDataLease> chunkLeases = ImmutableMap.of(new Chunk(0L), singleMemoryChunkLease());
        long contentLength = chunkLeases.values().iterator().next().serializedSizeInBytes();
        ListenableFuture<Map<Long, SpooledChunk>> future = storage.putStorageObject("test-file", chunkLeases, contentLength);

        assertThatThrownBy(() -> future.get(10, SECONDS))
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(fileSystem.interrupted.await(10, SECONDS))
                .as("worker thread should be interrupted on timeout")
                .isTrue();
    }

    @Test
    void testDeleteTimesOutAndInterruptsWorker()
            throws Exception
    {
        HangingFileSystem fileSystem = new HangingFileSystem();
        TrinoFsSpoolingStorage storage = createStorage(fileSystem, new Duration(500, MILLISECONDS));

        ListenableFuture<Void> future = storage.deleteDirectories(ImmutableList.of("test-dir"));

        assertThatThrownBy(() -> future.get(10, SECONDS))
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(fileSystem.interrupted.await(10, SECONDS))
                .as("worker thread should be interrupted on timeout")
                .isTrue();
    }

    private TrinoFsSpoolingStorage createStorage(TrinoFileSystem fileSystem, Duration operationTimeout)
    {
        return new TrinoFsSpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory("s3://bucket/"),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                fileSystem,
                newWorkerExecutor(),
                newWorkerExecutor(),
                new TrinoFsSpoolingConfig().setOperationTimeout(operationTimeout),
                newTimeoutExecutor());
    }

    // A direct executor would run the blocking call inline on the caller's thread, so it would never return a Future for the timeout logic to cancel/interrupt
    private ListeningExecutorService newWorkerExecutor()
    {
        ListeningExecutorService executor = listeningDecorator(newSingleThreadExecutor(daemonThreadsNamed("test-worker-%s")));
        executors.add(executor);
        return executor;
    }

    private ScheduledExecutorService newTimeoutExecutor()
    {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("test-timeout-%s"));
        executors.add(executor);
        return executor;
    }

    private static ChunkDataLease singleMemoryChunkLease()
    {
        return toChunkDataLease(ImmutableList.of(new DataPage(0, 0, Slices.utf8Slice("test-data"))));
    }
}
