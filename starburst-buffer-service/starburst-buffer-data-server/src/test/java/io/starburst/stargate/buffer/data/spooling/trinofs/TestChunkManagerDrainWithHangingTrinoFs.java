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

import com.google.common.base.VerifyException;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.Slices;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.execution.ChunkDataFactory;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.ExchangeChunkBytes;
import io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.TestingMemoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestChunkManagerDrainWithHangingTrinoFs
{
    private static final String EXCHANGE_ID = "exchange-drain-test";

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<ExecutorService> spoolingExecutors = new ArrayList<>();

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
        spoolingExecutors.forEach(ExecutorService::shutdownNow);
        spoolingExecutors.clear();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDrainCompletesWithinTimeoutWhenFilesystemHangs()
            throws Exception
    {
        HangingFileSystem fileSystem = new HangingFileSystem();
        TrinoFsSpoolingStorage spoolingStorage = createTrinoFsStorage(fileSystem, new Duration(200, TimeUnit.MILLISECONDS));

        DataSize maxMemory = DataSize.of(64, MEGABYTE);
        DataSize chunkSize = DataSize.of(1, MEGABYTE);
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(maxMemory),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());

        // Short drain timeout so the test completes fast; 3s total / 2 attempts = 1.5s per await.
        Duration drainAllChunksTimeout = new Duration(3, TimeUnit.SECONDS);
        int drainingMaxAttempts = 2;

        ChunkManager chunkManager = createChunkManager(
                memoryAllocator, spoolingStorage, chunkSize, drainAllChunksTimeout, drainingMaxAttempts);

        chunkManager.registerExchange(EXCHANGE_ID, ChunkDeliveryMode.STANDARD, Optional.empty());
        // drainAllChunks finishes open chunks before spooling, so any in-flight data suffices.
        chunkManager.addDataPages(
                EXCHANGE_ID,
                0,
                0,
                0,
                0L,
                List.of(Slices.utf8Slice("x".repeat(1024 * 512))));

        // drainAllChunks() must return (not hang) — the @Timeout above is the guard.
        // It throws VerifyException because chunks remain unspooled after all retries exhaust.
        assertThatThrownBy(chunkManager::drainAllChunks)
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("chunks exist after spooling all chunks");

        assertThat(chunkManager.getSpooledChunksCount())
                .describedAs("hanging filesystem must not produce successful spooled chunks")
                .isEqualTo(0);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDrainReleasesLeasesWhenCancellableWritesHang()
            throws Exception
    {
        HangingFileSystem fileSystem = new HangingFileSystem();
        // no per-operation timeout in play; the writes are only unblocked by the cancellation that follows the drain timeout
        TrinoFsSpoolingStorage spoolingStorage = createTrinoFsStorage(fileSystem, new Duration(1, TimeUnit.HOURS));

        MemoryAllocator memoryAllocator = createMemoryAllocator();
        long freeMemoryBeforeDrain = memoryAllocator.getFreeMemory();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator, spoolingStorage, DataSize.of(1, MEGABYTE), new Duration(3, TimeUnit.SECONDS), 2);
        addChunkData(chunkManager);

        assertThatThrownBy(chunkManager::drainAllChunks)
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("chunks exist after spooling all chunks");

        assertThat(fileSystem.interrupted.await(10, TimeUnit.SECONDS))
                .describedAs("hanging write must be cancelled")
                .isTrue();
        // a lease left behind by a cancelled write keeps a reference to the chunk memory, so dropping the exchange would not free it
        chunkManager.removeExchange(EXCHANGE_ID);
        assertThat(memoryAllocator.getFreeMemory())
                .describedAs("leases of cancelled writes must be released")
                .isEqualTo(freeMemoryBeforeDrain);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testDrainRetriesChunksOfCancelledWrites()
            throws Exception
    {
        Path rootPath = Files.createTempDirectory("drain-retry");
        HangFirstWriteFileSystem fileSystem = new HangFirstWriteFileSystem(new LocalFileSystemFactory(rootPath).create(ConnectorIdentity.ofUser("buffer")));
        // no per-operation timeout in play; the first write is only unblocked by the cancellation that follows the drain timeout
        TrinoFsSpoolingStorage spoolingStorage = createTrinoFsStorage(fileSystem, "local:///", new Duration(1, TimeUnit.HOURS));

        MemoryAllocator memoryAllocator = createMemoryAllocator();
        long freeMemoryBeforeDrain = memoryAllocator.getFreeMemory();
        ChunkManager chunkManager = createChunkManager(
                memoryAllocator, spoolingStorage, DataSize.of(1, MEGABYTE), new Duration(6, TimeUnit.SECONDS), 2);
        addChunkData(chunkManager);

        // the first attempt times out and its write is cancelled; the chunks are not released, so the second attempt spools them
        Future<?> drainFuture = executor.submit(chunkManager::drainAllChunks);

        assertThat(fileSystem.interrupted.await(10, TimeUnit.SECONDS))
                .describedAs("hanging write must be cancelled")
                .isTrue();
        assertEventually(
                new Duration(30, TimeUnit.SECONDS),
                () -> assertThat(chunkManager.getSpooledChunksCount())
                        .describedAs("chunks of the cancelled write must be spooled by the retry")
                        .isGreaterThan(0));

        // simulate Trino acknowledging all closed chunks so that draining completes
        chunkManager.markAllClosedChunksReceived(EXCHANGE_ID);
        assertThat(drainFuture).succeedsWithin(30, TimeUnit.SECONDS);

        chunkManager.removeExchange(EXCHANGE_ID);
        assertThat(memoryAllocator.getFreeMemory())
                .describedAs("leases retained for the retry must be released once draining finishes")
                .isEqualTo(freeMemoryBeforeDrain);
    }

    private static MemoryAllocator createMemoryAllocator()
    {
        return new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, MEGABYTE)),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());
    }

    private static void addChunkData(ChunkManager chunkManager)
    {
        chunkManager.registerExchange(EXCHANGE_ID, ChunkDeliveryMode.STANDARD, Optional.empty());
        // drainAllChunks finishes open chunks before spooling, so any in-flight data suffices.
        chunkManager.addDataPages(
                EXCHANGE_ID,
                0,
                0,
                0,
                0L,
                List.of(Slices.utf8Slice("x".repeat(1024 * 512))));
    }

    private TrinoFsSpoolingStorage createTrinoFsStorage(TrinoFileSystem fileSystem, Duration operationTimeout)
    {
        return createTrinoFsStorage(fileSystem, "s3://bucket/", operationTimeout);
    }

    private TrinoFsSpoolingStorage createTrinoFsStorage(TrinoFileSystem fileSystem, String spoolingDirectory, Duration operationTimeout)
    {
        return new TrinoFsSpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory(spoolingDirectory),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                fileSystem,
                newWorkerExecutor(),
                newWorkerExecutor(),
                new TrinoFsSpoolingConfig().setOperationTimeout(operationTimeout),
                newTimeoutExecutor());
    }

    private ChunkManager createChunkManager(
            MemoryAllocator memoryAllocator,
            TrinoFsSpoolingStorage spoolingStorage,
            DataSize chunkSize,
            Duration drainAllChunksTimeout,
            int drainingMaxAttempts)
    {
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig()
                .setChunkTargetSize(chunkSize)
                .setChunkMaxSize(chunkSize)
                .setChunkSliceSize(DataSize.of(256, DataSize.Unit.KILOBYTE))
                .setChunkSpoolInterval(succinctDuration(100, TimeUnit.SECONDS))
                .setChunkSpoolConcurrency(4);
        DataServerConfig dataServerConfig = new DataServerConfig()
                .setTraceResourceReportingEnabled(false)
                .setDataIntegrityVerificationEnabled(false)
                .setMinDrainingDuration(succinctDuration(0, TimeUnit.SECONDS))
                .setChunkListPollTimeout(succinctDuration(5, TimeUnit.MILLISECONDS))
                .setDrainAllChunksTimeout(drainAllChunksTimeout)
                .setDrainingMaxAttempts(drainingMaxAttempts);
        ExchangeChunkBytes exchangeAllocatedBytes = new ExchangeChunkBytes();
        ChunkDataFactory chunkDataFactory = new ChunkDataFactory(
                Optional.empty(),
                memoryAllocator,
                executor,
                Optional.empty(),
                exchangeAllocatedBytes,
                new DataServerStats(),
                chunkManagerConfig,
                dataServerConfig);
        return new ChunkManager(
                new BufferNodeId(0L),
                new BufferNodeStateManager(),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                spoolingStorage,
                new TestingTicker(),
                new SpooledChunksByExchange(),
                exchangeAllocatedBytes,
                Optional.empty(),
                chunkDataFactory,
                new DataServerStats(),
                new Tracer()
                {
                    @Override
                    public SpanBuilder spanBuilder(String spanName)
                    {
                        return null;
                    }
                },
                executor);
    }

    private ListeningExecutorService newWorkerExecutor()
    {
        ExecutorService executor = Executors.newSingleThreadExecutor(daemonThreadsNamed("test-worker-%s"));
        spoolingExecutors.add(executor);
        return listeningDecorator(executor);
    }

    private ScheduledExecutorService newTimeoutExecutor()
    {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("test-timeout-%s"));
        executor.setRemoveOnCancelPolicy(true);
        spoolingExecutors.add(executor);
        return executor;
    }
}
