/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.base.Ticker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.execution.ChunkDataFactory;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.ExchangeChunkBytes;
import io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.TestingMemoryConfig;
import io.starburst.stargate.buffer.data.server.AddDataPagesInProgressTracker.InProgressLatch;
import io.starburst.stargate.buffer.data.server.AddDataPagesInProgressTracker.Stage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createLocalSpoolingStorage;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestDrainService
{
    @Test
    public void testDrainBroadcastsStateChangesToDiscovery()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            ChunkManager chunkManager = createChunkManager(dataServerStats, executor);
            List<BufferNodeState> broadcastStates = new CopyOnWriteArrayList<>();
            DiscoveryApi discoveryApi = createDiscoveryApiStub(broadcastStates);
            DiscoveryBroadcast discoveryBroadcast = createDiscoveryBroadcast(discoveryApi, bufferNodeStateManager);

            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, chunkManager, Optional.of(discoveryBroadcast));

            drainService.awaitDrain();

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
            assertEventually(() -> assertThat(broadcastStates).containsExactly(BufferNodeState.DRAINING, BufferNodeState.DRAINED));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testDrainFailureStillTransitionsToDrained()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            ChunkManager chunkManager = createChunkManager(dataServerStats, executor);
            // pre-drain the chunk manager directly, so DrainService's own attempt fails with
            // "already started draining" and exercises the exception path in DrainService.drain()
            chunkManager.drainAllChunks();

            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, chunkManager, Optional.empty());

            drainService.awaitDrain();

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testStuckInFlightRequestIsReportedWithThreadDump()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (LogCapture logs = new LogCapture()) {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(dataServerStats);
            DrainService drainService = createDrainService(bufferNodeStateManager, tracker, createChunkManager(dataServerStats, executor), succinctDuration(1, SECONDS), succinctDuration(200, MILLISECONDS));
            InProgressLatch latch = tracker.incrementAndGetLatch("exchange-1", 1, 2, 3);
            latch.enterStage(Stage.AWAITING_PAGE_WRITES);

            drainService.drain();

            assertEventually(() -> assertThat(logs.messages()).anyMatch(message -> message.startsWith("Still 1 in flight addData requests after waiting")));
            // no progress was made, so dump is logged exactly once, before giving up
            assertThat(logs.messages().stream().filter(message -> message.startsWith("No in flight addData request completed in"))).hasSize(1);
            assertThat(logs.messages()).anyMatch(message -> message.startsWith("Thread dump: ") && message.contains(Thread.currentThread().getName()));
            assertThat(logs.messages()).anyMatch(message -> message.startsWith("In flight addData request: exchange=exchange-1, taskId=1, attemptId=2, dataPagesId=3, stage=AWAITING_PAGE_WRITES,"));
            // drain proceeds even though the request never completed
            assertEventually(() -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));
            latch.release();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInFlightRequestReportsCurrentStage()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (LogCapture logs = new LogCapture()) {
            DataServerStats dataServerStats = new DataServerStats();
            AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(dataServerStats);
            DrainService drainService = createDrainService(new BufferNodeStateManager(), tracker, createChunkManager(dataServerStats, executor), succinctDuration(1, SECONDS), succinctDuration(30, SECONDS));
            InProgressLatch latch = tracker.incrementAndGetLatch("exchange-1", 1, 2, 3);
            latch.enterStage(Stage.READING_BODY);
            latch.enterStage(Stage.ADDING_PAGES);

            drainService.drain();

            assertEventually(() -> assertThat(logs.messages()).anyMatch(message -> message.startsWith("In flight addData request:") && message.contains("stage=ADDING_PAGES")));
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("In flight addData request:") && message.contains("stage=READING_BODY"));
            // thread dump threshold is not reached, in flight requests are reported only when giving up
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("Thread dump: "));
            latch.release();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCompletingRequestsBeforeThresholdDoesNotDumpThreads()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (LogCapture logs = new LogCapture()) {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(dataServerStats);
            DrainService drainService = createDrainService(bufferNodeStateManager, tracker, createChunkManager(dataServerStats, executor), succinctDuration(30, SECONDS), succinctDuration(30, SECONDS));
            InProgressLatch first = tracker.incrementAndGetLatch("exchange-1", 1, 0, 1);
            InProgressLatch second = tracker.incrementAndGetLatch("exchange-1", 2, 0, 2);

            drainService.drain();
            assertEventually(() -> assertThat(logs.messages()).contains("Waiting until remaining 2 in flight addData requests complete"));
            first.release();
            assertEventually(() -> assertThat(logs.messages()).contains("Waiting until remaining 1 in flight addData requests complete"));
            second.release();

            assertEventually(() -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("Thread dump: ") || message.startsWith("No in flight addData request completed in"));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testDrainWithoutInFlightRequestsLogsNoDiagnostics()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (LogCapture logs = new LogCapture()) {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, createChunkManager(dataServerStats, executor), Optional.empty());

            drainService.awaitDrain();

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
            assertThat(logs.messages()).contains("No more remaining in flight addData requests");
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("Thread dump: ") || message.startsWith("In flight addData request:") || message.startsWith("No in flight addData request completed in"));
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static DrainService createDrainService(
            BufferNodeStateManager bufferNodeStateManager,
            AddDataPagesInProgressTracker tracker,
            ChunkManager chunkManager,
            Duration maxWaitNoInProgressAddDataPagesRequests,
            Duration noProgressThreadDumpThreshold)
    {
        return new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(10, SECONDS)),
                bufferNodeStateManager,
                tracker,
                chunkManager,
                Optional.empty(),
                maxWaitNoInProgressAddDataPagesRequests,
                noProgressThreadDumpThreshold);
    }

    private static class LogCapture
            implements AutoCloseable
    {
        private final Logger logger = Logger.getLogger(DrainService.class.getName());
        private final SimpleFormatter formatter = new SimpleFormatter();
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final Handler handler = new Handler()
        {
            @Override
            public void publish(LogRecord record)
            {
                messages.add(formatter.formatMessage(record));
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };

        public LogCapture()
        {
            handler.setLevel(Level.ALL);
            logger.addHandler(handler);
        }

        public List<String> messages()
        {
            return messages;
        }

        @Override
        public void close()
        {
            logger.removeHandler(handler);
        }
    }

    private static DrainService createDrainService(BufferNodeStateManager bufferNodeStateManager, DataServerStats dataServerStats, ChunkManager chunkManager, Optional<DiscoveryBroadcast> discoveryBroadcast)
    {
        return new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(10, SECONDS)),
                bufferNodeStateManager,
                new AddDataPagesInProgressTracker(dataServerStats),
                chunkManager,
                discoveryBroadcast);
    }

    @Test
    public void testDrainIdempotency()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            ChunkManager chunkManager = createChunkManager(dataServerStats, executor);
            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, chunkManager, Optional.empty());

            drainService.drain();
            drainService.drain(); // second direct call should be a no-op since draining already started
            drainService.awaitDrain(); // third call inside awaiting should also be a no-op since draining already started

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static DiscoveryBroadcast createDiscoveryBroadcast(DiscoveryApi discoveryApi, BufferNodeStateManager bufferNodeStateManager)
    {
        return new DiscoveryBroadcast(
                new BufferNodeId(0L),
                discoveryApi,
                bufferNodeStateManager,
                () -> new BufferNodeInfo(0L, URI.create("http://buffer-node-0"), Optional.empty(), Optional.empty(), bufferNodeStateManager.getState(), Instant.now()),
                new DataServerConfig());
    }

    private static DiscoveryApi createDiscoveryApiStub(List<BufferNodeState> broadcastStates)
    {
        return new DiscoveryApi()
        {
            @Override
            public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
            {
                broadcastStates.add(bufferNodeInfo.state());
            }

            @Override
            public BufferNodeInfoResponse getBufferNodes()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static ChunkManager createChunkManager(DataServerStats dataServerStats, ExecutorService executor)
    {
        DataServerConfig dataServerConfig = new DataServerConfig()
                .setTraceResourceReportingEnabled(false)
                .setMinDrainingDuration(succinctDuration(0, SECONDS));
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig();
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, DataSize.Unit.MEGABYTE)),
                new MemoryAllocatorConfig(),
                chunkManagerConfig,
                dataServerStats);
        SpoolingStorage spoolingStorage = createLocalSpoolingStorage();
        ExchangeChunkBytes exchangeAllocatedBytes = new ExchangeChunkBytes();
        ChunkDataFactory chunkDataFactory = new ChunkDataFactory(
                Optional.empty(), memoryAllocator, executor, Optional.empty(), exchangeAllocatedBytes, dataServerStats, chunkManagerConfig, dataServerConfig);
        return new ChunkManager(
                new BufferNodeId(0L),
                new BufferNodeStateManager(),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                spoolingStorage,
                Ticker.systemTicker(),
                new SpooledChunksByExchange(),
                exchangeAllocatedBytes,
                Optional.empty(),
                chunkDataFactory,
                dataServerStats,
                _ -> null,
                executor);
    }
}
