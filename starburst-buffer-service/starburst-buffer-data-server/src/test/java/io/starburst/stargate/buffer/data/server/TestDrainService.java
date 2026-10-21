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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
@Isolated("Captures the shared DrainService logger")
public class TestDrainService
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final DataServerStats stats = new DataServerStats();
    private final BufferNodeStateManager stateManager = new BufferNodeStateManager();
    private final List<DrainService> drainServices = new ArrayList<>();
    private final List<DiscoveryBroadcast> discoveryBroadcasts = new ArrayList<>();

    @AfterEach
    public void tearDown()
    {
        drainServices.forEach(DrainService::stop);
        discoveryBroadcasts.forEach(DiscoveryBroadcast::stop);
        executor.close();
    }

    @Test
    public void testDrainBroadcastsStateChangesToDiscovery()
    {
        ChunkManager chunkManager = createChunkManager(stats, executor);
        List<BufferNodeState> broadcastStates = new CopyOnWriteArrayList<>();
        DiscoveryApi discoveryApi = createDiscoveryApiStub(broadcastStates);
        DiscoveryBroadcast discoveryBroadcast = createDiscoveryBroadcast(discoveryApi, stateManager);

        DrainService drainService = createDrainService(stateManager, stats, chunkManager, Optional.of(discoveryBroadcast));

        drainService.awaitDrain();

        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
        assertThat(broadcastStates).contains(BufferNodeState.DRAINED);
    }

    @Test
    public void testAwaitDrainWaitsForFinalAnnouncement()
            throws Exception
    {
        CountDownLatch announcementStarted = new CountDownLatch(1);
        CountDownLatch allowAnnouncement = new CountDownLatch(1);

        List<BufferNodeState> announcedStates = new CopyOnWriteArrayList<>();
        DiscoveryApi discoveryApi = createDiscoveryApiStub(nodeInfo -> {
            if (nodeInfo.state() == BufferNodeState.DRAINED) {
                announcementStarted.countDown();
                await(allowAnnouncement);
            }
            announcedStates.add(nodeInfo.state());
        });
        DrainService drainService = createDrainService(stateManager, stats, createChunkManager(stats, executor), Optional.of(createDiscoveryBroadcast(discoveryApi, stateManager)));
        try {
            Future<?> firstWaiter = executor.submit(drainService::awaitDrain);
            assertThat(announcementStarted.await(10, SECONDS)).isTrue();
            assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED);

            drainService.drain();
            Future<?> secondWaiter = executor.submit(drainService::awaitDrain);
            assertThatThrownBy(() -> firstWaiter.get(100, MILLISECONDS)).isInstanceOf(TimeoutException.class);
            assertThatThrownBy(() -> secondWaiter.get(100, MILLISECONDS)).isInstanceOf(TimeoutException.class);

            // An interrupted waiter must not cancel publication or affect the other waiters.
            Future<Boolean> interruptedWaiter = executor.submit(() -> {
                Thread.currentThread().interrupt();
                drainService.awaitDrain();
                return Thread.interrupted();
            });
            assertThat(interruptedWaiter.get(10, SECONDS)).isTrue();

            allowAnnouncement.countDown();
            firstWaiter.get(10, SECONDS);
            secondWaiter.get(10, SECONDS);
            assertThat(announcedStates).contains(BufferNodeState.DRAINED);
        }
        finally {
            allowAnnouncement.countDown();
            drainService.stop();
        }
    }

    @Test
    public void testBlockedInitialAnnouncementDoesNotBlockDrainingOrShutdown()
            throws Exception
    {
        assertBlockedAnnouncementDoesNotBlockShutdown(BufferNodeState.DRAINING);
    }

    @Test
    public void testBlockedFinalAnnouncementDoesNotBlockShutdown()
            throws Exception
    {
        assertBlockedAnnouncementDoesNotBlockShutdown(BufferNodeState.DRAINED);
    }

    private void assertBlockedAnnouncementDoesNotBlockShutdown(BufferNodeState blockedState)
            throws Exception
    {
        CountDownLatch announcementStarted = new CountDownLatch(1);
        CountDownLatch allowAnnouncement = new CountDownLatch(1);

        DiscoveryApi discoveryApi = createDiscoveryApiStub(nodeInfo -> {
            if (nodeInfo.state() == blockedState) {
                announcementStarted.countDown();
                await(allowAnnouncement);
            }
        });
        AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);
        InProgressLatch inFlightRequest = tracker.incrementAndGetLatch("exchange-1", 1, 0, 1);
        DrainService drainService = new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(500, MILLISECONDS)).setMinDrainingDuration(succinctDuration(0, SECONDS)),
                stateManager,
                tracker,
                createChunkManager(stats, executor),
                Optional.of(createDiscoveryBroadcast(discoveryApi, stateManager)),
                succinctDuration(2, SECONDS),
                succinctDuration(30, SECONDS),
                succinctDuration(100, MILLISECONDS));
        drainServices.add(drainService);
        try (LogCapture logs = new LogCapture()) {
            Future<?> waiter = executor.submit(drainService::awaitDrain);
            if (blockedState == BufferNodeState.DRAINING) {
                // Make sure the initial announcement is in flight before allowing local draining to finish.
                assertThat(announcementStarted.await(10, SECONDS)).isTrue();
            }
            inFlightRequest.release();
            assertThat(announcementStarted.await(10, SECONDS)).isTrue();
            assertEventually(() -> assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED));
            waiter.get(10, SECONDS);
            assertThat(logs.messages()).contains("Timed out waiting for buffer drain and final discovery announcement (state: DRAINED)");

            // A second shutdown hook must not start a new timeout budget.
            executor.submit(drainService::awaitDrain).get(100, MILLISECONDS);
        }
        finally {
            inFlightRequest.release();
            allowAnnouncement.countDown();
            drainService.stop();
        }
    }

    @Test
    public void testFinalPublicationHasTimeAfterDrainingOverrunsItsAllowance()
    {
        assertSlowDrainAnnouncesFinalState(succinctDuration(2, SECONDS), false);
    }

    @Test
    public void testLateDrainStillAttemptsFinalPublication()
    {
        assertSlowDrainAnnouncesFinalState(succinctDuration(0, SECONDS), true);
    }

    private void assertSlowDrainAnnouncesFinalState(Duration publicationTimeout, boolean expectShutdownTimeout)
    {
        List<BufferNodeState> announcedStates = new CopyOnWriteArrayList<>();
        // Simulate local draining taking longer than its 50ms allowance.
        ChunkManager chunkManager = createChunkManager(stats, executor, succinctDuration(200, MILLISECONDS));
        DrainService drainService = createDrainService(
                stateManager,
                stats,
                chunkManager,
                createDiscoveryApiStub(announcedStates),
                succinctDuration(50, MILLISECONDS),
                publicationTimeout);
        try (LogCapture logs = new LogCapture()) {
            drainService.awaitDrain();
            if (expectShutdownTimeout) {
                assertThat(logs.messages()).contains("Timed out waiting for buffer drain and final discovery announcement (state: DRAINING)");
                // Shutdown is allowed to proceed, but if the service is still alive, late completion must try publication.
                assertEventually(() -> assertThat(announcedStates).contains(BufferNodeState.DRAINED));
            }
            else {
                assertThat(announcedStates).contains(BufferNodeState.DRAINED);
                assertThat(logs.messages()).noneMatch(message -> message.startsWith("Timed out waiting") || message.startsWith("Buffer drain finalization failed"));
            }
        }
    }

    @Test
    public void testShutdownAllowanceIncludesMinimumDrainingDuration()
    {
        Duration minDrainingDuration = succinctDuration(2, SECONDS);
        List<BufferNodeState> announcedStates = new CopyOnWriteArrayList<>();
        DrainService drainService = new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(10, MILLISECONDS)).setMinDrainingDuration(minDrainingDuration),
                stateManager,
                new AddDataPagesInProgressTracker(stats),
                createChunkManager(stats, executor, minDrainingDuration),
                Optional.of(createDiscoveryBroadcast(createDiscoveryApiStub(announcedStates), stateManager)),
                succinctDuration(0, SECONDS),
                succinctDuration(30, SECONDS),
                succinctDuration(1, SECONDS));
        drainServices.add(drainService);
        try (LogCapture logs = new LogCapture()) {
            drainService.awaitDrain();
            assertThat(announcedStates).contains(BufferNodeState.DRAINED);
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("Timed out waiting"));
        }
    }

    private static void await(CountDownLatch latch)
    {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private DrainService createDrainService(BufferNodeStateManager stateManager, DataServerStats stats, ChunkManager chunkManager, DiscoveryApi discoveryApi, Duration timeout, Duration publicationTimeout)
    {
        DrainService drainService = new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(timeout).setMinDrainingDuration(succinctDuration(0, SECONDS)),
                stateManager,
                new AddDataPagesInProgressTracker(stats),
                chunkManager,
                Optional.of(createDiscoveryBroadcast(discoveryApi, stateManager)),
                succinctDuration(0, SECONDS),
                succinctDuration(30, SECONDS),
                publicationTimeout);
        drainServices.add(drainService);
        return drainService;
    }

    @Test
    public void testDrainFailureStillTransitionsToDrained()
    {
        ChunkManager chunkManager = createChunkManager(stats, executor);
        // pre-drain the chunk manager directly, so DrainService's own attempt fails with
        // "already started draining" and exercises the exception path in DrainService.drain()
        chunkManager.drainAllChunks();

        List<BufferNodeState> announcedStates = new CopyOnWriteArrayList<>();
        DiscoveryBroadcast discoveryBroadcast = createDiscoveryBroadcast(createDiscoveryApiStub(announcedStates), stateManager);
        DrainService drainService = createDrainService(stateManager, stats, chunkManager, Optional.of(discoveryBroadcast));

        drainService.awaitDrain();

        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
        assertThat(announcedStates).contains(BufferNodeState.DRAINED);
    }

    @Test
    public void testStuckInFlightRequestIsReportedWithThreadDump()
    {
        try (LogCapture logs = new LogCapture()) {
            AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);
            DrainService drainService = createDrainService(stateManager, tracker, createChunkManager(stats, executor), succinctDuration(1, SECONDS), succinctDuration(200, MILLISECONDS));
            InProgressLatch latch = tracker.incrementAndGetLatch("exchange-1", 1, 2, 3);
            latch.enterStage(Stage.AWAITING_PAGE_WRITES);

            drainService.drain();

            assertEventually(() -> assertThat(logs.messages()).anyMatch(message -> message.startsWith("Still 1 in flight addData requests after waiting")));
            // no progress was made, so dump is logged exactly once, before giving up
            assertThat(logs.messages().stream().filter(message -> message.startsWith("No in flight addData request completed in"))).hasSize(1);
            assertThat(logs.messages()).anyMatch(message -> message.startsWith("Thread dump: ") && message.contains(Thread.currentThread().getName()));
            assertThat(logs.messages()).anyMatch(message -> message.startsWith("In flight addData request: exchange=exchange-1, taskId=1, attemptId=2, dataPagesId=3, stage=AWAITING_PAGE_WRITES,"));
            // drain proceeds even though the request never completed
            assertEventually(() -> assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED));
            latch.release();
        }
    }

    @Test
    public void testInFlightRequestReportsCurrentStage()
    {
        try (LogCapture logs = new LogCapture()) {
            AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);
            DrainService drainService = createDrainService(new BufferNodeStateManager(), tracker, createChunkManager(stats, executor), succinctDuration(1, SECONDS), succinctDuration(30, SECONDS));
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
    }

    @Test
    public void testCompletingRequestsBeforeThresholdDoesNotDumpThreads()
    {
        try (LogCapture logs = new LogCapture()) {
            AddDataPagesInProgressTracker tracker = new AddDataPagesInProgressTracker(stats);
            DrainService drainService = createDrainService(stateManager, tracker, createChunkManager(stats, executor), succinctDuration(30, SECONDS), succinctDuration(30, SECONDS));
            InProgressLatch first = tracker.incrementAndGetLatch("exchange-1", 1, 0, 1);
            InProgressLatch second = tracker.incrementAndGetLatch("exchange-1", 2, 0, 2);

            drainService.drain();
            assertEventually(() -> assertThat(logs.messages()).contains("Waiting until remaining 2 in flight addData requests complete"));
            first.release();
            assertEventually(() -> assertThat(logs.messages()).contains("Waiting until remaining 1 in flight addData requests complete"));
            second.release();

            assertEventually(() -> assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED));
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("Thread dump: ") || message.startsWith("No in flight addData request completed in"));
        }
    }

    @Test
    public void testDrainWithoutInFlightRequestsLogsNoDiagnostics()
    {
        try (LogCapture logs = new LogCapture()) {
            DrainService drainService = createDrainService(stateManager, stats, createChunkManager(stats, executor), Optional.empty());

            drainService.awaitDrain();

            assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
            assertThat(logs.messages()).contains("No more remaining in flight addData requests");
            assertThat(logs.messages()).noneMatch(message -> message.startsWith("Thread dump: ") || message.startsWith("In flight addData request:") || message.startsWith("No in flight addData request completed in"));
        }
    }

    private DrainService createDrainService(
            BufferNodeStateManager bufferNodeStateManager,
            AddDataPagesInProgressTracker tracker,
            ChunkManager chunkManager,
            Duration maxWaitNoInProgressAddDataPagesRequests,
            Duration noProgressThreadDumpThreshold)
    {
        DrainService drainService = new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(10, SECONDS)),
                bufferNodeStateManager,
                tracker,
                chunkManager,
                Optional.empty(),
                maxWaitNoInProgressAddDataPagesRequests,
                noProgressThreadDumpThreshold,
                succinctDuration(30, SECONDS));
        drainServices.add(drainService);
        return drainService;
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

    private DrainService createDrainService(BufferNodeStateManager bufferNodeStateManager, DataServerStats dataServerStats, ChunkManager chunkManager, Optional<DiscoveryBroadcast> discoveryBroadcast)
    {
        DrainService drainService = new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(10, SECONDS)),
                bufferNodeStateManager,
                new AddDataPagesInProgressTracker(dataServerStats),
                chunkManager,
                discoveryBroadcast);
        drainServices.add(drainService);
        return drainService;
    }

    @Test
    public void testDrainIdempotency()
    {
        try (LogCapture logs = new LogCapture()) {
            ChunkManager chunkManager = createChunkManager(stats, executor);
            DrainService drainService = createDrainService(stateManager, stats, chunkManager, Optional.empty());

            drainService.drain();
            drainService.drain(); // second direct call should be a no-op since draining already started
            drainService.awaitDrain(); // third call inside awaiting should also be a no-op since draining already started

            assertThat(stateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
            assertThat(logs.messages().stream().filter(message -> message.equals("Draining buffer node has been requested"))).hasSize(1);
        }
    }

    @Test
    public void testAwaitDrainAfterStopLogsStartupFailure()
    {
        try (LogCapture logs = new LogCapture()) {
            DrainService drainService = createDrainService(stateManager, stats, createChunkManager(stats, executor), Optional.empty());
            drainService.stop();

            drainService.awaitDrain();

            assertThat(stateManager.getState()).isEqualTo(BufferNodeState.STARTING);
            assertThat(logs.messages()).contains("Failed to start buffer drain (state: STARTING)");
        }
    }

    @Test
    public void testDrainAfterStopDoesNotChangeState()
    {
        DrainService drainService = createDrainService(stateManager, stats, createChunkManager(stats, executor), Optional.empty());
        drainService.stop();

        assertThatThrownBy(drainService::drain)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Drain service is stopped");
        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.STARTING);
    }

    private DiscoveryBroadcast createDiscoveryBroadcast(DiscoveryApi discoveryApi, BufferNodeStateManager bufferNodeStateManager)
    {
        DiscoveryBroadcast broadcast = new DiscoveryBroadcast(
                new BufferNodeId(0L),
                discoveryApi,
                bufferNodeStateManager,
                () -> new BufferNodeInfo(0L, URI.create("http://buffer-node-0"), Optional.empty(), Optional.empty(), bufferNodeStateManager.getState(), Instant.now()),
                new DataServerConfig().setBroadcastInterval(succinctDuration(10, MILLISECONDS)));
        discoveryBroadcasts.add(broadcast);
        return broadcast;
    }

    private static DiscoveryApi createDiscoveryApiStub(List<BufferNodeState> broadcastStates)
    {
        return createDiscoveryApiStub(nodeInfo -> broadcastStates.add(nodeInfo.state()));
    }

    private static DiscoveryApi createDiscoveryApiStub(Consumer<BufferNodeInfo> update)
    {
        return new DiscoveryApi()
        {
            @Override
            public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
            {
                update.accept(bufferNodeInfo);
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
        return createChunkManager(dataServerStats, executor, succinctDuration(0, SECONDS));
    }

    private static ChunkManager createChunkManager(DataServerStats dataServerStats, ExecutorService executor, Duration minDrainingDuration)
    {
        DataServerConfig dataServerConfig = new DataServerConfig()
                .setTraceResourceReportingEnabled(false)
                .setMinDrainingDuration(minDrainingDuration);
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
