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

import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.InvalidBufferNodeUpdateException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.airlift.units.Duration.succinctDuration;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDiscoveryBroadcast
{
    private static final long NODE_ID = 42L;
    private static final URI BASE_URI = URI.create("http://localhost:8080");

    private BufferNodeStateManager stateManager;
    private TestingDiscoveryApi discoveryApi;
    private DataServerConfig config;

    @BeforeEach
    void setUp()
    {
        stateManager = new BufferNodeStateManager();
        stateManager.transitionState(BufferNodeState.STARTED);
        discoveryApi = new TestingDiscoveryApi();
        config = new DataServerConfig()
                .setBroadcastInterval(succinctDuration(100, MILLISECONDS))
                .setBroadcastFailureInactivityThreshold(succinctDuration(30, SECONDS));
    }

    @Test
    void testNodeLifecycle()
    {
        BufferNodeStateManager lifecycleStateManager = new BufferNodeStateManager();
        DiscoveryBroadcast broadcast = createBroadcast(lifecycleStateManager, () -> nodeInfo(lifecycleStateManager.getState()));

        // STARTING: broadcast is skipped
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.STARTING);
        broadcast.broadcast();
        assertThat(discoveryApi.getUpdateCount()).isZero();
        assertThat(broadcast.isRegistered()).isFalse();

        // STARTED: first broadcast transitions to ACTIVE and calls updateBufferNode twice —
        // once with the initial state, once immediately after the ACTIVE transition
        lifecycleStateManager.transitionState(BufferNodeState.STARTED);
        broadcast.broadcast();
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE);
        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(2);
        discoveryApi.resetUpdateCount();

        // ACTIVE: subsequent broadcasts send a single update, no further state transitions
        broadcast.broadcast();
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE);
        discoveryApi.resetUpdateCount();

        // DRAINING: node keeps reporting its state to discovery
        lifecycleStateManager.transitionState(BufferNodeState.DRAINING);
        broadcast.broadcast();
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.DRAINING);
        discoveryApi.resetUpdateCount();

        // DRAINED
        lifecycleStateManager.transitionState(BufferNodeState.DRAINED);
        broadcast.broadcast();
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
    }

    @Test
    void testEarlyDrainingLifecycle()
    {
        // Drain starts before the node ever registers with discovery.
        // broadcast() must not attempt transition to ACTIVE when the current state does not allow it.
        BufferNodeStateManager lifecycleStateManager = new BufferNodeStateManager();
        DiscoveryBroadcast broadcast = createBroadcast(lifecycleStateManager, () -> nodeInfo(lifecycleStateManager.getState()));

        // STARTING: broadcast is skipped
        broadcast.broadcast();
        assertThat(discoveryApi.getUpdateCount()).isZero();
        assertThat(broadcast.isRegistered()).isFalse();

        // DRAINING starts before the node reaches ACTIVE
        lifecycleStateManager.transitionState(BufferNodeState.STARTED);
        lifecycleStateManager.transitionState(BufferNodeState.DRAINING);

        // First broadcast while DRAINING
        broadcast.broadcast();
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.DRAINING);
        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
        discoveryApi.resetUpdateCount();

        // Verify lastSuccessfulBroadcast was properly set:
        // a failure within the threshold must mark the node unregistered
        discoveryApi.failNextUpdates(1);
        broadcast.broadcast();
        assertThat(broadcast.isRegistered()).isFalse();

        broadcast.broadcast();
        assertThat(broadcast.isRegistered()).isTrue();
        discoveryApi.resetUpdateCount();

        // DRAINED: node keeps reporting to discovery
        lifecycleStateManager.transitionState(BufferNodeState.DRAINED);
        broadcast.broadcast();
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(lifecycleStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
    }

    @Test
    void testBroadcastSkippedWhenNodeStateIsStarting()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(BufferNodeState.STARTING));
        broadcast.broadcast();

        assertThat(discoveryApi.getUpdateCount()).isZero();
        assertThat(broadcast.isRegistered()).isFalse();
    }

    @Test
    void testFailureWithNoPriorSuccessDoesNotMarkUnregistered()
    {
        // With no prior successful broadcast, lastSuccessfulBroadcast is at epoch.
        // epoch + threshold is way before now, so the inactivity condition is false
        // and registration state is not changed (remains unregistered).
        discoveryApi.failNextUpdates(1);
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(BufferNodeState.STARTED));
        broadcast.broadcast();

        assertThat(broadcast.isRegistered()).isFalse();
        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.STARTED);
    }

    @Test
    void testBroadcastFailureAndRecovery()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(BufferNodeState.STARTED));
        registerNode(broadcast);

        discoveryApi.failNextUpdates(1);
        broadcast.broadcast();
        assertThat(broadcast.isRegistered()).isFalse();
        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.ACTIVE);

        broadcast.broadcast();
        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.ACTIVE);
    }

    @Test
    void testBroadcastFailureBeyondInactivityThresholdDoesNotMarkUnregistered()
    {
        // With a 0ms threshold, lastSuccessfulBroadcast + 0 is never after Instant.now(),
        // so the unregistered branch is never entered regardless of how recently we succeeded.
        DataServerConfig zeroThresholdConfig = new DataServerConfig()
                .setBroadcastInterval(succinctDuration(100, MILLISECONDS))
                .setBroadcastFailureInactivityThreshold(succinctDuration(0, MILLISECONDS));
        DiscoveryBroadcast broadcast = createBroadcast(stateManager, () -> nodeInfo(BufferNodeState.STARTED), zeroThresholdConfig);
        registerNode(broadcast);

        discoveryApi.failNextUpdates(1);
        broadcast.broadcast();

        assertThat(broadcast.isRegistered()).isTrue();
        assertThat(stateManager.getState()).isEqualTo(BufferNodeState.ACTIVE);
    }

    @Test
    void testDrainedAnnouncementWaitsForInFlightDrainingAnnouncement()
            throws Exception
    {
        stateManager.transitionState(BufferNodeState.DRAINING);
        CountDownLatch drainingAnnouncementStarted = new CountDownLatch(1);
        CountDownLatch allowDrainingAnnouncement = new CountDownLatch(1);
        List<BufferNodeState> announcedStates = new CopyOnWriteArrayList<>();
        DiscoveryApi blockingDiscoveryApi = new TestingDiscoveryApi()
        {
            @Override
            public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
            {
                if (bufferNodeInfo.state() == BufferNodeState.DRAINING) {
                    drainingAnnouncementStarted.countDown();
                    try {
                        assertThat(allowDrainingAnnouncement.await(10, SECONDS)).isTrue();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                announcedStates.add(bufferNodeInfo.state());
            }
        };
        DiscoveryBroadcast broadcast = new DiscoveryBroadcast(
                new BufferNodeId(NODE_ID),
                blockingDiscoveryApi,
                stateManager,
                () -> nodeInfo(stateManager.getState()),
                config);
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            Future<?> drainingAnnouncement = executor.submit(broadcast::broadcast);
            assertThat(drainingAnnouncementStarted.await(10, SECONDS)).isTrue();
            // Local draining may finish while discovery is still publishing DRAINING.
            stateManager.transitionState(BufferNodeState.DRAINED);
            CountDownLatch drainedAnnouncementStarted = new CountDownLatch(1);
            Future<?> drainedAnnouncement = executor.submit(() -> {
                drainedAnnouncementStarted.countDown();
                broadcast.broadcast();
            });
            assertThat(drainedAnnouncementStarted.await(10, SECONDS)).isTrue();
            assertThatThrownBy(() -> drainedAnnouncement.get(100, MILLISECONDS)).isInstanceOf(TimeoutException.class);

            allowDrainingAnnouncement.countDown();
            drainingAnnouncement.get(10, SECONDS);
            drainedAnnouncement.get(10, SECONDS);
            // Reversing this order would leave discovery reporting DRAINING after the node shuts down.
            assertThat(announcedStates).containsExactly(BufferNodeState.DRAINING, BufferNodeState.DRAINED);
        }
        finally {
            allowDrainingAnnouncement.countDown();
            broadcast.stop();
            executor.shutdownNow();
        }
    }

    @Test
    void testFinalAnnouncementIsAttemptedWithNoRemainingAllowance()
            throws Exception
    {
        stateManager.transitionState(BufferNodeState.DRAINING);
        stateManager.transitionState(BufferNodeState.DRAINED);
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));

        broadcast.broadcastUntilSuccess(succinctDuration(0, SECONDS));

        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
    }

    @Test
    void testFinalAnnouncementRetriesTransientFailures()
            throws Exception
    {
        stateManager.transitionState(BufferNodeState.DRAINING);
        stateManager.transitionState(BufferNodeState.DRAINED);
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        discoveryApi.failNextUpdates(2);

        broadcast.broadcastUntilSuccess(succinctDuration(10, SECONDS));

        assertThat(discoveryApi.updateAttempts.get()).isEqualTo(3);
        assertThat(discoveryApi.getUpdateCount()).isEqualTo(1);
    }

    @Test
    void testUnavailableDiscoveryTimesOut()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        discoveryApi.failNextUpdates(Integer.MAX_VALUE);

        assertThatThrownBy(() -> broadcast.broadcastUntilSuccess(succinctDuration(250, MILLISECONDS)))
                .isInstanceOf(TimeoutException.class)
                .hasMessage("Timed out publishing buffer node announcement");
        assertThat(discoveryApi.updateAttempts.get()).isGreaterThan(1);
        assertThat(discoveryApi.getUpdateCount()).isZero();
    }

    @Test
    void testFinalAnnouncementDoesNotRetryInvalidUpdate()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        discoveryApi.rejectNextUpdate = true;

        assertThatThrownBy(() -> broadcast.broadcastUntilSuccess(succinctDuration(10, SECONDS)))
                .isInstanceOf(InvalidBufferNodeUpdateException.class)
                .hasMessage("Node already registered with a different URI");
        assertThat(discoveryApi.updateAttempts.get()).isEqualTo(1);
    }

    @Test
    void testInvalidUpdateDoesNotStopPeriodicBroadcasts()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        discoveryApi.rejectNextUpdate = true;
        try {
            broadcast.start();
            assertEventually(() -> assertThat(discoveryApi.getUpdateCount()).isGreaterThanOrEqualTo(2));
            assertThat(broadcast.isRegistered()).isTrue();
        }
        finally {
            broadcast.stop();
        }
    }

    @Test
    void testTransientFailureDoesNotStopPeriodicBroadcasts()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        discoveryApi.failNextUpdates(1);
        try {
            broadcast.start();
            assertEventually(() -> assertThat(discoveryApi.getUpdateCount()).isGreaterThanOrEqualTo(2));
            assertThat(broadcast.isRegistered()).isTrue();
        }
        finally {
            broadcast.stop();
        }
    }

    @Test
    void testRequestAfterStopIsBestEffort()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        broadcast.stop();

        broadcast.requestBroadcast();

        assertThat(discoveryApi.updateAttempts.get()).isZero();
    }

    @Test
    void testInterruptedFinalAnnouncementDoesNotRetry()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(stateManager.getState()));
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> broadcast.broadcastUntilSuccess(succinctDuration(10, SECONDS)))
                    .isInstanceOf(InterruptedException.class);
            assertThat(discoveryApi.updateAttempts.get()).isZero();
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void testStopCannotBeCalledTwice()
    {
        DiscoveryBroadcast broadcast = createBroadcast(() -> nodeInfo(BufferNodeState.STARTED));
        broadcast.stop();

        assertThatThrownBy(broadcast::stop)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already stopped");
    }

    private DiscoveryBroadcast createBroadcast(Supplier<BufferNodeInfo> supplier)
    {
        return createBroadcast(stateManager, supplier, config);
    }

    private DiscoveryBroadcast createBroadcast(BufferNodeStateManager manager, Supplier<BufferNodeInfo> supplier)
    {
        return createBroadcast(manager, supplier, config);
    }

    private DiscoveryBroadcast createBroadcast(BufferNodeStateManager manager, Supplier<BufferNodeInfo> supplier, DataServerConfig serverConfig)
    {
        return new DiscoveryBroadcast(
                new BufferNodeId(NODE_ID),
                discoveryApi,
                manager,
                supplier,
                serverConfig);
    }

    private void registerNode(DiscoveryBroadcast broadcast)
    {
        broadcast.broadcast();
        discoveryApi.resetUpdateCount();
    }

    private BufferNodeInfo nodeInfo(BufferNodeState state)
    {
        return new BufferNodeInfo(NODE_ID, BASE_URI, Optional.empty(), Optional.empty(), state, Instant.now());
    }

    private static class TestingDiscoveryApi
            implements DiscoveryApi
    {
        private final AtomicInteger updateAttempts = new AtomicInteger();
        private final AtomicInteger updateCount = new AtomicInteger();
        private int failuresRemaining;
        private boolean rejectNextUpdate;

        @Override
        public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
        {
            updateAttempts.incrementAndGet();
            if (rejectNextUpdate) {
                rejectNextUpdate = false;
                throw new InvalidBufferNodeUpdateException("Node already registered with a different URI");
            }
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new RuntimeException("Discovery server unavailable");
            }
            updateCount.incrementAndGet();
        }

        @Override
        public BufferNodeInfoResponse getBufferNodes()
        {
            throw new UnsupportedOperationException();
        }

        public int getUpdateCount()
        {
            return updateCount.get();
        }

        public void resetUpdateCount()
        {
            updateCount.set(0);
        }

        public void failNextUpdates(int count)
        {
            this.failuresRemaining = count;
        }
    }
}
