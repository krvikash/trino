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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.units.Duration.succinctDuration;
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
        private int updateCount;
        private int failuresRemaining;

        @Override
        public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
        {
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new RuntimeException("Discovery server unavailable");
            }
            updateCount++;
        }

        @Override
        public BufferNodeInfoResponse getBufferNodes()
        {
            throw new UnsupportedOperationException();
        }

        public int getUpdateCount()
        {
            return updateCount;
        }

        public void resetUpdateCount()
        {
            updateCount = 0;
        }

        public void failNextUpdates(int count)
        {
            this.failuresRemaining = count;
        }
    }
}
