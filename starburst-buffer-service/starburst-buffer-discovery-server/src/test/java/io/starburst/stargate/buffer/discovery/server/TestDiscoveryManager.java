/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import io.airlift.testing.TestingTicker;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.InvalidBufferNodeUpdateException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.DRAINED;
import static io.starburst.stargate.buffer.BufferNodeState.DRAINING;
import static io.starburst.stargate.buffer.discovery.server.DiscoveryManager.DRAINED_NODES_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.discovery.server.DiscoveryManagerConfig.DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDiscoveryManager
{
    private static final long NODE_ID = 1L;
    private static final URI NODE_URI = URI.create("http://node-1:8080");

    @Test
    public void testNodeLostWhileActiveRecordsNodeLostWithoutDrain()
            throws InvalidBufferNodeUpdateException
    {
        TestingTicker ticker = new TestingTicker();
        DiscoveryStats discoveryStats = new DiscoveryStats();
        DiscoveryManager discoveryManager = new DiscoveryManager(ticker, new DiscoveryManagerConfig(), discoveryStats);

        updateNodeInfo(discoveryManager, ACTIVE);
        assertThat(discoveryManager.getNodeInfos()).hasSize(1);

        // advance past the staleness threshold without any further update from the node
        ticker.increment(DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD.roundTo(SECONDS) + 1, SECONDS);
        discoveryManager.cleanup();

        assertThat(discoveryManager.getNodeInfos()).isEmpty();
        assertThat(discoveryStats.getNodeStateStats().getNodesLostWithoutDrain().getTotalCount()).isEqualTo(1);
    }

    @Test
    public void testNodeLostWhileDrainingDoesNotRecordNodeLostWithoutDrain()
            throws InvalidBufferNodeUpdateException
    {
        TestingTicker ticker = new TestingTicker();
        DiscoveryStats discoveryStats = new DiscoveryStats();
        DiscoveryManager discoveryManager = new DiscoveryManager(ticker, new DiscoveryManagerConfig(), discoveryStats);

        updateNodeInfo(discoveryManager, DRAINING);

        ticker.increment(DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD.roundTo(SECONDS) + 1, SECONDS);
        discoveryManager.cleanup();

        assertThat(discoveryManager.getNodeInfos()).isEmpty();
        assertThat(discoveryStats.getNodeStateStats().getNodesLostWithoutDrain().getTotalCount()).isEqualTo(0);
    }

    @Test
    public void testNodeLostWhileDrainedDoesNotRecordNodeLostWithoutDrain()
            throws InvalidBufferNodeUpdateException
    {
        TestingTicker ticker = new TestingTicker();
        DiscoveryStats discoveryStats = new DiscoveryStats();
        DiscoveryManager discoveryManager = new DiscoveryManager(ticker, new DiscoveryManagerConfig(), discoveryStats);

        updateNodeInfo(discoveryManager, DRAINED);

        // drained nodes are only marked stale after the longer DRAINED_NODES_STALENESS_THRESHOLD
        ticker.increment(DRAINED_NODES_STALENESS_THRESHOLD.roundTo(SECONDS) + 1, SECONDS);
        discoveryManager.cleanup();

        assertThat(discoveryManager.getNodeInfos()).isEmpty();
        assertThat(discoveryStats.getNodeStateStats().getNodesLostWithoutDrain().getTotalCount()).isEqualTo(0);
    }

    @Test
    public void testDrainedNodeNotMarkedStaleBeforeExtraThreshold()
            throws InvalidBufferNodeUpdateException
    {
        TestingTicker ticker = new TestingTicker();
        DiscoveryStats discoveryStats = new DiscoveryStats();
        DiscoveryManager discoveryManager = new DiscoveryManager(ticker, new DiscoveryManagerConfig(), discoveryStats);

        updateNodeInfo(discoveryManager, DRAINED);

        // past the regular staleness threshold, but well within DRAINED_NODES_STALENESS_THRESHOLD
        ticker.increment(DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD.roundTo(SECONDS) + 1, SECONDS);
        discoveryManager.cleanup();

        assertThat(discoveryManager.getNodeInfos()).hasSize(1);
        assertThat(discoveryStats.getNodeStateStats().getNodesLostWithoutDrain().getTotalCount()).isEqualTo(0);
    }

    private void updateNodeInfo(DiscoveryManager discoveryManager, BufferNodeState state)
            throws InvalidBufferNodeUpdateException
    {
        discoveryManager.updateNodeInfos(new BufferNodeInfo(NODE_ID, NODE_URI, Optional.empty(), Optional.empty(), state, Instant.now()));
    }
}
