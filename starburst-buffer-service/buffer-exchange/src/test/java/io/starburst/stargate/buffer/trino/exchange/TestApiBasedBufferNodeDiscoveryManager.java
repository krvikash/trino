/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.testing.TestingTicker;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.trino.exchange.BufferNodeDiscoveryManager.BufferNodesState;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.internal.Iterables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.DRAINED;
import static io.starburst.stargate.buffer.BufferNodeState.STARTED;
import static io.starburst.stargate.buffer.BufferNodeState.STARTING;
import static io.starburst.stargate.buffer.trino.exchange.ApiBasedBufferNodeDiscoveryManager.DRAINED_NODES_KEEP_TIMEOUT;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestApiBasedBufferNodeDiscoveryManager
{
    private ListeningScheduledExecutorService executor;

    @BeforeAll
    public void setup()
    {
        executor = MoreExecutors.listeningDecorator(newScheduledThreadPool(4, daemonThreadsNamed("test-api-based-node-discovery-manager-%s")));
    }

    @AfterAll
    public void teardown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Test
    public void test()
            throws ExecutionException, InterruptedException
    {
        TestingDiscoveryApi discoveryApi = new TestingDiscoveryApi();
        TestingTicker ticker = new TestingTicker();
        ApiBasedBufferNodeDiscoveryManager discoveryManager = new ApiBasedBufferNodeDiscoveryManager(
                discoveryApi.getApiFactory(),
                executor,
                succinctDuration(0, MILLISECONDS), // always allow force refresh
                succinctDuration(10, MILLISECONDS),
                ticker);
        discoveryManager.start();

        assertThatThrownBy(discoveryManager::getBufferNodes).hasMessage("Could not get initial cluster state");

        discoveryApi.setComplete(true);
        discoveryApi.updateBufferNode(bufferNode(1, STARTING));
        discoveryApi.updateBufferNode(bufferNode(2, STARTING));
        discoveryApi.updateBufferNode(bufferNode(3, STARTING));

        discoveryManager.forceRefresh().get();
        BufferNodesState bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 3)
                .hasActiveNodes()
                .hasNodesInState(STARTING, 1, 2, 3);

        // some nodes transition further
        discoveryApi.updateBufferNode(bufferNode(2, STARTED));
        discoveryApi.updateBufferNode(bufferNode(3, ACTIVE));

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 3)
                .hasActiveNodes(3)
                .hasNodesInState(STARTING, 1)
                .hasNodesInState(STARTED, 2)
                .hasNodesInState(ACTIVE, 3);

        // all active
        discoveryApi.updateBufferNode(bufferNode(1, ACTIVE));
        discoveryApi.updateBufferNode(bufferNode(2, ACTIVE));

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 3)
                .hasActiveNodes(1, 2, 3)
                .hasNodesInState(ACTIVE, 1, 2, 3);

        // new node added
        discoveryApi.updateBufferNode(bufferNode(4, STARTING));

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 3, 4)
                .hasActiveNodes(1, 2, 3)
                .hasNodesInState(ACTIVE, 1, 2, 3)
                .hasNodesInState(STARTING, 4);

        // a node disappeared - not expected but possible due to failure
        discoveryApi.deleteBufferNode(3);

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 4)
                .hasActiveNodes(1, 2)
                .hasNodesInState(ACTIVE, 1, 2)
                .hasNodesInState(STARTING, 4);

        // empty incomplete state reported (discovery server restart)
        discoveryApi.reset();

        // no changes to the state
        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 4)
                .hasActiveNodes(1, 2)
                .hasNodesInState(ACTIVE, 1, 2)
                .hasNodesInState(STARTING, 4);

        // still incomplete state but one of the nodes updated
        discoveryApi.updateBufferNode(bufferNode(4, ACTIVE));

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 4)
                .hasActiveNodes(1, 2, 4)
                .hasNodesInState(ACTIVE, 1, 2, 4);

        // we get complete response and one of nodes transitioned to DRAINED
        discoveryApi.setComplete(true);
        discoveryApi.updateBufferNode(bufferNode(1, ACTIVE));
        discoveryApi.updateBufferNode(bufferNode(2, ACTIVE));
        discoveryApi.updateBufferNode(bufferNode(4, DRAINED));

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 4)
                .hasActiveNodes(1, 2)
                .hasNodesInState(ACTIVE, 1, 2)
                .hasNodesInState(DRAINED, 4);

        // drained node is cached even if discovery server stop sending it to us
        discoveryApi.deleteBufferNode(4);

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 4)
                .hasActiveNodes(1, 2)
                .hasNodesInState(ACTIVE, 1, 2)
                .hasNodesInState(DRAINED, 4);

        // node is still there for long time
        ticker.increment(DRAINED_NODES_KEEP_TIMEOUT.toMillis() - 1, MILLISECONDS);

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2, 4)
                .hasActiveNodes(1, 2)
                .hasNodesInState(ACTIVE, 1, 2)
                .hasNodesInState(DRAINED, 4);

        // but disappears eventually
        ticker.increment(2, MILLISECONDS);

        discoveryManager.forceRefresh().get();
        bufferNodes = discoveryManager.getBufferNodes();
        assertBufferNodes(bufferNodes)
                .hasAllNodes(1, 2)
                .hasActiveNodes(1, 2)
                .hasNodesInState(ACTIVE, 1, 2);
    }

    private static BufferNodeInfo bufferNode(int nodeId, BufferNodeState state)
    {
        Optional<BufferNodeStats> stats = state == ACTIVE ? Optional.of(new BufferNodeStats(1, 2, 3, 4, 5, 6, 7, 8, 9)) : Optional.empty();
        return new BufferNodeInfo(nodeId, URI.create("http://blah" + nodeId), stats, state, Instant.now());
    }

    private static class BufferNodesStateAssert
            extends AbstractObjectAssert<BufferNodesStateAssert, BufferNodesState>
    {
        protected Iterables iterables = Iterables.instance();

        public BufferNodesStateAssert(BufferNodesState bufferNodesState)
        {
            super(bufferNodesState, BufferNodesStateAssert.class);
        }

        public BufferNodesStateAssert hasNoNodes()
        {
            return hasAllNodes();
        }

        public BufferNodesStateAssert hasActiveNodes(long... expectedActiveNodeIds)
        {
            isNotNull();
            Set<Long> actualActiveNodeIds = actual.getActiveBufferNodes().keySet();
            iterables.assertContainsExactlyInAnyOrder(info, actualActiveNodeIds, asLongs(expectedActiveNodeIds));
            return this;
        }

        public BufferNodesStateAssert hasAllNodes(long... expectedAllNodeIds)
        {
            isNotNull();
            Set<Long> actualAllNodeIds = actual.getAllBufferNodes().keySet();
            iterables.assertContainsExactlyInAnyOrder(info, actualAllNodeIds, asLongs(expectedAllNodeIds));
            return this;
        }

        public BufferNodesStateAssert hasNodesInState(BufferNodeState nodeState, long... expectedNodesInState)
        {
            isNotNull();
            Set<Long> actualNodesInState = actual.getAllBufferNodes().entrySet().stream()
                    .filter(entry -> entry.getValue().state() == nodeState)
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());
            iterables.assertContainsExactlyInAnyOrder(info, actualNodesInState, asLongs(expectedNodesInState));
            return this;
        }

        private Long[] asLongs(long... nodeIds)
        {
            Long[] longNodeIds = new Long[nodeIds.length];
            for (int i = 0; i < nodeIds.length; i++) {
                longNodeIds[i] = (long) nodeIds[i];
            }
            return longNodeIds;
        }
    }

    private static BufferNodesStateAssert assertBufferNodes(BufferNodesState bufferNodesState)
    {
        return new BufferNodesStateAssert(bufferNodesState);
    }

    private static class TestingDiscoveryApi
            implements DiscoveryApi
    {
        private volatile boolean complete;
        private final Map<Long, BufferNodeInfo> bufferNodeInfos = new ConcurrentHashMap<>();

        public ApiFactory getApiFactory()
        {
            return new ApiFactory()
            {
                @Override
                public DiscoveryApi createDiscoveryApi()
                {
                    return TestingDiscoveryApi.this;
                }

                @Override
                public DataApi createDataApi(BufferNodeInfo nodeInfo)
                {
                    throw new IllegalArgumentException("not implemented");
                }
            };
        }

        public void reset()
        {
            complete = false;
            bufferNodeInfos.clear();
        }

        public void setComplete(boolean complete)
        {
            this.complete = complete;
        }

        @Override
        public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
        {
            bufferNodeInfos.put(bufferNodeInfo.nodeId(), bufferNodeInfo);
        }

        public void deleteBufferNode(long bufferNodeId)
        {
            BufferNodeInfo removed = bufferNodeInfos.remove(bufferNodeId);
            assertThat(removed).describedAs("node %s not found", bufferNodeId).isNotNull();
        }

        @Override
        public BufferNodeInfoResponse getBufferNodes()
        {
            return new BufferNodeInfoResponse(
                    complete,
                    ImmutableSet.copyOf(bufferNodeInfos.values()));
        }
    }
}
