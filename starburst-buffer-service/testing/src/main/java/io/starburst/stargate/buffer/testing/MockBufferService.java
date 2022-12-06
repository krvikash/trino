/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class MockBufferService
        implements DiscoveryApi
{
    public static final long PAGES_PER_CHUNK = 2;

    private final Map<Long, MockDataNode> dataNodes = new HashMap<>();
    private final MockDrainedStorage drainedStorage;

    public MockBufferService(int dataNodesCount)
    {
        this.drainedStorage = new MockDrainedStorage();
        for (long nodeId = 0; nodeId < dataNodesCount; ++nodeId) {
            dataNodes.put(nodeId, new MockDataNode(nodeId, drainedStorage));
        }
    }

    public DiscoveryApi getDiscoveryApi()
    {
        return this;
    }

    public DataApi getDataApi(long nodeId)
    {
        MockDataNode mockDataNode = dataNodes.get(nodeId);
        checkArgument(mockDataNode != null, "Node with id %d not found", nodeId);
        return mockDataNode;
    }

    @Override
    public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public BufferNodeInfoResponse getBufferNodes()
    {
        return new BufferNodeInfoResponse(
                true,
                dataNodes.values().stream()
                        .map(MockDataNode::getNodeInfo)
                        .filter(Optional::isPresent)
                        .map(Optional::orElseThrow)
                        .collect(toImmutableSet()));
    }

    public synchronized void markNodeDraining(long nodeId)
    {
        getNode(nodeId).markNodeDraining();
    }

    public synchronized void markNodeDrained(long nodeId)
    {
        getNode(nodeId).markNodeDrained();
    }

    public synchronized void markNodeGone(long nodeId)
    {
        getNode(nodeId).markNodeGone();
    }

    public synchronized long addNode()
    {
        long newNodeId = dataNodes.size();
        verify(!dataNodes.containsKey(newNodeId), "Data node with id %s already present", newNodeId);
        dataNodes.put(newNodeId, new MockDataNode(newNodeId, drainedStorage));
        return newNodeId;
    }

    public synchronized Set<Long> getAllNodeIds()
    {
        return ImmutableSet.copyOf(dataNodes.keySet());
    }

    public synchronized MockDataNodeStats getNodeStats(long nodeId)
    {
        return getNode(nodeId).getStats();
    }

    @GuardedBy("this")
    private MockDataNode getNode(long nodeId)
    {
        MockDataNode dataNode = dataNodes.get(nodeId);
        checkState(dataNode != null, "data node %d not found", nodeId);
        return dataNode;
    }
}
