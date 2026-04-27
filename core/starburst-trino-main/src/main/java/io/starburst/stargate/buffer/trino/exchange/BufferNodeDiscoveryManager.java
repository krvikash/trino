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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public interface BufferNodeDiscoveryManager
{
    BufferNodesState getBufferNodes();

    ListenableFuture<Void> forceRefresh();

    class BufferNodesState
    {
        private final long timestamp;
        private final Map<Long, BufferNodeInfo> allBufferNodes;
        private final Map<Long, BufferNodeInfo> activeBufferNodes;
        private final Set<BufferNodeInfo> activeBufferNodesSet;

        public BufferNodesState(
                long timestamp,
                Map<Long, BufferNodeInfo> allBufferNodes)
        {
            this.timestamp = timestamp;
            this.allBufferNodes = ImmutableMap.copyOf(allBufferNodes);
            this.activeBufferNodes = allBufferNodes.entrySet().stream()
                    .filter(entry -> entry.getValue().state() == BufferNodeState.ACTIVE)
                    .peek(entry -> checkArgument(entry.getValue().stats().isPresent(), "stats not set for ACTIVE node %s", entry.getValue()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            this.activeBufferNodesSet = ImmutableSet.copyOf(activeBufferNodes.values());
        }

        public long getTimestamp()
        {
            return timestamp;
        }

        public Map<Long, BufferNodeInfo> getAllBufferNodes()
        {
            return allBufferNodes;
        }

        public Map<Long, BufferNodeInfo> getActiveBufferNodes()
        {
            return activeBufferNodes;
        }

        public Set<BufferNodeInfo> getActiveBufferNodesSet()
        {
            return activeBufferNodesSet;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("timestamp", timestamp)
                    .add("allBufferNodes", allBufferNodes)
                    .add("activeBufferNodes", activeBufferNodes)
                    .toString();
        }
    }
}
