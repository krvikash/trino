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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.BufferNodeStats;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

class TestingBufferNodeDiscoveryManager
        implements BufferNodeDiscoveryManager
{
    public static final BufferNodeStats DEFAULT_NODE_STATS = new BufferNodeStats(
            DataSize.of(32, DataSize.Unit.GIGABYTE).toBytes(),
            DataSize.of(16, DataSize.Unit.GIGABYTE).toBytes(),
            10,
            1000,
            500,
            1000,
            1,
            2,
            3);

    private BufferNodesState bufferNodesState = new BufferNodeDiscoveryManager.BufferNodesState(0, ImmutableMap.of()); // empty state by default

    public void setBufferNodes(BufferNodesState state)
    {
        this.bufferNodesState = state;
    }

    public void setBufferNodes(Consumer<BufferNodesStateBuilder> builderConsumer)
    {
        BufferNodesStateBuilder builder = BufferNodesStateBuilder.empty();
        builderConsumer.accept(builder);
        bufferNodesState = builder.build();
    }

    public void updateBufferNodes(Consumer<BufferNodesStateBuilder> builderConsumer)
    {
        requireNonNull(bufferNodesState, "bufferNodeState is null");
        BufferNodesStateBuilder builder = BufferNodesStateBuilder.ofState(bufferNodesState);
        builderConsumer.accept(builder);
        bufferNodesState = builder.build();
    }

    @Override
    public BufferNodesState getBufferNodes()
    {
        return bufferNodesState;
    }

    @Override
    public ListenableFuture<Void> forceRefresh()
    {
        // noop
        return immediateVoidFuture();
    }

    public static class BufferNodesStateBuilder
    {
        private final Map<Long, BufferNodeInfo> bufferNodesMap;

        private BufferNodesStateBuilder(Map<Long, BufferNodeInfo> bufferNodesMap)
        {
            this.bufferNodesMap = new HashMap<>(bufferNodesMap);
        }

        public static BufferNodesStateBuilder empty()
        {
            return new BufferNodesStateBuilder(ImmutableMap.of());
        }

        public static BufferNodesStateBuilder ofState(BufferNodesState state)
        {
            return new BufferNodesStateBuilder(state.getAllBufferNodes());
        }

        @CanIgnoreReturnValue
        public BufferNodesStateBuilder putNode(long nodeId, BufferNodeState state)
        {
            return putNode(nodeId, state, DEFAULT_NODE_STATS);
        }

        @CanIgnoreReturnValue
        public BufferNodesStateBuilder putNode(long nodeId, BufferNodeState state, BufferNodeStats stats)
        {
            bufferNodesMap.put(nodeId, new BufferNodeInfo(nodeId, URI.create("http://node_" + nodeId), Optional.of(stats), state, Instant.now()));
            return this;
        }

        @CanIgnoreReturnValue
        BufferNodesStateBuilder removeNode(long nodeId)
        {
            bufferNodesMap.remove(nodeId);
            return this;
        }

        public BufferNodesState build()
        {
            return new BufferNodesState(System.nanoTime(), bufferNodesMap);
        }
    }
}
