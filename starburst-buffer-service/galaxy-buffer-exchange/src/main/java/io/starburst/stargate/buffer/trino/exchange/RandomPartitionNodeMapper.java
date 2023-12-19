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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeStats;

import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class RandomPartitionNodeMapper
        implements PartitionNodeMapper
{
    // todo: maybe constrain the mapping to only use as subset of nodes in buffering service cluster
    //       based on the size of the Trino cluster we are operating in. It does not necessarily makes sense
    //       to distribute the work over 20 buffer nodes if the Trino cluster itself only has 4 nodes.
    private final BufferNodeDiscoveryManager discoveryManager;
    private final int outputPartitionCount;
    private final ImmutableMap<Integer, Integer> baseNodesCount;
    @GuardedBy("this")
    private RandomSelector<BufferNodeInfo> currentNodeSelector;
    @GuardedBy("this")
    private long currentNodeSelectorTimestamp;

    public RandomPartitionNodeMapper(BufferNodeDiscoveryManager discoveryManager, int outputPartitionCount)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.outputPartitionCount = outputPartitionCount;
        ImmutableMap.Builder<Integer, Integer> baseNodesCount = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> baseNodesCount.put(partition, 1));
        this.baseNodesCount = baseNodesCount.buildOrThrow();
    }

    @Override
    public synchronized ListenableFuture<PartitionNodeMapping> getMapping(int taskPartitionId)
    {
        RandomSelector<BufferNodeInfo> selector = getBufferNodeSelector();

        ImmutableListMultimap.Builder<Integer, Long> mapping = ImmutableListMultimap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> mapping.put(partition, selector.next().nodeId()));
        return immediateFuture(new PartitionNodeMapping(mapping.build(), baseNodesCount));
    }

    @GuardedBy("this")
    private RandomSelector<BufferNodeInfo> getBufferNodeSelector()
    {
        BufferNodeDiscoveryManager.BufferNodesState bufferNodesState = discoveryManager.getBufferNodes();
        if (bufferNodesState.getTimestamp() > currentNodeSelectorTimestamp) {
            currentNodeSelector = buildBufferNodeSelector(bufferNodesState);
            currentNodeSelectorTimestamp = bufferNodesState.getTimestamp();
        }

        return currentNodeSelector;
    }

    private RandomSelector<BufferNodeInfo> buildBufferNodeSelector(BufferNodeDiscoveryManager.BufferNodesState bufferNodesState)
    {
        Set<BufferNodeInfo> bufferNodes = bufferNodesState.getActiveBufferNodesSet();

        if (bufferNodes.size() == 0) {
            throw new RuntimeException("no ACTIVE buffer nodes available");
        }

        long maxChunksCount = bufferNodes.stream().mapToLong(node ->
                node.stats().orElseThrow().openChunks()
                        + node.stats().orElseThrow().closedChunks()
                        + node.stats().orElseThrow().spooledChunks()).max().orElseThrow();
        return RandomSelector.weighted(
                bufferNodes,
                node -> {
                    BufferNodeStats stats = node.stats().orElseThrow();
                    double memoryWeight = (double) stats.freeMemory() / stats.totalMemory();
                    int chunksCount = stats.openChunks() + stats.closedChunks() + stats.spooledChunks();
                    double chunksWeight;
                    if (maxChunksCount == 0) {
                        chunksWeight = 0.0;
                    }
                    else {
                        chunksWeight = max(0.0, 1.0 - (double) chunksCount / maxChunksCount);
                    }

                    if (memoryWeight < chunksWeight) {
                        // if we are constrained more with memory let's just use that
                        return memoryWeight;
                    }
                    // if we have plenty of memory lets take chunks count into account
                    return (memoryWeight + chunksWeight) / 2;
                });
    }

    @Override
    public void refreshMapping()
    {
        // nothing to do here
    }
}
