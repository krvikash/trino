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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.max;

public class PinningPartitionNodeMapper
        implements PartitionNodeMapper
{
    private final BufferNodeDiscoveryManager discoveryManager;
    private final int outputPartitionCount;
    private final ImmutableMap<Integer, Integer> baseNodesCount;

    @GuardedBy("this")
    private PartitionNodeMapping currentMapping;

    public PinningPartitionNodeMapper(BufferNodeDiscoveryManager discoveryManager, int outputPartitionCount)
    {
        this.discoveryManager = discoveryManager;
        this.outputPartitionCount = outputPartitionCount;
        ImmutableMap.Builder<Integer, Integer> baseNodesCount = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> baseNodesCount.put(partition, 1));
        this.baseNodesCount = baseNodesCount.buildOrThrow();
    }

    @Override
    public synchronized ListenableFuture<PartitionNodeMapping> getMapping(int taskPartitionId)
    {
        if (currentMapping == null) {
            currentMapping = computeMapping();
        }
        return immediateFuture(currentMapping);
    }

    private PartitionNodeMapping computeMapping()
    {
        Set<BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes().getActiveBufferNodesSet();

        if (bufferNodes.size() == 0) {
            // todo keep trying to get mapping for some time. To be figured out how to do that not blocking call to instantiateSink or refreshSinkInstanceHandle
            throw new RuntimeException("no ACTIVE buffer nodes available");
        }

        long maxChunksCount = bufferNodes.stream().mapToLong(node ->
                node.stats().orElseThrow().openChunks()
                        + node.stats().orElseThrow().closedChunks()
                        + node.stats().orElseThrow().spooledChunks()).max().orElseThrow();
        RandomSelector<BufferNodeInfo> selector = RandomSelector.weighted(
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

        ImmutableListMultimap.Builder<Integer, Long> mapping = ImmutableListMultimap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> mapping.put(partition, selector.next().nodeId()));
        return new PartitionNodeMapping(mapping.build(), baseNodesCount);
    }

    @Override
    public synchronized void refreshMapping()
    {
        checkState(currentMapping != null, "currentMapping should be already set");
        Map<Long, BufferNodeInfo> activeBufferNodes = discoveryManager.getBufferNodes().getActiveBufferNodes();
        PartitionNodeMapping newMapping = computeMapping();
        ImmutableListMultimap.Builder<Integer, Long> finalMapping = ImmutableListMultimap.builder();

        for (Map.Entry<Integer, Collection<Long>> entry : currentMapping.getMapping().asMap().entrySet()) {
            Integer partition = entry.getKey();
            Long oldBufferNodeId = getOnlyElement(entry.getValue());
            if (activeBufferNodes.containsKey(oldBufferNodeId)) {
                // keep old mapping entry
                finalMapping.put(partition, oldBufferNodeId);
            }
            else {
                // use new mapping

                finalMapping.put(partition, getOnlyElement(newMapping.getMapping().get(partition)));
            }
        }
        currentMapping = new PartitionNodeMapping(finalMapping.build(), baseNodesCount);
    }
}
