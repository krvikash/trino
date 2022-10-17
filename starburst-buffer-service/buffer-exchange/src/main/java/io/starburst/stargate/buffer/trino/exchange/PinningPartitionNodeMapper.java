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
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.BufferNodeStats;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PinningPartitionNodeMapper
        implements PartitionNodeMapper
{
    private final BufferNodeDiscoveryManager discoveryManager;
    private final int outputPartitionCount;

    @GuardedBy("this")
    private Map<Integer, Long> currentMapping;

    public PinningPartitionNodeMapper(BufferNodeDiscoveryManager discoveryManager, int outputPartitionCount)
    {
        this.discoveryManager = discoveryManager;
        this.outputPartitionCount = outputPartitionCount;
    }

    @Override
    public synchronized Map<Integer, Long> getMapping(int taskPartitionId)
    {
        if (currentMapping == null) {
            currentMapping = computeMapping();
        }
        return currentMapping;
    }

    private Map<Integer, Long> computeMapping()
    {
        List<BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes().values().stream()
                .filter(node -> node.getState() == BufferNodeState.RUNNING)
                .filter(node -> node.getStats().isPresent())
                .collect(toImmutableList());

        if (bufferNodes.size() == 0) {
            // todo keep trying to get mapping for some time. To be figured out how to do that not blocking call to instantiateSink or refreshSinkInstanceHandle
            throw new RuntimeException("no RUNNING buffer nodes available");
        }

        long maxChunksCount = bufferNodes.stream().mapToLong(node -> node.getStats().orElseThrow().getOpenChunks() + node.getStats().orElseThrow().getClosedChunks()).max().orElseThrow();
        RandomSelector<BufferNodeInfo> selector = RandomSelector.weighted(
                bufferNodes,
                node -> {
                    BufferNodeStats stats = node.getStats().orElseThrow();
                    double memoryWeight = (double) stats.getFreeMemory() / stats.getTotalMemory();
                    int chunksCount = stats.getOpenChunks() + stats.getClosedChunks();
                    double chunksWeight;
                    if (maxChunksCount == 0) {
                        chunksWeight = 0.0;
                    }
                    else {
                        chunksWeight = 1.0 - (double) chunksCount / maxChunksCount;
                    }

                    if (memoryWeight < chunksWeight) {
                        // if we are constrained more with memory let's just use that
                        return memoryWeight;
                    }
                    // if we have plenty of memory lets take chunks count into account
                    return (memoryWeight + chunksWeight) / 2;
                });

        ImmutableMap.Builder<Integer, Long> mapping = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> mapping.put(partition, selector.next().getNodeId()));
        return mapping.buildOrThrow();
    }

    @Override
    public synchronized void refreshMapping()
    {
        checkState(currentMapping != null, "currentMapping should be already set");
        Map<Long, BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes();
        Map<Integer, Long> newMapping = computeMapping();
        ImmutableMap.Builder<Integer, Long> finalMapping = ImmutableMap.builder();

        for (Map.Entry<Integer, Long> entry : currentMapping.entrySet()) {
            Integer partition = entry.getKey();
            Long oldBufferNodeId = entry.getValue();
            BufferNodeInfo oldBufferNodeInfo = bufferNodes.get(oldBufferNodeId);
            if (oldBufferNodeInfo != null && oldBufferNodeInfo.getState() == BufferNodeState.RUNNING) {
                // keep old mapping entry
                finalMapping.put(partition, oldBufferNodeId);
            }
            else {
                // use new mapping
                finalMapping.put(partition, newMapping.get(partition));
            }
        }
        currentMapping = finalMapping.buildOrThrow();
    }
}
