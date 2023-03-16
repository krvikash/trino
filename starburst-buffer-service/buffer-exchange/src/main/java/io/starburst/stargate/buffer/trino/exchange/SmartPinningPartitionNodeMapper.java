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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.trino.exchange.BufferNodeDiscoveryManager.BufferNodesState;
import io.trino.spi.exchange.ExchangeId;

import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/*
 Partition to buffer node mapper which tries to distribute partitions evenly among the cluster keeping a set of
 buffer nodes responsible for given partition in reasonable bounds. If we have less partitions we allow more buffer
 nodes to be responsible for each partition. If we have plenty partitions assign smaller set of buffer nodes for each.

 If some buffer nodes go away, of overall number of active buffer nodes changes, the changes
 to precomputed mapping are incremental. Most of the mapping remains intact.

 Algorithm is parametrized with following variables
 - maxNodesPerPartition - maximum number of nodes assigned to a single partition
 - minNodesPerPartition - minimum number of nodes assigned to a single partition

 At runtime, we determine:
  - outputPartitionsCount - number of output partitions for given exchange
  - bufferNodesCount - total number of active buffer nodes

 Number of buffer nodes per partition is determined using following routine
  nodesPerPartition = ceil(bufferNodesCount / outputPartitionsCount)
  nodesPerPartition = min(nodesPerPartition, maxNodesPerPartition)
  nodesPerPartition = max(nodesPerPartition, minNodesPerPartition)

 Set of nodes responsible per partition is determined using uniform random.

 Then individual buffer nodes to be used by given tasks are selected from base mapping
 using weighted random based on utilization of individual buffer nodes.
*/
public class SmartPinningPartitionNodeMapper
        implements PartitionNodeMapper
{
    private static final Logger log = Logger.get(SmartPinningPartitionNodeMapper.class);

    private final ExchangeId exchangeId;
    private final BufferNodeDiscoveryManager discoveryManager;
    private final int outputPartitionCount;
    private final int minNodesPerPartition;
    private final int maxNodesPerPartition;

    // mapping state
    @GuardedBy("this")
    private Set<Long> previousActiveNodes = ImmutableSet.of();
    @GuardedBy("this")
    private long previousBufferNodeStateTimestamp;
    @GuardedBy("this")
    private final Multimap<Integer, Long> partitionToNode = HashMultimap.create();
    @GuardedBy("this")
    private final Multimap<Long, Integer> nodeToPartition = HashMultimap.create();
    @GuardedBy("this")
    private final Map<Long, NodeUsage> nodeUsageById = new HashMap<>();
    @GuardedBy("this")
    private final SortedSet<NodeUsage> nodeUsages = new TreeSet<>();

    public SmartPinningPartitionNodeMapper(
            ExchangeId exchangeId,
            BufferNodeDiscoveryManager discoveryManager,
            int outputPartitionCount,
            int minNodesPerPartition,
            int maxNodesPerPartition)
    {
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        checkArgument(minNodesPerPartition >= 1, "minNodesPerPartition must be greater or equal to 1");
        checkArgument(maxNodesPerPartition >= 1, "maxNodesPerPartition must be greater or equal to 1");
        checkArgument(maxNodesPerPartition >= minNodesPerPartition, "maxNodesPerPartition must be greater or equal to minNodesPerPartition");
        this.outputPartitionCount = outputPartitionCount;
        this.minNodesPerPartition = minNodesPerPartition;
        this.maxNodesPerPartition = maxNodesPerPartition;
    }

    @Override
    public synchronized ListenableFuture<Map<Integer, Long>> getMapping(int taskPartitionId)
    {
        BufferNodesState bufferNodesState = discoveryManager.getBufferNodes();

        Set<BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes().getActiveBufferNodesSet();
        int bufferNodesCount = bufferNodes.size();
        if (bufferNodesCount == 0) {
            // todo keep trying to get mapping for some time. To be figured out how to do that not blocking call to instantiateSink or refreshSinkInstanceHandle
            throw new RuntimeException("no ACTIVE buffer nodes available");
        }

        updateBaseMapping(bufferNodesState);

        ImmutableMap.Builder<Integer, Long> mapping = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> {
            List<BufferNodeInfo> candidateNodes = partitionToNode.get(partition).stream()
                    .map(nodeId -> bufferNodesState.getActiveBufferNodes().get(nodeId))
                    .collect(toImmutableList());

            RandomSelector<BufferNodeInfo> bufferNodeInfoRandomSelector = buildNodeSelector(candidateNodes);
            mapping.put(partition, bufferNodeInfoRandomSelector.next().nodeId());
        });
        return Futures.immediateFuture(mapping.buildOrThrow());
    }

    private RandomSelector<BufferNodeInfo> buildNodeSelector(List<BufferNodeInfo> bufferNodes)
    {
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
                        chunksWeight = 1.0 - (double) chunksCount / maxChunksCount;
                    }

                    if (memoryWeight < chunksWeight) {
                        // if we are constrained more with memory let's just use that
                        return memoryWeight;
                    }
                    // if we have plenty of memory lets take chunks count into account
                    return (memoryWeight + chunksWeight) / 2;
                });
    }

    @GuardedBy("this")
    private void updateBaseMapping(BufferNodesState bufferNodesState)
    {
        checkArgument(!bufferNodesState.getActiveBufferNodes().isEmpty(), "empty active nodes");

        if (bufferNodesState.getTimestamp() == previousBufferNodeStateTimestamp) {
            // nothing changed
            return;
        }
        previousBufferNodeStateTimestamp = bufferNodesState.getTimestamp();

        if (bufferNodesState.getAllBufferNodes().equals(previousActiveNodes)) {
            // nothing changed
            return;
        }

        Map<Long, BufferNodeInfo> activeNodes = bufferNodesState.getActiveBufferNodes();

        // drop removed nodes
        Set<Long> removedNodes = previousActiveNodes.stream()
                .filter(nodeId -> !activeNodes.containsKey(nodeId))
                .collect(toImmutableSet());
        for (Long nodeId : removedNodes) {
            NodeUsage nodeUsage = nodeUsageById.remove(nodeId);
            verify(nodeUsage != null, "no nodeUsageById entry found for %s", nodeId);
            verify(nodeUsages.remove(nodeUsage), "no nodeUsages entry found for %s", nodeUsage);
            nodeToPartition.removeAll(nodeId);
        }
        partitionToNode.entries().removeIf(entry -> removedNodes.contains(entry.getValue()));

        // add new nodes
        Set<Long> newNodes = activeNodes.keySet().stream()
                .filter(nodeId -> !previousActiveNodes.contains(nodeId))
                .collect(toImmutableSet());
        for (Long nodeId : newNodes) {
            NodeUsage nodeUsage = new NodeUsage(nodeId, 0);
            nodeUsages.add(nodeUsage);
            nodeUsageById.put(nodeId, nodeUsage);
        }

        int targetNodesPerPartition = targetNodesPerPartitionCount(activeNodes.size());
        // remove nodes from mapping for partitions above target
        for (int partition = 0; partition < outputPartitionCount; partition++) {
            int extraCount = partitionToNode.get(partition).size() - targetNodesPerPartition;
            if (extraCount <= 0) {
                continue;
            }

            Iterator<Long> iterator = partitionToNode.get(partition).iterator();
            for (int i = 0; i < extraCount; ++i) {
                Long nodeId = iterator.next();
                decrementNodeUsage(nodeId);
                nodeToPartition.remove(nodeId, partition);
                iterator.remove();
            }
        }

        // add nodes to mapping for partitions below target
        for (int partition = 0; partition < outputPartitionCount; partition++) {
            int missingCount = targetNodesPerPartition - partitionToNode.get(partition).size();
            if (missingCount <= 0) {
                continue;
            }

            for (int i = 0; i < missingCount; ++i) {
                for (NodeUsage nodeUsage : nodeUsages) {
                    long nodeId = nodeUsage.getNodeId();
                    if (!partitionToNode.containsEntry(partition, nodeId)) {
                        partitionToNode.put(partition, nodeId);
                        nodeToPartition.put(nodeId, partition);
                        incrementNodeUsage(nodeId);
                        break;
                    }
                }
            }
        }

        // rebalance over nodes if needed
        while (true) {
            NodeUsage leastUsed = nodeUsages.first();
            NodeUsage mostUsed = nodeUsages.last();
            if (mostUsed.getUsedByCount() - leastUsed.getUsedByCount() <= 1) {
                // we are good
                break;
            }
            Collection<Integer> mostUsedPartitions = nodeToPartition.get(mostUsed.getNodeId());
            boolean swapped = false;
            for (Integer partition : mostUsedPartitions) {
                if (!partitionToNode.containsEntry(partition, leastUsed.getNodeId())) {
                    partitionToNode.remove(partition, mostUsed.getNodeId());
                    nodeToPartition.remove(mostUsed.getNodeId(), partition);
                    partitionToNode.put(partition, leastUsed.getNodeId());
                    nodeToPartition.put(leastUsed.getNodeId(), partition);
                    decrementNodeUsage(mostUsed.getNodeId());
                    incrementNodeUsage(leastUsed.getNodeId());
                    swapped = true;
                    break; // we need to break eventually. Otherwise, leastUsed usage count would be equal to mostUsed usage count and we checked it before
                }
            }
            verify(swapped, "invalid internal state; expected to swap least<->most used");
        }

        log.debug("compute base node mapping for %s: %s", exchangeId, partitionToNode);
        previousActiveNodes = ImmutableSet.copyOf(activeNodes.keySet());
    }

    public Collection<Long> getPartitionBaseMapping(int partition)
    {
        return partitionToNode.get(partition);
    }

    @GuardedBy("this")
    private void incrementNodeUsage(long nodeId)
    {
        NodeUsage nodeUsage = nodeUsageById.get(nodeId);
        verify(nodeUsage != null, "no nodeUsage entry for %s", nodeId);
        nodeUsages.remove(nodeUsage);
        NodeUsage newNodeUsage = new NodeUsage(nodeId, nodeUsage.getUsedByCount() + 1);
        nodeUsages.add(newNodeUsage);
        nodeUsageById.put(nodeId, newNodeUsage);
    }

    @GuardedBy("this")
    private void decrementNodeUsage(long nodeId)
    {
        NodeUsage nodeUsage = nodeUsageById.get(nodeId);
        verify(nodeUsage != null, "no nodeUsage entry for %s", nodeId);
        verify(nodeUsage.getUsedByCount() >= 1, "expected usedByCount >= 1; %s", nodeUsage);
        nodeUsages.remove(nodeUsage);
        NodeUsage newNodeUsage = new NodeUsage(nodeId, nodeUsage.getUsedByCount() - 1);
        nodeUsages.add(newNodeUsage);
        nodeUsageById.put(nodeId, newNodeUsage);
    }

    private int targetNodesPerPartitionCount(int activeBufferNodesCount)
    {
        int nodesPerPartition = (int) Math.ceil(1.0 * activeBufferNodesCount / outputPartitionCount);
        nodesPerPartition = max(minNodesPerPartition, nodesPerPartition);
        nodesPerPartition = min(maxNodesPerPartition, nodesPerPartition);

        // ensure we are not exceeding all active nodes in the cluster
        nodesPerPartition = min(nodesPerPartition, activeBufferNodesCount);
        return nodesPerPartition;
    }

    @Override
    public void refreshMapping()
    {
        // noop - we keep base mapping up to data always
    }

    private static class NodeUsage
            implements Comparable<NodeUsage>
    {
        private final long nodeId;
        private final int usedByCount;
        private final int noise;

        public NodeUsage(long nodeId, int usedByCount)
        {
            this.nodeId = nodeId;
            this.usedByCount = usedByCount;
            this.noise = ThreadLocalRandom.current().nextInt();
        }

        public long getNodeId()
        {
            return nodeId;
        }

        public int getUsedByCount()
        {
            return usedByCount;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NodeUsage nodeUsage = (NodeUsage) o;
            return nodeId == nodeUsage.nodeId && usedByCount == nodeUsage.usedByCount && noise == nodeUsage.noise;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeId, usedByCount, noise);
        }

        @Override
        public int compareTo(NodeUsage other)
        {
            int result = Integer.compare(usedByCount, other.usedByCount);
            if (result == 0) {
                result = Integer.compare(noise, other.noise);
            }
            if (result == 0) {
                result = Long.compare(nodeId, other.nodeId);
            }
            return result;
        }
    }
}
