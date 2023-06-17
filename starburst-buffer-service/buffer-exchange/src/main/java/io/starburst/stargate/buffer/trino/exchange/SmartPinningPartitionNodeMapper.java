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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.trino.exchange.BufferNodeDiscoveryManager.BufferNodesState;
import io.trino.spi.exchange.ExchangeId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/*
 Partition to buffer node mapper which tries to distribute partitions evenly among the cluster keeping a set of
 buffer nodes responsible for given partition in reasonable bounds. If we have less partitions we allow more buffer
 nodes to be responsible for each partition. If we have plenty partitions assign smaller set of buffer nodes for each.

 If some buffer nodes go away, of overall number of active buffer nodes changes, the changes
 to precomputed mapping are incremental. Most of the mapping remains intact.

 Algorithm is parametrized with following variables
  - minBaseNodesPerPartition - minimum number of base nodes assigned to a single partition
  - maxBaseNodesPerPartition - maximum number of base nodes assigned to a single partition
  - bonusNodesMultiplier - multiplier we use to derive number of total nodes (base + extra) based on number of base nodes
  - minTotalNodesPerPartition - minimum number of total nodes (base + extra) assigned to a single partition
  - maxTotalNodesPerPartition - maximum number of total nodes (base + extra) assigned to a single partition

 At runtime, we determine:
  - outputPartitionsCount - number of output partitions for given exchange
  - bufferNodesCount - total number of active buffer nodes

 Base of buffer nodes per partition is determined using following routine:
  baseNodesPerPartition = ceil(bufferNodesCount / outputPartitionsCount)
  baseNodesPerPartition = min(baseNodesPerPartition, maxBaseNodesPerPartition)
  baseNodesPerPartition = max(baseNodesPerPartition, minBaseNodesPerPartition)

Total (base + extra) number of buffer nodes per partition is determined using follwing routine:
 totalNodesPerPartition = baseNodesPerPartition + baseNodesPerPartition * bonusNodesMultiplier // (we add baseNodesPerPartition * bonusNodesMultiplier on to of what we already had)
 totalNodesPerPartition = max(minTotalNodesPerPartition, totalNodesPerPartition)
 totalNodesPerPartition = min(maxTotalNodesPerPartition, totalNodesPerPartition)

 Set of nodes responsible per partition is determined using uniform random.

 Unless task is required to preserve row order within partition it gets all mapping for its use.
 The task will start with only using base nodes; and scale up to extra nodes if needed.
 For tasks which preserve row order a single, random node is selected for each partition.
*/
public class SmartPinningPartitionNodeMapper
        implements PartitionNodeMapper
{
    private static final Logger log = Logger.get(SmartPinningPartitionNodeMapper.class);

    private final ExchangeId exchangeId;
    private final BufferNodeDiscoveryManager discoveryManager;
    private final ScheduledExecutorService executor;
    private final boolean preserveOrderWithinPartition;
    private final int outputPartitionCount;
    private final int minBaseNodesPerPartition;
    private final int maxBaseNodesPerPartition;
    private final double bonusNodesMultiplier;
    private final int minTotalNodesPerPartition;
    private final int maxTotalNodesPerPartition;

    private final Duration maxWaitActiveBufferNodes;

    // mapping state
    @GuardedBy("this")
    private Set<Long> previousActiveNodes = ImmutableSet.of();
    @GuardedBy("this")
    private long previousBufferNodeStateTimestamp;
    @GuardedBy("this")
    private final ListMultimap<Integer, Long> partitionToNode = ArrayListMultimap.create();
    @GuardedBy("this")
    private final Multimap<Long, Integer> nodeToPartition = HashMultimap.create();
    @GuardedBy("this")
    private Map<Integer, Integer> baseNodesCount;
    @GuardedBy("this")
    private final Map<Long, NodeUsage> nodeUsageById = new HashMap<>();
    @GuardedBy("this")
    private final SortedSet<NodeUsage> nodeUsages = new TreeSet<>();

    public SmartPinningPartitionNodeMapper(
            ExchangeId exchangeId,
            BufferNodeDiscoveryManager discoveryManager,
            ScheduledExecutorService executor,
            int outputPartitionCount,
            boolean preserveOrderWithinPartition,
            int minBaseNodesPerPartition,
            int maxBaseNodesPerPartition,
            double bonusNodesMultiplier,
            int minTotalNodesPerPartition,
            int maxTotalNodesPerPartition,
            Duration maxWaitActiveBufferNodes)
    {
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        checkArgument(minBaseNodesPerPartition >= 1, "minBaseNodesPerPartition must be greater or equal to 1");
        checkArgument(maxBaseNodesPerPartition >= 1, "maxBaseNodesPerPartition must be greater or equal to 1");
        checkArgument(maxBaseNodesPerPartition >= minBaseNodesPerPartition, "maxBaseNodesPerPartition must be greater or equal to minBaseNodesPerPartition");
        checkArgument(bonusNodesMultiplier >= 0, "bonusNodesMultiplier must bet greater than or equal to 0");
        checkArgument(minTotalNodesPerPartition >= minBaseNodesPerPartition, "minTotalNodesPerPartition must be greater than or equalt to minBaseNodesPerPartition");
        checkArgument(maxTotalNodesPerPartition >= maxBaseNodesPerPartition, "maxTotalNodesPerPartition must be greater than or equal to maxBaseNodesPerPartition");
        checkArgument(maxTotalNodesPerPartition >= minTotalNodesPerPartition, "maxTotalNodesPerPartition must be greater or equal to minTotalNodesPerPartition");
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.minBaseNodesPerPartition = minBaseNodesPerPartition;
        this.maxBaseNodesPerPartition = maxBaseNodesPerPartition;
        this.bonusNodesMultiplier = bonusNodesMultiplier;
        this.minTotalNodesPerPartition = minTotalNodesPerPartition;
        this.maxTotalNodesPerPartition = maxTotalNodesPerPartition;
        this.maxWaitActiveBufferNodes = requireNonNull(maxWaitActiveBufferNodes, "maxWaitActiveBufferNodes is null");
    }

    @Override
    public synchronized ListenableFuture<PartitionNodeMapping> getMapping(int taskPartitionId)
    {
        ListenableFuture<BufferNodesState> bufferNodesStateFuture = getBufferNodeStateWithActiveNodes();

        return Futures.transform(bufferNodesStateFuture, bufferNodesState -> {
            synchronized (SmartPinningPartitionNodeMapper.this) {
                updateBaseMapping(bufferNodesState);

                if (preserveOrderWithinPartition) {
                    // pick one buffer node per partition
                    ImmutableListMultimap.Builder<Integer, Long> mapping = ImmutableListMultimap.builder();
                    IntStream.range(0, outputPartitionCount).forEach(partition -> {
                        List<BufferNodeInfo> candidateNodes = partitionToNode.get(partition).stream()
                                .map(nodeId -> bufferNodesState.getActiveBufferNodes().get(nodeId))
                                .collect(toImmutableList());

                        RandomSelector<BufferNodeInfo> bufferNodeInfoRandomSelector = buildNodeSelector(candidateNodes);
                        mapping.put(partition, bufferNodeInfoRandomSelector.next().nodeId());
                    });
                    // it is fine to use baseNodesCount here with values greater than 1 even though there is just a single node mapped to each partition
                    return new PartitionNodeMapping(mapping.build(), baseNodesCount);
                }
                // return whole mapping
                return new PartitionNodeMapping(partitionToNode, baseNodesCount);
            }
        },
        directExecutor());
    }

    private ListenableFuture<BufferNodesState> getBufferNodeStateWithActiveNodes()
    {
        BufferNodesState bufferNodesState = discoveryManager.getBufferNodes();
        if (!bufferNodesState.getActiveBufferNodesSet().isEmpty()) {
            return Futures.immediateFuture(bufferNodesState);
        }

        return waitForActiveBufferNodes();
    }

    private SettableFuture<BufferNodesState> waitForActiveBufferNodes()
    {
        SettableFuture<BufferNodesState> resultFuture = SettableFuture.create();
        long waitStart = System.currentTimeMillis();
        long waitSleep = maxWaitActiveBufferNodes.toMillis() / 20;

        ScheduledFuture<?> schedulingFuture = executor.scheduleWithFixedDelay(() -> {
            try {
                long now = System.currentTimeMillis();
                BufferNodesState bufferNodesState = discoveryManager.getBufferNodes();
                if (!bufferNodesState.getActiveBufferNodesSet().isEmpty()) {
                    resultFuture.set(bufferNodesState);
                    return;
                }
                if (now - waitStart > maxWaitActiveBufferNodes.toMillis()) {
                    resultFuture.setException(new RuntimeException("no ACTIVE buffer nodes available"));
                }
            }
            catch (Exception e) {
                resultFuture.setException(new RuntimeException("unexpected exception waiting for ACTIVE buffer nodes", e));
            }
        }, waitSleep, waitSleep, MILLISECONDS);

        resultFuture.addListener(() -> schedulingFuture.cancel(true), directExecutor()); // cancel subsequent executions when we are done

        return resultFuture;
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

        if (bufferNodesState.getActiveBufferNodes().equals(previousActiveNodes)) {
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

        int targetBaseNodesPerPartition = targetBaseNodesPerPartitionCount(activeNodes.size());
        int targetTotalNodesPerPartition = targetTotalNodesPerPartitionCount(targetBaseNodesPerPartition, activeNodes.size());

        // remove nodes from mapping for partitions above target
        for (int partition = 0; partition < outputPartitionCount; partition++) {
            int extraCount = partitionToNode.get(partition).size() - targetTotalNodesPerPartition;
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
            int missingCount = targetTotalNodesPerPartition - partitionToNode.get(partition).size();
            if (missingCount <= 0) {
                continue;
            }

            for (int i = 0; i < missingCount; ++i) {
                for (NodeUsage nodeUsage : nodeUsages) {
                    long nodeId = nodeUsage.getNodeId();
                    if (!partitionToNode.containsEntry(partition, nodeId)) {
                        partitionToNodePutRandomPosition(partition, nodeId);
                        nodeToPartition.put(nodeId, partition);
                        incrementNodeUsage(nodeId);
                        break;
                    }
                }
            }
        }

        // TODO: now we are rebalancing all nodes alltogether; this does not guarantee balance if you just look at
        //       base mapping subset. It should not be a big deal in practice though given fact that we either have just
        //       one partition (where it does not matter at all) or 1000 partitions where potential inequalities between partitions
        //       should be covered by the fact that many partitions use same buffer nodes anyway

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
                    partitionToNodePutRandomPosition(partition, leastUsed.getNodeId());
                    nodeToPartition.put(leastUsed.getNodeId(), partition);
                    decrementNodeUsage(mostUsed.getNodeId());
                    incrementNodeUsage(leastUsed.getNodeId());
                    swapped = true;
                    break; // we need to break eventually. Otherwise, leastUsed usage count would be equal to mostUsed usage count and we checked it before
                }
            }
            verify(swapped, "invalid internal state; expected to swap least<->most used");
        }

        // update baseNodesCount mapping
        ImmutableMap.Builder<Integer, Integer> baseNodesCountBuilder = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> baseNodesCountBuilder.put(partition, targetBaseNodesPerPartition));
        this.baseNodesCount = baseNodesCountBuilder.buildOrThrow();

        log.debug("compute base node mapping for %s: %s", exchangeId, partitionToNode);
        previousActiveNodes = ImmutableSet.copyOf(activeNodes.keySet());
    }

    @GuardedBy("this")
    private void partitionToNodePutRandomPosition(int partition, long nodeId)
    {
        List<Long> nodes = partitionToNode.get(partition);
        int targetNodeIndex = ThreadLocalRandom.current().nextInt(nodes.size() + 1);
        // insertion is O(N) here but the lists should not be long (we are not assigning more than 64 nodes for partition right now).
        // alternative would be to add always at the end and reshuffle but we do not want to do that as we want the mapping
        // to stay as close to previous one when we are adding new nodes/rebalancing
        nodes.add(targetNodeIndex, nodeId);
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

    private int targetBaseNodesPerPartitionCount(int activeBufferNodesCount)
    {
        int baseNodesPerPartition = (int) Math.ceil(1.0 * activeBufferNodesCount / outputPartitionCount);
        baseNodesPerPartition = max(minBaseNodesPerPartition, baseNodesPerPartition);
        baseNodesPerPartition = min(maxBaseNodesPerPartition, baseNodesPerPartition);

        // ensure we are not exceeding all active nodes in the cluster
        baseNodesPerPartition = min(baseNodesPerPartition, activeBufferNodesCount);
        return baseNodesPerPartition;
    }

    private int targetTotalNodesPerPartitionCount(int baseNodesPerPartition, int activeBufferNodesCount)
    {
        int totalNodesPerPartition = (int) (baseNodesPerPartition + baseNodesPerPartition * bonusNodesMultiplier);

        totalNodesPerPartition = max(minTotalNodesPerPartition, totalNodesPerPartition);
        totalNodesPerPartition = min(maxTotalNodesPerPartition, totalNodesPerPartition);

        // ensure we are not exceeding all active nodes in the cluster
        totalNodesPerPartition = min(totalNodesPerPartition, activeBufferNodesCount);
        return totalNodesPerPartition;
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
