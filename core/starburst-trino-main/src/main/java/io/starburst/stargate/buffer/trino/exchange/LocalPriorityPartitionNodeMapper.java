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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.trino.exchange.BufferNodeDiscoveryManager.BufferNodesState;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LocalPriorityPartitionNodeMapper
        implements PartitionNodeMapper
{
    private static final Logger log = Logger.get(LocalPriorityPartitionNodeMapper.class);

    private final BufferNodeDiscoveryManager discoveryManager;
    private final ScheduledExecutorService executor;
    private final int outputPartitionCount;
    private final int totalNodesPerPartition;
    private final Duration maxWaitActiveBufferNodes;
    private final Map<Integer, Integer> baseNodesCount;

    public LocalPriorityPartitionNodeMapper(
            BufferNodeDiscoveryManager discoveryManager,
            ScheduledExecutorService executor,
            int outputPartitionCount,
            int totalNodesPerPartition,
            Duration maxWaitActiveBufferNodes)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        checkArgument(outputPartitionCount > 0, "outputPartitionCount must be > 0");
        this.outputPartitionCount = outputPartitionCount;
        checkArgument(totalNodesPerPartition > 0, "totalNodesPerPartition must be > 0");
        this.totalNodesPerPartition = totalNodesPerPartition;
        this.maxWaitActiveBufferNodes = requireNonNull(maxWaitActiveBufferNodes, "maxWaitActiveBufferNodes is null");
        ImmutableMap.Builder<Integer, Integer> baseNodesCount = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> baseNodesCount.put(partition, 1));
        this.baseNodesCount = baseNodesCount.buildOrThrow();
    }

    @Override
    public ListenableFuture<PartitionNodeMapping> getMapping(int taskPartitionId, Optional<Node> taskNode)
    {
        return Futures.transform(
                getBufferNodeStateWithActiveNodes(),
                nodes -> {
                    Optional<BufferNodeInfo> localBufferNode = findLocalBufferNode(nodes.getActiveBufferNodes().values(), taskNode);
                    if (localBufferNode.isEmpty()) {
                        log.warn("No local buffer node found for task partition %s on node %s", taskPartitionId, taskNode);
                    }

                    List<BufferNodeInfo> otherActiveBufferNodes = new ArrayList<>();
                    for (BufferNodeInfo bufferNode : nodes.getActiveBufferNodes().values()) {
                        if (localBufferNode.isPresent() && localBufferNode.get().equals(bufferNode)) {
                            continue;
                        }
                        otherActiveBufferNodes.add(bufferNode);
                    }
                    shuffle(otherActiveBufferNodes, ThreadLocalRandom.current());

                    int extraRandomNodes = localBufferNode.isPresent() ? totalNodesPerPartition - 1 : totalNodesPerPartition;
                    ImmutableListMultimap.Builder<Integer, Long> mapping = ImmutableListMultimap.builder();
                    for (int partition = 0; partition < outputPartitionCount; partition++) {
                        if (localBufferNode.isPresent()) {
                            mapping.put(partition, localBufferNode.get().nodeId());
                        }

                        if (!otherActiveBufferNodes.isEmpty()) {
                            int randomStart = ThreadLocalRandom.current().nextInt(otherActiveBufferNodes.size());
                            for (int i = 0; i < extraRandomNodes; i++) {
                                int index = (randomStart + i) % otherActiveBufferNodes.size();
                                if (i != 0 && index == randomStart) {
                                    // we made a full circle, no more unique nodes available
                                    break;
                                }
                                BufferNodeInfo bufferNodeInfo = otherActiveBufferNodes.get(index);
                                mapping.put(partition, bufferNodeInfo.nodeId());
                            }
                        }
                    }
                    return new PartitionNodeMapping(mapping.build(), baseNodesCount);
                },
                directExecutor());
    }

    private Optional<BufferNodeInfo> findLocalBufferNode(Collection<BufferNodeInfo> activeBufferNodes, Optional<Node> taskNodeOptional)
    {
        return taskNodeOptional.flatMap(taskNode -> {
            for (BufferNodeInfo bufferNodeInfo : activeBufferNodes) {
                String bufferNodeHost = bufferNodeInfo.uri().getHost();
                int bufferNodePort = bufferNodeInfo.uri().getPort();

                if (taskNode.getHostAndPort().equals(HostAddress.fromParts(bufferNodeHost, bufferNodePort))) {
                    return Optional.of(bufferNodeInfo);
                }
            }
            return Optional.empty();
        });
    }

    @Override
    public void refreshMapping()
    {
        // nothing to do here
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
        long waitStart = System.nanoTime() / 1_000_000;
        long waitSleep = maxWaitActiveBufferNodes.toMillis() / 20;

        ScheduledFuture<?> schedulingFuture = executor.scheduleWithFixedDelay(() -> {
            try {
                long now = System.nanoTime() / 1_000_000;
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
}
