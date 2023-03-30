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
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.math.Stats;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.spi.exchange.ExchangeId;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Ordering.natural;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.Duration.succinctNanos;
import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.DRAINING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSmartPinningPartitionNodeMapper
{
    public static final ExchangeId EXCHANGE_ID = new ExchangeId("some-exchange");
    private static final Duration NO_WAIT = succinctNanos(0);

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(4);

    @AfterAll
    public void teardown()
    {
        executor.shutdownNow();
    }

    @Test
    void testBasicMapping()
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 10).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 1, 2, 4, NO_WAIT),
                1,
                4,
                4);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 2, 2, 4, NO_WAIT),
                2,
                4,
                8);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 3, 2, 4, NO_WAIT),
                3,
                4,
                10);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 5, 2, 4, NO_WAIT),
                5,
                2,
                10);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 10, 2, 4, NO_WAIT),
                10,
                2,
                10);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 20, 2, 4, NO_WAIT),
                20,
                2,
                10);

        // large cluster
        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 1000).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 1, 4, 32, NO_WAIT),
                1,
                32,
                32);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 100, 4, 32, NO_WAIT),
                100,
                10,
                1000);

        assertEvenNodesDistribution(
                new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 1000, 4, 32, NO_WAIT),
                1000,
                4,
                1000);
    }

    @Test
    void testChangeBufferNodeState()
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        SmartPinningPartitionNodeMapper mapper;

        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 100).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));
        mapper = new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 8, 4, 32, NO_WAIT);
        assertEvenNodesDistribution(mapper, 8, 13, 100);
        SetMultimap<Integer, Long> distributionAllActive = getFullDistribution(mapper);

        discoveryManager.updateBufferNodes(builder -> LongStream.range(0, 25).forEach(nodeId -> builder.putNode(nodeId, DRAINING)));
        discoveryManager.updateBufferNodes(builder -> LongStream.range(25, 50).forEach(builder::removeNode));
        assertEvenNodesDistribution(mapper, 8, 7, 50);
        SetMultimap<Integer, Long> distributionHalfDrained = getFullDistribution(mapper);

        // check if we reuse as much as possible of old assignment
        distributionHalfDrained.asMap().forEach((partition, nodes) -> {
            Set<Long> undrainedAssignedNodes = distributionAllActive.get(partition).stream()
                    .filter(nodeId -> nodeId >= 50)
                    .collect(toImmutableSet());
            assertThat(nodes).as("nodes for " + partition).matches(nodesCollection -> {
                Set<Long> nodesSet = ImmutableSet.copyOf(nodesCollection);
                return nodesSet.containsAll(undrainedAssignedNodes) || Sets.intersection(nodesSet, undrainedAssignedNodes).size() == 7;
            }, "nodes=%s, undrainedAssignedNodes=%s".formatted(natural().sortedCopy(nodes), natural().sortedCopy(undrainedAssignedNodes)));
        });

        // add some new nodes
        discoveryManager.updateBufferNodes(builder -> LongStream.range(100, 200).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));
        assertEvenNodesDistribution(mapper, 8, 19, 150);
        SetMultimap<Integer, Long> distributionMoreAdded = getFullDistribution(mapper);

        int droppedOldMappingNodes = 0;
        for (Map.Entry<Integer, Collection<Long>> entry : distributionMoreAdded.asMap().entrySet()) {
            Integer partition = entry.getKey();
            Collection<Long> nodes = entry.getValue();
            Set<Long> oldMappingNodesSet = ImmutableSet.copyOf(distributionHalfDrained.asMap().get(partition));
            Set<Long> newMappingNodesSet = ImmutableSet.copyOf(nodes);
            droppedOldMappingNodes += Sets.difference(oldMappingNodesSet, newMappingNodesSet).size();
        }
        assertThat(droppedOldMappingNodes).isEqualTo(4); // 4 nodes will be moved due to rebalancing algorithm
    }

    @Test
    void testClusterSmallerThanMin()
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        SmartPinningPartitionNodeMapper mapper;

        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 2).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));
        mapper = new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 4, 4, 32, NO_WAIT);
        assertEvenNodesDistribution(mapper, 4, 2, 2);
    }

    @Test
    void testNoActiveNodes()
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        SmartPinningPartitionNodeMapper mapper;
        mapper = new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 4, 4, 32, Duration.succinctDuration(500, TimeUnit.MILLISECONDS));

        ListenableFuture<PartitionNodeMapping> mappingFuture = mapper.getMapping(0);
        assertThat(mappingFuture).isNotDone();
        assertThat(mappingFuture)
                .failsWithin(1, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withMessageContaining("no ACTIVE buffer nodes available");
    }

    @Test
    void testActiveNodesAppearWithinTimeout()
            throws InterruptedException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        SmartPinningPartitionNodeMapper mapper;
        mapper = new SmartPinningPartitionNodeMapper(EXCHANGE_ID, discoveryManager, executor, 4, 4, 32, Duration.succinctDuration(1000, TimeUnit.MILLISECONDS));

        ListenableFuture<PartitionNodeMapping> mappingFuture = mapper.getMapping(0);
        assertThat(mappingFuture).isNotDone();
        Thread.sleep(200);
        assertThat(mappingFuture).isNotDone(); // still not done

        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 2).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));
        assertThat(mappingFuture)
                .succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(getFutureValue(mappingFuture).mapping()).hasSize(4);
    }

    private void assertEvenNodesDistribution(
            SmartPinningPartitionNodeMapper mapper,
            int expectedPartitionsCount,
            int expectedNodesPerPartition,
            int expectedNodesUsed)
    {
        Map<Integer, Map<Long, Long>> partitionNodeCountMap = new HashMap<>(); // partition -> nodeId -> count
        Map<Long, Long> nodeCountMap = new HashMap<>();
        int probesCount = 10000;
        int expectedProbesPerNode = probesCount / expectedNodesPerPartition;
        for (int i = 0; i < probesCount; ++i) {
            Map<Integer, Long> mapping = getFutureValue(mapper.getMapping(0)).mapping();
            mapping.forEach((partition, nodeId) -> partitionNodeCountMap.computeIfAbsent(partition, (k) -> new HashMap<>()).merge(nodeId, 1L, Long::sum));
            mapping.forEach((partition, nodeId) -> nodeCountMap.merge(nodeId, 1L, Long::sum));
        }

        assertThat(partitionNodeCountMap.size()).as("partition count").isEqualTo(expectedPartitionsCount);

        IntStream.range(0, expectedPartitionsCount).forEach(partition -> {
            assertThat(partitionNodeCountMap).containsKey(partition);
            Map<Long, Long> counts = partitionNodeCountMap.get(partition);
            assertThat(counts.keySet()).hasSize(expectedNodesPerPartition);
            Stats countsStats = Stats.of(counts.values());
            assertThat(countsStats.mean()).isCloseTo(expectedProbesPerNode, Percentage.withPercentage(10));
            assertThat(countsStats.populationStandardDeviation()).isLessThan(0.1 * expectedProbesPerNode);
        });

        assertThat(nodeCountMap).hasSize(expectedNodesUsed);
        // TODO how to check the distribution among nodes?
    }

    private SetMultimap<Integer, Long> getFullDistribution(SmartPinningPartitionNodeMapper mapper)
    {
        ImmutableSetMultimap.Builder<Integer, Long> distribution = ImmutableSetMultimap.builder();
        int probesCount = 10000;
        for (int i = 0; i < probesCount; ++i) {
            Map<Integer, Long> mapping = getFutureValue(mapper.getMapping(0)).mapping();
            mapping.forEach(distribution::put);
        }
        return distribution.build();
    }
}
