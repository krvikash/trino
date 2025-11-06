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
import com.google.common.collect.Multimaps;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.node.InternalNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.LongStream;

import static io.airlift.units.Duration.succinctNanos;
import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLocalPriorityPartitionNodeMapper
{
    private static final Duration NO_WAIT = succinctNanos(0);

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(4);

    @AfterAll
    public void teardown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testGetMappingNoTaskNodeRequested()
            throws ExecutionException, InterruptedException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 10).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));

        LocalPriorityPartitionNodeMapper mapper = new LocalPriorityPartitionNodeMapper(discoveryManager, executor, 4, 3, NO_WAIT);

        PartitionNodeMapping mapping = mapper.getMapping(1, Optional.empty()).get();
        assertThat(mapping.getBaseNodesCount()).isEqualTo(ImmutableMap.of(0, 1, 1, 1, 2, 1, 3, 1));
        assertThat(Multimaps.asMap(mapping.getMapping())).allSatisfy((_, values) -> {
            assertThat(values).hasSize(3);
            assertThat(ImmutableSet.copyOf(values)).hasSize(3);
        });
    }

    @Test
    public void testGetMappingTaskNodeMissingFromCluster()
            throws ExecutionException, InterruptedException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 10).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));

        LocalPriorityPartitionNodeMapper mapper = new LocalPriorityPartitionNodeMapper(discoveryManager, executor, 4, 3, NO_WAIT);

        InternalNode dummyNode = new InternalNode("dummy", URI.create("http://dummy:80"), NodeVersion.UNKNOWN, false);
        PartitionNodeMapping mapping = mapper.getMapping(1, Optional.of(dummyNode)).get();
        assertThat(mapping.getBaseNodesCount()).isEqualTo(ImmutableMap.of(0, 1, 1, 1, 2, 1, 3, 1));
        assertThat(Multimaps.asMap(mapping.getMapping())).allSatisfy((_, values) -> {
            assertThat(values).hasSize(3);
            assertThat(ImmutableSet.copyOf(values)).hasSize(3);
        });
    }

    @Test
    public void testGetMappingWithNode()
            throws ExecutionException, InterruptedException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 10).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));

        LocalPriorityPartitionNodeMapper mapper = new LocalPriorityPartitionNodeMapper(discoveryManager, executor, 4, 3, NO_WAIT);

        InternalNode node3 = new InternalNode("node3", URI.create("http://node3:80"), NodeVersion.UNKNOWN, false);
        PartitionNodeMapping mapping = mapper.getMapping(1, Optional.of(node3)).get();
        assertThat(mapping.getBaseNodesCount()).isEqualTo(ImmutableMap.of(0, 1, 1, 1, 2, 1, 3, 1));
        assertThat(Multimaps.asMap(mapping.getMapping()))
                .allSatisfy((_, values) -> {
                    assertThat(values).hasSize(3);
                    assertThat(ImmutableSet.copyOf(values)).hasSize(3);
                    assertThat(values.get(0)).isEqualTo(3);
                });
    }

    @Test
    public void testGetMappingSmallCluster()
            throws ExecutionException, InterruptedException
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> LongStream.range(0, 2).forEach(nodeId -> builder.putNode(nodeId, ACTIVE)));

        LocalPriorityPartitionNodeMapper mapper = new LocalPriorityPartitionNodeMapper(discoveryManager, executor, 4, 3, NO_WAIT);

        InternalNode node1 = new InternalNode("node1", URI.create("http://node1:80"), NodeVersion.UNKNOWN, false);
        PartitionNodeMapping mapping = mapper.getMapping(1, Optional.of(node1)).get();
        assertThat(mapping.getBaseNodesCount()).isEqualTo(ImmutableMap.of(0, 1, 1, 1, 2, 1, 3, 1));
        assertThat(Multimaps.asMap(mapping.getMapping()))
                .allSatisfy((_, values) -> {
                    assertThat(values).hasSize(2);
                    assertThat(ImmutableSet.copyOf(values)).hasSize(2);
                    assertThat(values.get(0)).isEqualTo(1);
                });
    }
}
