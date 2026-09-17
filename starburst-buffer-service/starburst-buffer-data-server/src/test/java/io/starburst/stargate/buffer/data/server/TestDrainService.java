/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.base.Ticker;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.execution.ChunkDataFactory;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.ExchangeChunkBytes;
import io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.TestingMemoryConfig;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createLocalSpoolingStorage;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDrainService
{
    @Test
    public void testDrainBroadcastsStateChangesToDiscovery()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            ChunkManager chunkManager = createChunkManager(dataServerStats, executor);
            List<BufferNodeState> broadcastStates = new CopyOnWriteArrayList<>();
            DiscoveryApi discoveryApi = createDiscoveryApiStub(broadcastStates);
            DiscoveryBroadcast discoveryBroadcast = createDiscoveryBroadcast(discoveryApi, bufferNodeStateManager);

            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, chunkManager, Optional.of(discoveryBroadcast));

            drainService.awaitDrain();

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
            assertEventually(() -> assertThat(broadcastStates).containsExactly(BufferNodeState.DRAINING, BufferNodeState.DRAINED));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testDrainFailureStillTransitionsToDrained()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            ChunkManager chunkManager = createChunkManager(dataServerStats, executor);
            // pre-drain the chunk manager directly, so DrainService's own attempt fails with
            // "already started draining" and exercises the exception path in DrainService.drain()
            chunkManager.drainAllChunks();

            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, chunkManager, Optional.empty());

            drainService.awaitDrain();

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static DrainService createDrainService(BufferNodeStateManager bufferNodeStateManager, DataServerStats dataServerStats, ChunkManager chunkManager, Optional<DiscoveryBroadcast> discoveryBroadcast)
    {
        return new DrainService(
                new DataServerConfig().setDrainAllChunksTimeout(succinctDuration(10, SECONDS)),
                bufferNodeStateManager,
                new AddDataPagesInProgressTracker(dataServerStats),
                chunkManager,
                discoveryBroadcast);
    }

    @Test
    public void testDrainIdempotency()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DataServerStats dataServerStats = new DataServerStats();
            BufferNodeStateManager bufferNodeStateManager = new BufferNodeStateManager();
            ChunkManager chunkManager = createChunkManager(dataServerStats, executor);
            DrainService drainService = createDrainService(bufferNodeStateManager, dataServerStats, chunkManager, Optional.empty());

            drainService.drain();
            drainService.drain(); // second direct call should be a no-op since draining already started
            drainService.awaitDrain(); // third call inside awaiting should also be a no-op since draining already started

            assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static DiscoveryBroadcast createDiscoveryBroadcast(DiscoveryApi discoveryApi, BufferNodeStateManager bufferNodeStateManager)
    {
        return new DiscoveryBroadcast(
                new BufferNodeId(0L),
                discoveryApi,
                bufferNodeStateManager,
                () -> new BufferNodeInfo(0L, URI.create("http://buffer-node-0"), Optional.empty(), Optional.empty(), bufferNodeStateManager.getState(), Instant.now()),
                new DataServerConfig());
    }

    private static DiscoveryApi createDiscoveryApiStub(List<BufferNodeState> broadcastStates)
    {
        return new DiscoveryApi()
        {
            @Override
            public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
            {
                broadcastStates.add(bufferNodeInfo.state());
            }

            @Override
            public BufferNodeInfoResponse getBufferNodes()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static ChunkManager createChunkManager(DataServerStats dataServerStats, ExecutorService executor)
    {
        DataServerConfig dataServerConfig = new DataServerConfig()
                .setTraceResourceReportingEnabled(false)
                .setMinDrainingDuration(succinctDuration(0, SECONDS));
        ChunkManagerConfig chunkManagerConfig = new ChunkManagerConfig();
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, DataSize.Unit.MEGABYTE)),
                new MemoryAllocatorConfig(),
                chunkManagerConfig,
                dataServerStats);
        SpoolingStorage spoolingStorage = createLocalSpoolingStorage();
        ExchangeChunkBytes exchangeAllocatedBytes = new ExchangeChunkBytes();
        ChunkDataFactory chunkDataFactory = new ChunkDataFactory(
                Optional.empty(), memoryAllocator, executor, Optional.empty(), exchangeAllocatedBytes, dataServerStats, chunkManagerConfig, dataServerConfig);
        return new ChunkManager(
                new BufferNodeId(0L),
                new BufferNodeStateManager(),
                chunkManagerConfig,
                dataServerConfig,
                memoryAllocator,
                spoolingStorage,
                Ticker.systemTicker(),
                new SpooledChunksByExchange(),
                exchangeAllocatedBytes,
                Optional.empty(),
                chunkDataFactory,
                dataServerStats,
                _ -> null,
                executor);
    }
}
