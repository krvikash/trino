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

import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.BufferNodeStats;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DiscoveryBroadcast
{
    private final DiscoveryApi discoverApi;
    private final long bufferNodeId;
    private final URI baseUri;
    private final MemoryAllocator memoryAllocator;
    private final ChunkManager chunkManager;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    @Inject
    public DiscoveryBroadcast(
            DiscoveryApi discoveryApi,
            @BufferNodeId long bufferNodeId,
            MemoryAllocator memoryAllocator,
            ChunkManager chunkManager,
            HttpServerInfo httpServerInfo,
            NodeInfo airliftNodeInfo)
    {
        this.discoverApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.bufferNodeId = bufferNodeId;
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");

        baseUri = UriBuilder.fromUri(httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri())
                .host(airliftNodeInfo.getExternalAddress())
                .build();
    }

    private final AtomicReference<BufferNodeState> bufferNodeState = new AtomicReference<>(BufferNodeState.STARTING);
    private final AtomicBoolean stopped = new AtomicBoolean();

    @PostConstruct
    public void start()
    {
        checkState(bufferNodeState.compareAndSet(BufferNodeState.STARTING, BufferNodeState.RUNNING), "already started");
        executor.scheduleWithFixedDelay(this::broadcast, 0, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        checkState(stopped.compareAndSet(false, true), "already stopped");
        executor.shutdownNow();
    }

    private void broadcast()
    {
        discoverApi.updateBufferNode(new BufferNodeInfo(bufferNodeId, baseUri, getBufferNodeStats(), bufferNodeState.get()));
    }

    private Optional<BufferNodeStats> getBufferNodeStats()
    {
        BufferNodeStats bufferNodeStats = new BufferNodeStats(
                memoryAllocator.getTotalMemory(),
                memoryAllocator.getFreeMemory(),
                chunkManager.getTrackedExchanges(),
                chunkManager.getOpenChunks(),
                chunkManager.getClosedChunks());
        return Optional.of(bufferNodeStats);
    }
}
