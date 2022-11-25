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
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeStats;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.STARTING;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DiscoveryBroadcast
{
    private static final Logger log = Logger.get(DiscoveryBroadcast.class);

    private static final int BROADCAST_INTERVAL_SECONDS = 5;
    private final DiscoveryApi discoverApi;
    private final long bufferNodeId;
    private final URI baseUri;
    private final MemoryAllocator memoryAllocator;
    private final ChunkManager chunkManager;
    private final BufferNodeStateManager stateManager;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final AtomicReference<Boolean> discoveryRegistrationState = new AtomicReference<>(null);

    @Inject
    public DiscoveryBroadcast(
            DiscoveryApi discoveryApi,
            BufferNodeId bufferNodeId,
            MemoryAllocator memoryAllocator,
            ChunkManager chunkManager,
            HttpServerInfo httpServerInfo,
            NodeInfo airliftNodeInfo,
            BufferNodeStateManager stateManager)
    {
        this.discoverApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.stateManager = requireNonNull(stateManager, "stateManager is null");

        baseUri = uriBuilderFrom(httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri())
                .host(airliftNodeInfo.getExternalAddress())
                .build();
    }

    private final AtomicBoolean stopped = new AtomicBoolean();

    @PostConstruct
    public void start()
    {
        log.info("Starting broadcasting info buffer node " + bufferNodeId);
        executor.scheduleWithFixedDelay(this::broadcast, 0, BROADCAST_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public boolean isRegistered()
    {
        return Optional.ofNullable(discoveryRegistrationState.get()).orElse(false);
    }

    @PreDestroy
    public void stop()
    {
        checkState(stopped.compareAndSet(false, true), "already stopped");
        executor.shutdownNow();
    }

    private void broadcast()
    {
        if (stateManager.getState() != STARTING) {
            try {
                discoverApi.updateBufferNode(new BufferNodeInfo(bufferNodeId, baseUri, getBufferNodeStats(), stateManager.getState()));
                if (isNull(discoveryRegistrationState.getAndSet(true))) {
                    // Only first registering to discovery server marks Data Server as ACTIVE
                    stateManager.transitionState(ACTIVE);
                }
            }
            catch (RuntimeException e) {
                if (isRegistered()) {
                    discoveryRegistrationState.set(false);
                }
                log.warn(e, "Failed to announce to discovery server. Retry in %s s.", BROADCAST_INTERVAL_SECONDS);
            }
        }
    }

    private Optional<BufferNodeStats> getBufferNodeStats()
    {
        BufferNodeStats bufferNodeStats = new BufferNodeStats(
                memoryAllocator.getTotalMemory(),
                memoryAllocator.getFreeMemory(),
                chunkManager.getTrackedExchanges(),
                chunkManager.getOpenChunks(),
                chunkManager.getClosedChunks(),
                chunkManager.getSpooledChunks());
        return Optional.of(bufferNodeStats);
    }
}
