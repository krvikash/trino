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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.STARTING;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DiscoveryBroadcast
{
    private static final Logger log = Logger.get(DiscoveryBroadcast.class);

    private final long bufferNodeId;
    private final DiscoveryApi discoverApi;
    private final BufferNodeStateManager stateManager;
    private final BufferNodeInfoService bufferNodeInfoService;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final AtomicReference<Boolean> discoveryRegistrationState = new AtomicReference<>(null);

    private final Duration broadcastInterval;

    @Inject
    public DiscoveryBroadcast(
            BufferNodeId bufferNodeId,
            DiscoveryApi discoveryApi,
            BufferNodeStateManager stateManager,
            BufferNodeInfoService bufferNodeInfoService,
            DataServerConfig config)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.discoverApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.stateManager = requireNonNull(stateManager, "stateManager is null");
        this.bufferNodeInfoService = requireNonNull(bufferNodeInfoService, "bufferNodeInfoService is null");
        this.broadcastInterval = config.getBroadcastInterval();
    }

    private final AtomicBoolean stopped = new AtomicBoolean();

    @PostConstruct
    public void start()
    {
        log.info("Starting broadcasting info buffer node " + bufferNodeId);
        executor.scheduleWithFixedDelay(this::broadcast, 0, broadcastInterval.toMillis(), TimeUnit.MILLISECONDS);
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

    public void broadcast()
    {
        BufferNodeInfo nodeInfo = bufferNodeInfoService.getNodeInfo();
        if (nodeInfo.state() != STARTING) {
            try {
                discoverApi.updateBufferNode(nodeInfo);
                if (isNull(discoveryRegistrationState.getAndSet(true))) {
                    // Only first registering to discovery server marks Data Server as ACTIVE
                    stateManager.transitionState(ACTIVE);
                    // update the state in discovery server immediately
                    discoverApi.updateBufferNode(bufferNodeInfoService.getNodeInfo());
                }
            }
            catch (RuntimeException e) {
                if (isRegistered()) {
                    discoveryRegistrationState.set(false);
                }
                log.warn(e, "Failed to announce to discovery server. Retry in %s.", broadcastInterval);
            }
        }
    }
}
