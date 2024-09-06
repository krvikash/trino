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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.STARTING;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DiscoveryBroadcast
{
    private static final Logger log = Logger.get(DiscoveryBroadcast.class);

    private final long bufferNodeId;
    private final DiscoveryApi discoverApi;
    private final BufferNodeStateManager stateManager;
    private final BufferNodeInfoService bufferNodeInfoService;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("discovery-broadcast-%s"));
    private final AtomicReference<Boolean> discoveryRegistrationState = new AtomicReference<>(null);
    private final AtomicReference<Instant> lastSuccessfulBroadcast = new AtomicReference<>(Instant.ofEpochMilli(0)); // long time ago

    private final Duration broadcastInterval;
    private final Duration broadcastFailureInactivityThreshold;

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
        this.broadcastFailureInactivityThreshold = config.getBroadcastFailureInactivityThreshold();
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
                Boolean previousRegistrationState = discoveryRegistrationState.getAndSet(true);
                if (previousRegistrationState == null) {
                    // Only first registering to discovery server marks Data Server as ACTIVE
                    stateManager.transitionState(ACTIVE);
                    // update the state in discovery server immediately
                    discoverApi.updateBufferNode(bufferNodeInfoService.getNodeInfo());
                }
                if (previousRegistrationState == null || !previousRegistrationState) {
                    log.info("Marking registered");
                }
                lastSuccessfulBroadcast.set(Instant.now());
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to announce to discovery server. Retry in %s.", broadcastInterval);
                if (lastSuccessfulBroadcast.get().plusMillis(broadcastFailureInactivityThreshold.toMillis()).isAfter(Instant.now())) {
                    Boolean previousRegistrationState = discoveryRegistrationState.getAndSet(false);
                    if (previousRegistrationState != null && previousRegistrationState) {
                        log.warn("Marking unregistered");
                    }
                }
            }
        }
    }
}
