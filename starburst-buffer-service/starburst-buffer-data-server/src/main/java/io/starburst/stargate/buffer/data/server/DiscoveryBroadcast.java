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
import io.starburst.stargate.buffer.discovery.client.InvalidBufferNodeUpdateException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.STARTING;
import static java.lang.Math.min;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DiscoveryBroadcast
{
    private static final Logger log = Logger.get(DiscoveryBroadcast.class);

    private final long bufferNodeId;
    private final DiscoveryApi discoverApi;
    private final BufferNodeStateManager stateManager;
    private final Supplier<BufferNodeInfo> bufferNodeInfoSupplier;

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
        this(bufferNodeId,
                discoveryApi,
                stateManager,
                requireNonNull(bufferNodeInfoService, "bufferNodeInfoService is null")::getNodeInfo,
                config);
    }

    DiscoveryBroadcast(
            BufferNodeId bufferNodeId,
            DiscoveryApi discoveryApi,
            BufferNodeStateManager stateManager,
            Supplier<BufferNodeInfo> bufferNodeInfoSupplier,
            DataServerConfig config)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.discoverApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.stateManager = requireNonNull(stateManager, "stateManager is null");
        this.bufferNodeInfoSupplier = requireNonNull(bufferNodeInfoSupplier, "bufferNodeInfoSupplier is null");
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

    /**
     * Requests an announcement without waiting for discovery.
     */
    public void requestBroadcast()
    {
        try {
            executor.execute(this::broadcast);
        }
        catch (RejectedExecutionException e) {
            log.warn(e, "Discovery broadcaster stopped before announcement could be scheduled");
        }
    }

    /**
     * Makes one best-effort announcement.
     */
    void broadcast()
    {
        try {
            broadcastOnce();
        }
        catch (InvalidBufferNodeUpdateException e) {
            // Do not let a rejected update terminate the periodic scheduled task.
            log.warn(e, "Discovery server rejected buffer node announcement");
        }
    }

    /**
     * Attempts publication at least once unless the thread is interrupted, retrying transient failures within the timeout.
     * The timeout bounds retries, not waiting for another announcement to release the monitor or an in-flight HTTP request.
     * Callers that require a hard waiting limit must enforce it separately.
     */
    public void broadcastUntilSuccess(Duration timeout)
            throws InterruptedException, TimeoutException
    {
        long deadlineNanos = nanoTime() + timeout.roundTo(NANOSECONDS);
        do {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted while publishing buffer node announcement");
            }
            if (broadcastOnce()) {
                return;
            }
            long remainingNanos = deadlineNanos - nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            NANOSECONDS.sleep(min(broadcastInterval.roundTo(NANOSECONDS), remainingNanos));
        }
        while (deadlineNanos - nanoTime() > 0);
        throw new TimeoutException("Timed out publishing buffer node announcement");
    }

    private synchronized boolean broadcastOnce()
    {
        try {
            BufferNodeInfo nodeInfo = bufferNodeInfoSupplier.get();
            if (nodeInfo.state() != STARTING) {
                discoverApi.updateBufferNode(nodeInfo);
                Boolean previousRegistrationState = discoveryRegistrationState.getAndSet(true);
                if (previousRegistrationState == null && stateManager.tryTransitionState(ACTIVE)) {
                    // Only first registering to discovery server marks Data Server as ACTIVE
                    // update the state in discovery server immediately
                    discoverApi.updateBufferNode(bufferNodeInfoSupplier.get());
                }
                if (previousRegistrationState == null || !previousRegistrationState) {
                    log.info("Marking registered (current state: %s)", stateManager.getState());
                }
                lastSuccessfulBroadcast.set(Instant.now());
                return true;
            }
        }
        catch (InvalidBufferNodeUpdateException e) {
            markBroadcastFailed();
            throw e;
        }
        catch (RuntimeException e) {
            markBroadcastFailed();
            log.warn(e, "Failed to announce to discovery server");
        }
        return false;
    }

    private void markBroadcastFailed()
    {
        if (lastSuccessfulBroadcast.get().plusMillis(broadcastFailureInactivityThreshold.toMillis()).isAfter(Instant.now())) {
            Boolean previousRegistrationState = discoveryRegistrationState.getAndSet(false);
            if (previousRegistrationState != null && previousRegistrationState) {
                log.warn("Marking unregistered (current state: %s)", stateManager.getState());
            }
        }
    }
}
