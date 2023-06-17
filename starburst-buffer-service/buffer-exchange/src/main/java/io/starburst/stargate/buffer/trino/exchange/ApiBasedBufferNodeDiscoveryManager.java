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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import jakarta.annotation.PostConstruct;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.BufferNodeState.DRAINED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ApiBasedBufferNodeDiscoveryManager
        implements BufferNodeDiscoveryManager
{
    private static final Logger log = Logger.get(ApiBasedBufferNodeDiscoveryManager.class);

    private static final BufferNodesState EMPTY_BUFFER_NODES_STATE = new BufferNodesState(0, ImmutableMap.of());
    private static final Duration REFRESH_INTERVAL = new Duration(5, SECONDS);
    private static final Duration MIN_FORCE_REFRESH_DELAY = new Duration(200, MILLISECONDS);
    private static final Duration READY_TIMEOUT_MILLIS = succinctDuration(30, SECONDS);
    @VisibleForTesting
    static final Duration DRAINED_NODES_KEEP_TIMEOUT = succinctDuration(12, HOURS);
    private static final Object MARKER = new Object();

    private final DiscoveryApi discoveryApi;
    private final ListeningScheduledExecutorService executorService;
    private final Duration minForceRefreshDelay;
    @GuardedBy("this")
    private final Map<Long, BufferNodeInfo> bufferNodeInfos = new HashMap<>();
    private final Cache<Long, Object> drainedNodes;
    private final AtomicReference<BufferNodesState> bufferNodes = new AtomicReference<>(EMPTY_BUFFER_NODES_STATE);
    private final AtomicLong lastRefresh = new AtomicLong(0);
    @GuardedBy("this")
    private ListenableFuture<Void> forceRefreshFuture;
    private final AtomicBoolean logNextSuccessfulRefresh = new AtomicBoolean(true);

    @Inject
    public ApiBasedBufferNodeDiscoveryManager(
            ApiFactory apiFactory,
            ListeningScheduledExecutorService executorService)
    {
        this(apiFactory, executorService, MIN_FORCE_REFRESH_DELAY, READY_TIMEOUT_MILLIS, systemTicker());
    }

    @VisibleForTesting
    ApiBasedBufferNodeDiscoveryManager(
            ApiFactory apiFactory,
            ListeningScheduledExecutorService executorService,
            Duration minForceRefreshDelay,
            Duration readyTimout,
            Ticker ticker)
    {
        this.discoveryApi = apiFactory.createDiscoveryApi();
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.minForceRefreshDelay = requireNonNull(minForceRefreshDelay, "minForceRefreshDelay is null");
        this.drainedNodes = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(DRAINED_NODES_KEEP_TIMEOUT.toMillis(), MILLISECONDS)
                .build();
    }

    @PostConstruct
    public void start()
    {
        // todo monitor error rate
        log.info("Initializing periodic refreshing of buffer nodes");
        executorService.scheduleWithFixedDelay(
                this::doRefresh,
                0,
                REFRESH_INTERVAL.toMillis(),
                MILLISECONDS);
    }

    @Override
    public synchronized ListenableFuture<Void> forceRefresh()
    {
        if (forceRefreshFuture != null && !forceRefreshFuture.isDone()) {
            return forceRefreshFuture;
        }
        long now = System.currentTimeMillis();
        if (now - lastRefresh.get() < minForceRefreshDelay.toMillis()) {
            return Futures.immediateVoidFuture();
        }
        forceRefreshFuture = nonCancellationPropagating(asVoid(executorService.submit(this::doRefresh)));
        return forceRefreshFuture;
    }

    private void doRefresh()
    {
        try {
            BufferNodeInfoResponse response = discoveryApi.getBufferNodes();
            lastRefresh.set(System.currentTimeMillis());
            synchronized (this) {
                if (response.responseComplete()) {
                    if (logNextSuccessfulRefresh.compareAndSet(true, false)) {
                        log.info("received COMPLETE buffer nodes info");
                    }
                    bufferNodeInfos.clear();
                }
                else {
                    log.info("received INCOMPLETE buffer nodes info");
                    logNextSuccessfulRefresh.set(true);
                }

                for (BufferNodeInfo bufferNodeInfo : response.bufferNodeInfos()) {
                    bufferNodeInfos.put(bufferNodeInfo.nodeId(), bufferNodeInfo);
                    if (bufferNodeInfo.state() == DRAINED) {
                        drainedNodes.put(bufferNodeInfo.nodeId(), MARKER);
                    }
                }
                bufferNodes.set(buildBufferNodesState());
            }
        }
        catch (Throwable e) {
            log.error(e, "Error getting buffer nodes info");
            logNextSuccessfulRefresh.set(true);
        }
    }

    @GuardedBy("this")
    private BufferNodesState buildBufferNodesState()
    {
        Map<Long, BufferNodeInfo> bufferNodeInfosWithDrained = new HashMap<>(bufferNodeInfos);
        for (Long drainedNodeId : drainedNodes.asMap().keySet()) {
            if (bufferNodeInfosWithDrained.containsKey(drainedNodeId) && bufferNodeInfosWithDrained.get(drainedNodeId).state() != DRAINED) {
                log.warn("Discovery server reported node which was DRAINED previously %s", bufferNodeInfosWithDrained.get(drainedNodeId));
                continue;
            }
            bufferNodeInfosWithDrained.put(drainedNodeId, new BufferNodeInfo(drainedNodeId, URI.create("http://drained_" + drainedNodeId), Optional.empty(), DRAINED, Instant.now()));
        }
        return new BufferNodesState(
                System.currentTimeMillis(),
                bufferNodeInfosWithDrained);
    }

    @Override
    public BufferNodesState getBufferNodes()
    {
        return bufferNodes.get();
    }
}
