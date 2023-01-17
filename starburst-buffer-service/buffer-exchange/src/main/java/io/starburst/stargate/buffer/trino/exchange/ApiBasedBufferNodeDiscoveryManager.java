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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ApiBasedBufferNodeDiscoveryManager
        implements BufferNodeDiscoveryManager
{
    private static final Logger log = Logger.get(ApiBasedBufferNodeDiscoveryManager.class);

    private static final Duration REFRESH_INTERVAL = new Duration(5, SECONDS);
    private static final Duration MIN_FORCE_REFRESH_DELAY = new Duration(200, MILLISECONDS);
    private static final long READY_TIMEOUT_MILLIS = 30_000;

    private final DiscoveryApi discoveryApi;
    private final ListeningScheduledExecutorService executorService;
    private final AtomicReference<BufferNodesState> bufferNodes = new AtomicReference<>(new BufferNodesState(0, ImmutableMap.of()));
    private final SettableFuture<Void> readyFuture = SettableFuture.create();
    private final AtomicLong lastRefresh = new AtomicLong(0);
    @GuardedBy("this")
    private ListenableFuture<Void> forceRefreshFuture;
    private final AtomicBoolean logNextSuccessfulRefresh = new AtomicBoolean(true);

    @Inject
    public ApiBasedBufferNodeDiscoveryManager(
            ApiFactory apiFactory,
            ListeningScheduledExecutorService executorService)
    {
        this.discoveryApi = apiFactory.createDiscoveryApi();
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @PostConstruct
    public void start()
    {
        // todo monitor error rate
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
        if (now - lastRefresh.get() < MIN_FORCE_REFRESH_DELAY.toMillis()) {
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
            if (response.responseComplete()) {
                if (logNextSuccessfulRefresh.compareAndSet(true, false)) {
                    log.info("received COMPLETE buffer nodes info");
                }
                bufferNodes.set(new BufferNodesState(
                        System.currentTimeMillis(),
                        uniqueIndex(response.bufferNodeInfos(), BufferNodeInfo::nodeId)));
                readyFuture.set(null);
            }
            else {
                log.info("received INCOMPLETE buffer nodes info");
                logNextSuccessfulRefresh.set(true);
            }
        }
        catch (Exception e) {
            log.error(e, "Error getting buffer nodes info");
            logNextSuccessfulRefresh.set(true);
        }
    }

    @Override
    public BufferNodesState getBufferNodes()
    {
        try {
            readyFuture.get(READY_TIMEOUT_MILLIS, MILLISECONDS);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not get initial cluster state", e);
        }
        return bufferNodes.get();
    }
}
