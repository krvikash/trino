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
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BufferNodeDiscoveryManager
{
    private static final Duration REFRESH_INTERVAL = new Duration(5, SECONDS);
    private static final long READY_TIMEOUT_MILLIS = 30_000;

    private final DiscoveryApi discoveryApi;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<BufferNodesState> bufferNodes = new AtomicReference<>(new BufferNodesState(0, ImmutableMap.of()));
    private final SettableFuture<Void> readyFuture = SettableFuture.create();

    @Inject
    public BufferNodeDiscoveryManager(
            ApiFactory apiFactory,
            ScheduledExecutorService executorService)
    {
        this.discoveryApi = apiFactory.createDiscoveryApi();
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @PostConstruct
    public void start()
    {
        executorService.scheduleWithFixedDelay(
                () -> {
                    // todo monitor error rate
                    BufferNodeInfoResponse response = discoveryApi.getBufferNodes();
                    if (response.responseComplete()) {
                        bufferNodes.set(new BufferNodesState(
                                System.currentTimeMillis(),
                                uniqueIndex(response.bufferNodeInfos(), BufferNodeInfo::nodeId)));
                        readyFuture.set(null);
                    }
                },
                0,
                REFRESH_INTERVAL.toMillis(),
                MILLISECONDS);
    }

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

    public record BufferNodesState(
            long timestamp,
            Map<Long, BufferNodeInfo> bufferNodeInfos) {}
}
