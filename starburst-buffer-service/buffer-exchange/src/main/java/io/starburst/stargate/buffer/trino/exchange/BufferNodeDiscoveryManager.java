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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BufferNodeDiscoveryManager
{
    private static final Logger log = Logger.get(BufferNodeDiscoveryManager.class);

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
        AtomicBoolean logNextSuccess = new AtomicBoolean(true);
        executorService.scheduleWithFixedDelay(
                () -> {
                    try {
                        // todo monitor error rate
                        BufferNodeInfoResponse response = discoveryApi.getBufferNodes();
                        if (response.responseComplete()) {
                            if (logNextSuccess.compareAndSet(true, false)) {
                                log.info("received COMPLETE buffer nodes info");
                            }
                            bufferNodes.set(new BufferNodesState(
                                    System.currentTimeMillis(),
                                    uniqueIndex(response.bufferNodeInfos(), BufferNodeInfo::nodeId)));
                            readyFuture.set(null);
                        }
                        else {
                            log.info("received INCOMPLETE buffer nodes info");
                            logNextSuccess.set(true);
                        }
                    }
                    catch (Exception e) {
                        log.error(e, "Error getting buffer nodes info");
                        logNextSuccess.set(true);
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

    public static class BufferNodesState
    {
        private final long timestamp;
        private final Map<Long, BufferNodeInfo> allBufferNodes;
        private final Map<Long, BufferNodeInfo> activeBufferNodes;
        private final Set<BufferNodeInfo> activeBufferNodesSet;

        BufferNodesState(
                long timestamp,
                Map<Long, BufferNodeInfo> allBufferNodes)
        {
            this.timestamp = timestamp;
            this.allBufferNodes = ImmutableMap.copyOf(allBufferNodes);
            this.activeBufferNodes = allBufferNodes.entrySet().stream()
                    .filter(entry -> entry.getValue().state() == BufferNodeState.ACTIVE)
                    .peek(entry -> checkArgument(entry.getValue().stats().isPresent(), "stats not set for ACTIVE node %s", entry.getValue()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            this.activeBufferNodesSet = allBufferNodes.values().stream()
                    .filter(info -> info.state() == BufferNodeState.ACTIVE)
                    .peek(info -> checkArgument(info.stats().isPresent(), "stats not set for ACTIVE node %s", info))
                    .collect(toImmutableSet());
        }

        public long getTimestamp()
        {
            return timestamp;
        }

        public Map<Long, BufferNodeInfo> getAllBufferNodes()
        {
            return allBufferNodes;
        }

        public Map<Long, BufferNodeInfo> getActiveBufferNodes()
        {
            return activeBufferNodes;
        }

        public Set<BufferNodeInfo> getActiveBufferNodesSet()
        {
            return activeBufferNodesSet;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("timestamp", timestamp)
                    .add("allBufferNodes", allBufferNodes)
                    .add("activeBufferNodes", activeBufferNodes)
                    .toString();
        }
    }
}
