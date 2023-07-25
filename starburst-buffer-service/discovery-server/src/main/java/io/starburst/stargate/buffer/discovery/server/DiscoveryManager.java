/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.discovery.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.InvalidBufferNodeUpdateException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.BufferNodeState.DRAINED;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DiscoveryManager
{
    private static final Logger LOG = Logger.get(DiscoveryManager.class);

    @VisibleForTesting
    static final Duration STALE_BUFFER_NODE_INFO_CLEANUP_THRESHOLD = succinctDuration(24, HOURS);
    @VisibleForTesting
    static final Duration DRAINED_NODES_STALENESS_THRESHOLD = succinctDuration(4, HOURS);

    private final Ticker ticker;
    private final ScheduledExecutorService executor;
    private final Duration bufferNodeDiscoveryStalenessThreshold;
    private final Duration startGracePeriod;

    private volatile long startTime;
    private final AtomicBoolean started = new AtomicBoolean();
    private final Map<Long, BufferNodeInfoHolder> nodeInfoHolders = new ConcurrentHashMap<>();

    private final AtomicReference<Set<BufferNodeInfo>> nodeInfosCache = new AtomicReference<>(ImmutableSet.of());

    @Inject
    public DiscoveryManager(@ForDiscoveryManager Ticker ticker, DiscoveryManagerConfig config)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        requireNonNull(config, "config is null");
        this.bufferNodeDiscoveryStalenessThreshold = config.getBufferNodeDiscoveryStalenessThreshold();
        checkArgument(bufferNodeDiscoveryStalenessThreshold.toMillis() <= DRAINED_NODES_STALENESS_THRESHOLD.toMillis(),
                "bufferNodeDiscoveryStalenessThreshold %s larger than DRAINED_NODES_STALENESS_THRESHOLD %s",
                bufferNodeDiscoveryStalenessThreshold, DRAINED_NODES_STALENESS_THRESHOLD);
        this.startGracePeriod = config.getStartGracePeriod();
        this.executor = newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void start()
    {
        checkState(started.compareAndSet(false, true), "Already started");
        startTime = tickerReadMillis();
        executor.scheduleWithFixedDelay(this::cleanup, 0, 5, SECONDS);
    }

    @PreDestroy
    public synchronized void stop()
    {
        executor.shutdownNow();
    }

    public boolean isInGracePeriod()
    {
        if (!started.get()) {
            return true;
        }
        return startTime + startGracePeriod.toMillis() > tickerReadMillis();
    }

    private long tickerReadMillis()
    {
        return ticker.read() / 1_000_000;
    }

    public void updateNodeInfos(BufferNodeInfo nodeInfo)
            throws InvalidBufferNodeUpdateException
    {
        long now = tickerReadMillis();
        BufferNodeInfoHolder holder = nodeInfoHolders.computeIfAbsent(nodeInfo.nodeId(), ignored -> {
            LOG.info("discovered new node %s", nodeInfo.nodeId());
            return new BufferNodeInfoHolder(nodeInfo, now);
        });

        URI storedNodeUri = holder.getLastNodeInfo().uri();
        if (!storedNodeUri.equals(nodeInfo.uri())) {
            throw new InvalidBufferNodeUpdateException("buffer node " + nodeInfo.nodeId() + " already seen with different uri: " + nodeInfo.uri() + " vs. " + storedNodeUri);
        }

        holder.updateNodeInfo(nodeInfo, now);
        rebuildNodeInfosCache();
    }

    private void rebuildNodeInfosCache()
    {
        while (true) {
            Set<BufferNodeInfo> oldValue = nodeInfosCache.get();
            ImmutableSet<BufferNodeInfo> newValue = nodeInfoHolders.values().stream()
                    .filter(holder -> !holder.isStale())
                    .map(BufferNodeInfoHolder::getLastNodeInfo)
                    .collect(toImmutableSet());

            if (nodeInfosCache.compareAndSet(oldValue, newValue)) {
                return;
            }
        }
    }

    public Set<BufferNodeInfo> getNodeInfos()
    {
        return nodeInfosCache.get();
    }

    @VisibleForTesting
    void cleanup()
    {
        Iterator<Map.Entry<Long, BufferNodeInfoHolder>> iterator = nodeInfoHolders.entrySet().iterator();
        long now = tickerReadMillis();
        long markStaleThreshold = now - bufferNodeDiscoveryStalenessThreshold.toMillis();
        long cleanupThreshold = now - STALE_BUFFER_NODE_INFO_CLEANUP_THRESHOLD.toMillis();
        long drainedNodesMarkStaleThreshold = now - DRAINED_NODES_STALENESS_THRESHOLD.toMillis();
        while (iterator.hasNext()) {
            Map.Entry<Long, BufferNodeInfoHolder> entry = iterator.next();
            BufferNodeInfoHolder infoHolder = entry.getValue();
            long lastUpdateTime = infoHolder.getLastUpdateTime();
            if (!infoHolder.isStale() && lastUpdateTime < markStaleThreshold) {
                BufferNodeState bufferNodeState = infoHolder.getLastNodeInfo().state();
                // Buffer nodes in drained state needs to be kept longer for the lifetime of a query,
                // to make sure that new workers will not read from a drained node
                if (!bufferNodeState.equals(DRAINED) || lastUpdateTime < drainedNodesMarkStaleThreshold) {
                    LOG.info("marking entry for node %s as stale; no update for %s; last state %s",
                            entry.getKey(),
                            succinctDuration(now - lastUpdateTime, MILLISECONDS),
                            infoHolder.getLastNodeInfo().state());
                    infoHolder.markStale();
                }
            }
            if (lastUpdateTime < cleanupThreshold) {
                LOG.info("deleting stale entry for node %s ; no update for %s", entry.getKey(), succinctDuration(now - lastUpdateTime, MILLISECONDS));
                iterator.remove();
            }
        }
        rebuildNodeInfosCache();
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    @interface ForDiscoveryManager {}
}
