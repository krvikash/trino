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
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class SinkMappingScaler
{
    private static final Logger log = Logger.get(SinkMappingScaler.class);

    private PartitionNodeMapping mapping;
    private final Duration minTimeBetweenScaleUps;
    private final double maxScaleUpGrowthFactor;
    private Optional<Stopwatch> lastScaleUp = Optional.empty();
    private final String externalExchangeId;
    private final int taskPartitionId;
    private final Ticker ticker;
    private boolean scaleUpLimit;

    private final Map<Integer, Integer> targetNodeCountPerPartition = new HashMap<>();

    public SinkMappingScaler(
            PartitionNodeMapping partitionNodeMapping,
            Duration minTimeBetweenScaleUps,
            double maxScaleUpGrowthFactor,
            String externalExchangeId,
            int taskPartitionId)
    {
        this(partitionNodeMapping, minTimeBetweenScaleUps, maxScaleUpGrowthFactor, externalExchangeId, taskPartitionId, Ticker.systemTicker());
    }

    @VisibleForTesting
    SinkMappingScaler(
            PartitionNodeMapping partitionNodeMapping,
            Duration minTimeBetweenScaleUps,
            double maxScaleUpGrowthFactor,
            String externalExchangeId,
            int taskPartitionId,
            Ticker ticker)
    {
        receivedUpdatedPartitionNodeMapping(partitionNodeMapping);
        this.minTimeBetweenScaleUps = requireNonNull(minTimeBetweenScaleUps, "minTimeBetweenScaleUps is null");
        checkArgument(maxScaleUpGrowthFactor > 1.0, "maxScaleUpGrowthFactor must be greater than 1.0");
        this.maxScaleUpGrowthFactor = maxScaleUpGrowthFactor;
        this.externalExchangeId = externalExchangeId;
        this.taskPartitionId = taskPartitionId;
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public synchronized void receivedUpdatedPartitionNodeMapping(PartitionNodeMapping mapping)
    {
        this.mapping = requireNonNull(mapping, "mapping is null");
        for (Integer partitionId : mapping.getBaseNodesCount().keySet()) {
            Integer baseCount = mapping.getBaseNodesCount().get(partitionId);
            Integer previousTargetCount = targetNodeCountPerPartition.getOrDefault(partitionId, 0);
            int availableNodes = mapping.getMapping().get(partitionId).size();
            Integer newTargetCount = min(availableNodes, max(baseCount, previousTargetCount));
            targetNodeCountPerPartition.put(partitionId, newTargetCount);
        }
        // reset scaleUpLimit flag
        scaleUpLimit = false;
    }

    public synchronized Result process(SinkDataPool dataPool, BufferExchangeSink.ActiveMapping activeMapping)
    {
        return process(new SinkStateProvider() {
            @Override
            public Set<Long> getActiveBufferNodesForPartition(Integer partition)
            {
                return activeMapping.getBufferNodesForPartition(partition);
            }

            @Override
            public Map<Integer, Long> getAddedDataDistribution()
            {
                return dataPool.getAddedDataDistribution();
            }

            @Override
            public Optional<Duration> timeSinceDataPoolLastFull()
            {
                return dataPool.timeSinceDataPoolLastFull();
            }
        });
    }

    public synchronized Result process(SinkStateProvider sinkStateProvider)
    {
        scaleUpIfNeeded(sinkStateProvider);

        // update mapping
        Multimap<Integer, Long> writersToAdd = HashMultimap.create();
        for (Integer partition : mapping.getMapping().keySet()) {
            Set<Long> assignedBufferNodes = sinkStateProvider.getActiveBufferNodesForPartition(partition);
            List<Long> mappingNodes = mapping.getMapping().get(partition);
            Integer baseNodesCount = mapping.getBaseNodesCount().get(partition);
            int missing = targetNodeCountPerPartition.get(partition) - sinkStateProvider.getActiveBufferNodesForPartition(partition).size();

            int pos = 0;
            for (Long bufferNodeId : mappingNodes) {
                if (pos >= baseNodesCount && missing <= 0) {
                    // bail out if we already added all base nodes, and no need to add anymore.
                    break;
                }
                if (!assignedBufferNodes.contains(bufferNodeId)) {
                    writersToAdd.put(partition, bufferNodeId);
                    missing--;
                }
                pos++;
            }
        }
        return new Result(writersToAdd);
    }

    @GuardedBy("this")
    private void scaleUpIfNeeded(SinkStateProvider sinkStateProvider)
    {
        if (scaleUpLimit) {
            return;
        }

        if (lastScaleUp.isPresent() && lastScaleUp.get().elapsed(TimeUnit.MILLISECONDS) < minTimeBetweenScaleUps.toMillis()) {
            // cant scale up too often
            return;
        }

        Optional<Duration> timeSinceLastFull = sinkStateProvider.timeSinceDataPoolLastFull();
        if (timeSinceLastFull.isEmpty()) {
            // we are keeping up with writing so no need for scale-up
            return;
        }

        if (lastScaleUp.isPresent() && lastScaleUp.get().elapsed(TimeUnit.MILLISECONDS) <
                timeSinceLastFull.get().toMillis() + minTimeBetweenScaleUps.toMillis() * 0.3) {
            // last time we were full was before previous scale-up or not much after it
            return;
        }

        // scale-up
        Map<Integer, Long> addedDataDistribution = sinkStateProvider.getAddedDataDistribution();
        long totalDataAdded = addedDataDistribution.values().stream().mapToLong(value -> value).sum();

        boolean scaledUp = false;
        for (Integer partition : mapping.getMapping().keySet()) {
            Long dataAdded = addedDataDistribution.getOrDefault(partition, 0L);
            double fraction = (double) dataAdded / totalDataAdded;
            int currentTarget = targetNodeCountPerPartition.get(partition);

            int totalNodes = mapping.getMapping().get(partition).size();

            // determine number of buffer nodes to be used based on distribution fraction for a partition
            int newTarget = max((int) ceil(totalNodes * fraction), currentTarget);
            // but not scale up too fast
            newTarget = min(newTarget, (int) ceil(currentTarget * maxScaleUpGrowthFactor));
            // and not exceed number of available nodes
            newTarget = min(newTarget, totalNodes);

            if (newTarget != currentTarget) {
                scaledUp = true;
                targetNodeCountPerPartition.put(partition, newTarget);
            }
        }
        if (scaledUp) {
            lastScaleUp = Optional.of(Stopwatch.createStarted(ticker));
            log.debug("scaled up writers for %s.%s", externalExchangeId, taskPartitionId);
        }
        else {
            scaleUpLimit = true;
            log.info("got to scaling limit for %s.%s", externalExchangeId, taskPartitionId);
        }
    }

    public record Result(Multimap<Integer, Long> writersToAdd)
    {
        public static final Result EMPTY = new Result(ImmutableMultimap.of());

        public Result
        {
            requireNonNull(writersToAdd, "writersToAdd is null");
        }
    }

    interface SinkStateProvider
    {
        Set<Long> getActiveBufferNodesForPartition(Integer partition);

        Map<Integer, Long> getAddedDataDistribution();

        Optional<Duration> timeSinceDataPoolLastFull();
    }
}
