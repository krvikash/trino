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

import io.starburst.stargate.buffer.trino.exchange.MetricsBuilder.CounterMetricBuilder;
import io.starburst.stargate.buffer.trino.exchange.MetricsBuilder.DistributionMetricBuilder;
import io.trino.spi.metrics.Metrics;

/**
 * Tracks metrics for exchange operations on the coordinator side.
 * Records coordinator-tracked events like sink creation, source handle generation,
 * and chunk discovery on the coordinator.
 * Thread-safe via internal synchronization.
 */
class ExchangeCoordinatorMetricsBuilder
{
    private final MetricsBuilder metricsBuilder = new MetricsBuilder();

    // Exchange operation metrics
    private final CounterMetricBuilder sinkAddedMetric = metricsBuilder.getCounterMetric("BufferExchange.sinkAddedTotal");
    private final CounterMetricBuilder sinkInstancesCreatedMetric = metricsBuilder.getCounterMetric("BufferExchange.sinkInstancesCreatedTotal");
    private final CounterMetricBuilder sinkInstancesUpdatedMetric = metricsBuilder.getCounterMetric("BufferExchange.sinkInstancesUpdatedTotal");
    private final CounterMetricBuilder chunksDiscoveredMetric = metricsBuilder.getCounterMetric("BufferExchange.chunksDiscoveredTotal");
    private final CounterMetricBuilder sourceHandlesCreatedMetric = metricsBuilder.getCounterMetric("BufferExchange.sourceHandlesCreatedTotal");
    private final CounterMetricBuilder bufferNodesPolledMetric = metricsBuilder.getCounterMetric("BufferExchange.bufferNodesPolledTotal");

    // Distribution metrics
    private final DistributionMetricBuilder chunkSizeBytesMetric = metricsBuilder.getDistributionMetric("BufferExchange.chunkSizeBytes");
    private final DistributionMetricBuilder sourceHandleChunksCountMetric = metricsBuilder.getDistributionMetric("BufferExchange.sourceHandleChunksCount");
    private final DistributionMetricBuilder sourceHandleDataSizeBytesMetric = metricsBuilder.getDistributionMetric("BufferExchange.sourceHandleDataSizeBytes");

    public synchronized void incrementSinkAdded()
    {
        sinkAddedMetric.increment();
    }

    public synchronized void incrementSinkInstanceCreated()
    {
        sinkInstancesCreatedMetric.increment();
    }

    public synchronized void incrementSinkInstanceUpdated()
    {
        sinkInstancesUpdatedMetric.increment();
    }

    public synchronized void recordSourceHandleCreated(int chunkCount, long dataSize)
    {
        sourceHandlesCreatedMetric.increment();
        sourceHandleChunksCountMetric.add(chunkCount);
        sourceHandleDataSizeBytesMetric.add(dataSize);
    }

    public synchronized void recordChunkDiscovered(long dataSizeInBytes)
    {
        chunksDiscoveredMetric.increment();
        chunkSizeBytesMetric.add(dataSizeInBytes);
    }

    public synchronized void incrementBufferNodePolled()
    {
        bufferNodesPolledMetric.increment();
    }

    public synchronized Metrics buildMetrics()
    {
        return metricsBuilder.buildMetrics();
    }
}
