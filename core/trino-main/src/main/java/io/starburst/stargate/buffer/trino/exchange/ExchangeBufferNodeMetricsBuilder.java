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

import io.starburst.stargate.buffer.data.client.BufferNodeExchangeMetrics;
import io.trino.spi.metrics.Metrics;

import java.util.concurrent.ConcurrentHashMap;

import static io.starburst.stargate.buffer.trino.exchange.MetricsBuilder.CounterMetricBuilder;
import static io.starburst.stargate.buffer.trino.exchange.MetricsBuilder.DistributionMetricBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Tracks and aggregates metrics from buffer nodes.
 * Maintains the latest metrics snapshot from each buffer node,
 * replacing old values when new metrics arrive (e.g., from periodic pings).
 * Thread-safe via internal synchronization.
 */
class ExchangeBufferNodeMetricsBuilder
{
    private final ConcurrentHashMap<Long, BufferNodeExchangeMetrics> bufferNodeMetrics = new ConcurrentHashMap<>();

    public synchronized void update(long bufferNodeId, BufferNodeExchangeMetrics metrics)
    {
        requireNonNull(metrics, "metrics is null");
        bufferNodeMetrics.put(bufferNodeId, metrics);
    }

    public synchronized Metrics buildMetrics()
    {
        // Create fresh builder to avoid accumulating metrics across calls
        MetricsBuilder metricsBuilder = new MetricsBuilder();

        // Buffer node aggregated metrics
        CounterMetricBuilder totalChunksMetric = metricsBuilder.getCounterMetric("BufferExchange.totalChunks");
        CounterMetricBuilder chunksInMemoryMetric = metricsBuilder.getCounterMetric("BufferExchange.chunksInMemory");
        CounterMetricBuilder chunksSpooledMetric = metricsBuilder.getCounterMetric("BufferExchange.chunksSpooled");
        CounterMetricBuilder totalBytesMetric = metricsBuilder.getCounterMetric("BufferExchange.totalBytes");
        CounterMetricBuilder bytesInMemoryMetric = metricsBuilder.getCounterMetric("BufferExchange.bytesInMemory");
        CounterMetricBuilder bytesSpooledMetric = metricsBuilder.getCounterMetric("BufferExchange.bytesSpooled");

        // Distribution of data across buffer nodes
        DistributionMetricBuilder bytesPerBufferNodeMetric = metricsBuilder.getDistributionMetric("BufferExchange.bytesPerBufferNode");
        DistributionMetricBuilder chunksPerBufferNodeMetric = metricsBuilder.getDistributionMetric("BufferExchange.chunksPerBufferNode");
        DistributionMetricBuilder partitionsPerBufferNodeMetric = metricsBuilder.getDistributionMetric("BufferExchange.partitionsPerBufferNode");

        for (BufferNodeExchangeMetrics nodeMetrics : bufferNodeMetrics.values()) {
            // Aggregate chunk counts
            totalChunksMetric.add(nodeMetrics.totalChunks());
            chunksInMemoryMetric.add(nodeMetrics.chunksInMemory());
            chunksSpooledMetric.add(nodeMetrics.chunksSpooled());

            // Aggregate byte counts
            totalBytesMetric.add(nodeMetrics.totalBytes());
            bytesInMemoryMetric.add(nodeMetrics.bytesInMemory());
            bytesSpooledMetric.add(nodeMetrics.bytesSpooled());

            // Record per-node distribution metrics
            bytesPerBufferNodeMetric.add(nodeMetrics.totalBytes());
            chunksPerBufferNodeMetric.add(nodeMetrics.totalChunks());
            partitionsPerBufferNodeMetric.add(nodeMetrics.partitionCount());
        }

        return metricsBuilder.buildMetrics();
    }
}
