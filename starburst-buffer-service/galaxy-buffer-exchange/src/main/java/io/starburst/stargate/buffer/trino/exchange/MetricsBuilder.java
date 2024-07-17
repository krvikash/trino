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

import io.airlift.stats.TDigest;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MetricsBuilder
{
    private final ConcurrentMap<String, MetricBuilder> metricBuilders = new ConcurrentHashMap<>();

    public Metrics buildMetrics()
    {
        return new Metrics(
                metricBuilders.entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().build())));
    }

    private interface MetricBuilder
    {
        Metric<?> build();
    }

    public CounterMetricBuilder getCounterMetric(String key)
    {
        return (CounterMetricBuilder) metricBuilders.computeIfAbsent(key, _ -> new CounterMetricBuilder());
    }

    public DistributionMetricBuilder getDistributionMetric(String key)
    {
        return (DistributionMetricBuilder) metricBuilders.computeIfAbsent(key, _ -> new DistributionMetricBuilder());
    }

    public static class CounterMetricBuilder
            implements MetricBuilder
    {
        private final AtomicLong counter = new AtomicLong();

        public void increment()
        {
            counter.incrementAndGet();
        }

        public void add(long delta)
        {
            counter.addAndGet(delta);
        }

        @Override
        public Metric<?> build()
        {
            return new LongCount(counter.get());
        }
    }

    public static class DistributionMetricBuilder
            implements MetricBuilder
    {
        private final TDigest digest = new TDigest();

        public synchronized void add(double value)
        {
            digest.add(value);
        }

        @Override
        public synchronized Metric<?> build()
        {
            return new TDigestHistogram(TDigest.copyOf(digest));
        }
    }
}
