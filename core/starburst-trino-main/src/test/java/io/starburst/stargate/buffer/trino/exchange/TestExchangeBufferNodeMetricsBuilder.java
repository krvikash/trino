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
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExchangeBufferNodeMetricsBuilder
{
    @Test
    public void testSpoolingMetricsAggregateAcrossBufferNodes()
    {
        ExchangeBufferNodeMetricsBuilder builder = new ExchangeBufferNodeMetricsBuilder();
        builder.update(1, new BufferNodeExchangeMetrics(1, 0, 0, 2, 40, 2, 40, 15, 25));
        builder.update(2, new BufferNodeExchangeMetrics(1, 0, 0, 1, 7, 1, 7, 7, 0));

        Metrics metrics = builder.buildMetrics();

        assertThat(metrics.getMetrics().get("BufferExchange.memorySpooledBytes")).isEqualTo(new LongCount(22));
        assertThat(metrics.getMetrics().get("BufferExchange.localDiskSpooledBytes")).isEqualTo(new LongCount(25));
    }
}
