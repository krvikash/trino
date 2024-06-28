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

import io.airlift.testing.TestingTicker;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRateMonitor
{
    private static final long BUFFER_NODE_ID_0 = 0L;
    private static final long BUFFER_NODE_ID_1 = 1L;

    @Test
    public void testDelayCalculation()
    {
        TestingTicker ticker = new TestingTicker();
        RateMonitor rateMonitor = new RateMonitor(ticker);

        // no rate limit information, then no delay
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_0, Optional.empty());
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(0L);
        assertThat(rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0)).isEqualTo(0);

        // #requests per second = 25, avg processing time = 10ms, then execution interval is Math.max(0, 1000 / 25 - 10) = 30ms
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_0, Optional.of(new RateLimitInfo(25, 10)));
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(30L);
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(70L);
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(110L);
        assertThat(rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0)).isEqualTo(3);

        // different buffer nodes' schedule should be independent
        // #requests per second = 10, avg processing time = 20ms, then execution interval is Math.max(0, 1000 / 10 - 20) = 80ms
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_1, Optional.of(new RateLimitInfo(10, 20)));
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_1)).isEqualTo(80L);
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_1)).isEqualTo(180L);
        assertThat(rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_1)).isEqualTo(2);

        // delay should reflect the passage of time
        ticker.increment(100, TimeUnit.MILLISECONDS);
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(50L);
        assertThat(rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0)).isEqualTo(2);

        // #requests per second = 50, avg processing time = 40ms, then execution interval is Math.max(0, 1000 / 50 - 40) = 0ms
        ticker.increment(20, TimeUnit.MILLISECONDS);
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_0, Optional.of(new RateLimitInfo(50, 40)));
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(70L);
        assertThat(rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0)).isEqualTo(110L);
        assertThat(rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0)).isEqualTo(4);
    }
}
