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

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertEquals(0L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(0, rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0));

        // #requests per second = 25, avg processing time = 10ms, then execution interval is Math.max(0, 1000 / 25 - 10) = 30ms
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_0, Optional.of(new RateLimitInfo(25, 10)));
        assertEquals(30L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(70L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(110L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(3, rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0));

        // different buffer nodes' schedule should be independent
        // #requests per second = 10, avg processing time = 20ms, then execution interval is Math.max(0, 1000 / 10 - 20) = 80ms
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_1, Optional.of(new RateLimitInfo(10, 20)));
        assertEquals(80L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_1));
        assertEquals(180L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_1));
        assertEquals(2, rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_1));

        // delay should reflect the passage of time
        ticker.increment(100, TimeUnit.MILLISECONDS);
        assertEquals(50L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(2, rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0));

        // #requests per second = 50, avg processing time = 40ms, then execution interval is Math.max(0, 1000 / 50 - 40) = 0ms
        ticker.increment(20, TimeUnit.MILLISECONDS);
        rateMonitor.updateRateLimitInfo(BUFFER_NODE_ID_0, Optional.of(new RateLimitInfo(50, 40)));
        assertEquals(70L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(110L, rateMonitor.registerExecutionSchedule(BUFFER_NODE_ID_0));
        assertEquals(4, rateMonitor.getExecutionScheduleSize(BUFFER_NODE_ID_0));
    }
}
