/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import io.airlift.testing.TestingTicker;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCastleInsightsRateLimiter
{
    @Test
    public void testStartsWithFullBurst()
    {
        CastleInsightsRateLimiter rateLimiter = new CastleInsightsRateLimiter(2, 3, new TestingTicker());

        assertThat(acquireAll(rateLimiter)).isEqualTo(6);
    }

    @Test
    public void testSustainedRateAfterBurstIsSpent()
    {
        TestingTicker ticker = new TestingTicker();
        CastleInsightsRateLimiter rateLimiter = new CastleInsightsRateLimiter(10, 5, ticker);
        acquireAll(rateLimiter);

        // Budget accrues continuously: one result every 6 seconds at 10 per minute
        ticker.increment(5, SECONDS);
        assertThat(rateLimiter.tryAcquire()).isFalse();
        ticker.increment(1, SECONDS);
        assertThat(rateLimiter.tryAcquire()).isTrue();
        assertThat(rateLimiter.tryAcquire()).isFalse();

        ticker.increment(1, MINUTES);
        assertThat(acquireAll(rateLimiter)).isEqualTo(10);
    }

    @Test
    public void testIdleBudgetAccumulatesUpToBurst()
    {
        TestingTicker ticker = new TestingTicker();
        CastleInsightsRateLimiter rateLimiter = new CastleInsightsRateLimiter(10, 5, ticker);
        acquireAll(rateLimiter);

        ticker.increment(2, MINUTES);
        assertThat(acquireAll(rateLimiter)).isEqualTo(20);

        // A long quiet stretch saves up no more than the burst
        ticker.increment(1, HOURS);
        assertThat(acquireAll(rateLimiter)).isEqualTo(50);
    }

    @Test
    public void testRejectsNonPositiveArguments()
    {
        assertThatThrownBy(() -> new CastleInsightsRateLimiter(0, 1, new TestingTicker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPerMinute must be positive");
        assertThatThrownBy(() -> new CastleInsightsRateLimiter(1, 0, new TestingTicker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("burstFactor must be positive");
    }

    private static int acquireAll(CastleInsightsRateLimiter rateLimiter)
    {
        int acquired = 0;
        while (rateLimiter.tryAcquire()) {
            acquired++;
        }
        return acquired;
    }
}
