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

import com.google.common.base.Ticker;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.LongMath.saturatedAdd;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Caps how many insight results a cluster sends, so a burst of completed queries cannot flood the
 * pipeline downstream. A token bucket: budget accrues at {@code maxPerMinute} and unused budget
 * accumulates up to {@code burstFactor} minutes' worth, so after a quiet stretch up to
 * {@code burstFactor * maxPerMinute} results go out at once, while sustained traffic is held to
 * {@code maxPerMinute}. The bucket starts full.
 */
@ThreadSafe
public class CastleInsightsRateLimiter
{
    // The budget is kept as the time it took to accrue, in whole nanoseconds, so it does not drift with rounding
    private final long nanosPerPermit;
    private final long maxBudgetNanos;
    private final Ticker ticker;

    @GuardedBy("this")
    private long budgetNanos;
    @GuardedBy("this")
    private long lastRefillNanos;

    public CastleInsightsRateLimiter(int maxPerMinute, int burstFactor, Ticker ticker)
    {
        checkArgument(maxPerMinute > 0, "maxPerMinute must be positive");
        checkArgument(burstFactor > 0, "burstFactor must be positive");
        this.nanosPerPermit = MINUTES.toNanos(1) / maxPerMinute;
        this.maxBudgetNanos = multiplyExact(nanosPerPermit, multiplyExact((long) maxPerMinute, burstFactor));
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.budgetNanos = maxBudgetNanos;
        this.lastRefillNanos = ticker.read();
    }

    /**
     * Takes one unit of budget, returning false when none is available.
     */
    public synchronized boolean tryAcquire()
    {
        long now = ticker.read();
        budgetNanos = min(maxBudgetNanos, saturatedAdd(budgetNanos, now - lastRefillNanos));
        lastRefillNanos = now;
        if (budgetNanos < nanosPerPermit) {
            return false;
        }
        budgetNanos -= nanosPerPermit;
        return true;
    }
}
