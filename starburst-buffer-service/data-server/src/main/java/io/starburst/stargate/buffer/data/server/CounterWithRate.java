/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.base.Ticker;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CounterWithRate
{
    private static final double MINIMUM_UPDATE_INTERVAL_IN_SECS = 1.0;

    private final Ticker ticker;

    private final AtomicLong counter = new AtomicLong();
    @GuardedBy("this")
    private long lastUpdateTimeNanos;
    @GuardedBy("this")
    private long lastCounter;
    @GuardedBy("this")
    private double rate;

    public CounterWithRate(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public void add(long count)
    {
        counter.getAndAdd(count);
    }

    public synchronized double getRate()
    {
        update(ticker.read(), counter.get());
        return rate;
    }

    public synchronized long getLastUpdateTimeMillis()
    {
        return lastUpdateTimeNanos / 1_000_000;
    }

    @GuardedBy("this")
    private void update(long currentTimeNanos, long counter)
    {
        if (lastUpdateTimeNanos != 0) {
            double timeDelta = (currentTimeNanos - lastUpdateTimeNanos) / 1_000_000_000d;
            if (timeDelta < MINIMUM_UPDATE_INTERVAL_IN_SECS) {
                // too close, skip update
                return;
            }
            long counterDelta = counter - lastCounter;
            rate = (double) counterDelta / timeDelta;
        }
        lastUpdateTimeNanos = currentTimeNanos;
        lastCounter = counter;
    }
}
