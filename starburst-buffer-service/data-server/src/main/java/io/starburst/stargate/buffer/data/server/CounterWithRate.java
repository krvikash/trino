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
import io.airlift.stats.CounterStat;

import static java.util.Objects.requireNonNull;

public class CounterWithRate
{
    private static final double MINIMUM_UPDATE_INTERVAL_IN_SECS = 1.0;

    private final CounterStat counterStat;
    private final Ticker ticker;

    private long lastUpdateTimeNanos;
    private long lastCounter;
    private double rate;

    public CounterWithRate(CounterStat counterStat, Ticker ticker)
    {
        this.counterStat = requireNonNull(counterStat, "counterStat is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public void updateCounterStat(long count)
    {
        counterStat.update(count);
    }

    public void update()
    {
        update(ticker.read(), counterStat.getTotalCount());
    }

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

    public long getCounter()
    {
        return lastCounter;
    }

    public double getRate()
    {
        return rate;
    }

    public long getLastUpdateTimeMillis()
    {
        return lastUpdateTimeNanos / 1_000_000;
    }
}
