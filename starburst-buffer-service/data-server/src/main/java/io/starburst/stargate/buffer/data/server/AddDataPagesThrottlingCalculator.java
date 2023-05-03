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

import io.airlift.stats.DecayCounter;

import javax.inject.Inject;

import java.util.concurrent.TimeUnit;

public class AddDataPagesThrottlingCalculator
{
    private final DecayCounter decayCounter;

    @Inject
    public AddDataPagesThrottlingCalculator(DataServerConfig config)
    {
        this.decayCounter = new DecayCounter(1 / config.getThrottlingCounterDecayDuration().getValue(TimeUnit.SECONDS));
    }

    public void recordThrottlingEvent()
    {
        decayCounter.add(1);
    }

    public long getNextRequestDelayInMillis()
    {
        return (long) decayCounter.getCount() * 1000;
    }
}
