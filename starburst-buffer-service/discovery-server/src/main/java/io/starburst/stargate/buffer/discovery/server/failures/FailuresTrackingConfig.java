/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server.failures;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

public class FailuresTrackingConfig
{
    private Duration failuresCounterDecayDuration = succinctDuration(5, MINUTES);

    public Duration getFailuresCounterDecayDuration()
    {
        return failuresCounterDecayDuration;
    }

    @Config("buffer.failures-tracking.failures-counter-decay-duration")
    public FailuresTrackingConfig setFailuresCounterDecayDuration(Duration failuresCounterDecayDuration)
    {
        this.failuresCounterDecayDuration = failuresCounterDecayDuration;
        return this;
    }
}
