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

import com.google.common.base.Ticker;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.DecayCounter;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.toIntExact;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class FailuresTrackingManager
{
    public static final Logger log = Logger.get(FailuresTrackingManager.class);

    private final DecayCounter counter;

    @Inject
    public FailuresTrackingManager(@ForFailuresTrackingManager Ticker ticker, FailuresTrackingConfig config)
    {
        counter = new DecayCounter(1.0 / config.getFailuresCounterDecayDuration().getValue(TimeUnit.SECONDS), ticker);
    }

    public void registerFailure(Optional<String> exchangeId, String observingClient, String failureDetails)
    {
        log.warn("Buffer service failure registered: exchangeId=%s; observingClient=%s; failureDetails=%s", exchangeId, observingClient, failureDetails);
        counter.add(1);
    }

    int getRecentFailuresCount()
    {
        return toIntExact(Math.round(counter.getCount()));
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    @interface ForFailuresTrackingManager {}
}
