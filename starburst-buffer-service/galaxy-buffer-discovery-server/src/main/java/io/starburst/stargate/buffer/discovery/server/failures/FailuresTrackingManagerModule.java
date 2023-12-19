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
import com.google.inject.Binder;
import com.google.inject.Module;
import io.starburst.stargate.buffer.discovery.server.failures.FailuresTrackingManager.ForFailuresTrackingManager;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class FailuresTrackingManagerModule
        implements Module
{
    private final Ticker ticker;

    private FailuresTrackingManagerModule(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public static FailuresTrackingManagerModule withSystemTicker()
    {
        return new FailuresTrackingManagerModule(Ticker.systemTicker());
    }

    public static FailuresTrackingManagerModule withTicker(Ticker ticker)
    {
        return new FailuresTrackingManagerModule(ticker);
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(FailuresTrackingManager.class).in(SINGLETON);
        binder.bind(Ticker.class).annotatedWith(ForFailuresTrackingManager.class).toInstance(ticker);
        configBinder(binder).bindConfig(FailuresTrackingConfig.class);
    }
}
