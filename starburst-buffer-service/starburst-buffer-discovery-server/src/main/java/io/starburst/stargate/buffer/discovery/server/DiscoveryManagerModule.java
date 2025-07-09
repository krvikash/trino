/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.starburst.stargate.buffer.discovery.server.DiscoveryManager.ForDiscoveryManager;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DiscoveryManagerModule
        implements Module
{
    private final Ticker ticker;

    private DiscoveryManagerModule(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public static DiscoveryManagerModule withSystemTicker()
    {
        return new DiscoveryManagerModule(Ticker.systemTicker());
    }

    public static DiscoveryManagerModule withTicker(Ticker ticker)
    {
        return new DiscoveryManagerModule(ticker);
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DiscoveryStats.class).in(SINGLETON);
        newExporter(binder).export(DiscoveryStats.class).withGeneratedName();
        binder.bind(DiscoveryManager.class).in(SINGLETON);
        binder.bind(Ticker.class).annotatedWith(ForDiscoveryManager.class).toInstance(ticker);
        configBinder(binder).bindConfig(DiscoveryManagerConfig.class);
    }
}
