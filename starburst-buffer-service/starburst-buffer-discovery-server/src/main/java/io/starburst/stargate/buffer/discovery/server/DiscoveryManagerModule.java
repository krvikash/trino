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

import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DiscoveryManagerModule
        implements Module
{
    private final Ticker ticker;
    private final Optional<String> configPrefix;

    private DiscoveryManagerModule(
            Ticker ticker,
            Optional<String> configPrefix)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.configPrefix = requireNonNull(configPrefix, "configPrefix is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DiscoveryStats.class).in(SINGLETON);
        newExporter(binder).export(DiscoveryStats.class).withGeneratedName();
        binder.bind(DiscoveryManager.class).in(SINGLETON);
        binder.bind(Ticker.class).annotatedWith(ForDiscoveryManager.class).toInstance(ticker);
        configBinder(binder).bindConfig(DiscoveryManagerConfig.class, configPrefix.orElse(null));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Ticker ticker = Ticker.systemTicker();
        private Optional<String> configPrefix = Optional.empty();

        public Builder withTicker(Ticker ticker)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            return this;
        }

        public Builder withConfigPrefix(String configPrefix)
        {
            requireNonNull(configPrefix, "configPrefix is null");
            this.configPrefix = Optional.of(configPrefix);
            return this;
        }

        public DiscoveryManagerModule build()
        {
            return new DiscoveryManagerModule(ticker, configPrefix);
        }
    }
}
