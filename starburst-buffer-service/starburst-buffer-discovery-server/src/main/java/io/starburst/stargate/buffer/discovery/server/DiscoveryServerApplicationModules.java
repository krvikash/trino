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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.buffer.discovery.server.failures.FailuresTrackingManagerModule;

import java.util.Optional;

import static com.google.common.base.Ticker.systemTicker;

public final class DiscoveryServerApplicationModules
{
    private DiscoveryServerApplicationModules() {}

    public static Module getDiscoveryServerApplicationModule()
    {
        return getDiscoveryServerApplicationModule(Optional.empty());
    }

    public static Module getDiscoveryServerApplicationModule(String configPrefix)
    {
        return getDiscoveryServerApplicationModule(Optional.of(configPrefix));
    }

    private static Module getDiscoveryServerApplicationModule(Optional<String> configPrefix)
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                DiscoveryManagerModule.Builder discoveryModule = DiscoveryManagerModule.builder().withTicker(systemTicker());
                configPrefix.ifPresent(discoveryModule::withConfigPrefix);
                install(discoveryModule.build());
                install(FailuresTrackingManagerModule.withSystemTicker());
                install(new DiscoveryServerMainModule());
            }
        };
    }
}
