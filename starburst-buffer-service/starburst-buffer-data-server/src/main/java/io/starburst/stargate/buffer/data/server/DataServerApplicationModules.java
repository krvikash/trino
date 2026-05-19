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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.Optional;

public final class DataServerApplicationModules
{
    private DataServerApplicationModules() {}

    public static Module getDataServerApplicationModule()
    {
        return getDataServerApplicationModule(Optional.empty(), true, false);
    }

    public static Module getDataServerApplicationModules(String configPrefix, boolean trinoCollocated)
    {
        return getDataServerApplicationModule(Optional.of(configPrefix), !trinoCollocated, trinoCollocated);
    }

    public static Module getSpoolingConfigurationModule(String configPrefix)
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                install(new SpoolingStorageModule(Optional.of(configPrefix), true));
            }
        };
    }

    private static Module getDataServerApplicationModule(Optional<String> configPrefix, boolean bindStandaloneDiscoveryApiModule, boolean useStaticMemoryConfig)
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                DataServerMainModule.Builder dataServerMainModule = DataServerMainModule.builder();
                configPrefix.ifPresent(dataServerMainModule::withConfigPrefix);
                dataServerMainModule = dataServerMainModule.withUseStaticMemoryConfig(useStaticMemoryConfig);

                install(dataServerMainModule.build());
                install(new SpoolingStorageModule(configPrefix, false));
                if (bindStandaloneDiscoveryApiModule) {
                    install(new StandaloneDiscoveryApiModule());
                }
            }
        };
    }
}
