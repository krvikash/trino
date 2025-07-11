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

public final class DataServerApplicationModules
{
    private DataServerApplicationModules() {}

    public static Module getDataServerApplicationModule()
    {
        return new AbstractConfigurationAwareModule() {
            @Override
            protected void setup(Binder binder)
            {
                install(new DiscoveryApiModule());
                install(new DataServerMainModule());
                install(new SpoolingStorageModule());
            }
        };
    }
}
