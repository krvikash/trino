/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.stargate.AllowForSpoolingProtocol;
import com.starburstdata.trino.plugin.stargate.StargateAuthenticationModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.client.spooling.SegmentLoader;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class StargateParallelModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorPageSourceProvider.class)
                .setBinding()
                .to(StargateParallelPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class)
                .setBinding().to(StargateParallelSplitManager.class).in(SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SegmentLoader.class).toProvider(SegmentLoaderProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(Boolean.class, AllowForSpoolingProtocol.class))
                .setBinding()
                .toInstance(Boolean.TRUE);

        configBinder(binder).bindConfig(StargateParallelConfig.class);
        binder.bind(StargateClientFactory.class).in(Scopes.SINGLETON);
        install(new StargateAuthenticationModule());
    }
}
