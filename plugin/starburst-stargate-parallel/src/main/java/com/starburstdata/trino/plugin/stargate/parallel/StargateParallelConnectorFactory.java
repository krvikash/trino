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

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.stargate.EnableWrites;
import com.starburstdata.trino.plugin.stargate.StargateMetadataFactory;
import com.starburstdata.trino.plugin.stargate.StargateModule;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class StargateParallelConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final boolean enableWrites;

    public StargateParallelConnectorFactory(String name, boolean enableWrites)
    {
        this.name = requireNonNull(name, "name is null");
        this.enableWrites = enableWrites;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new ConnectorContextModule(catalogName, context),
                binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(enableWrites),
                binder -> binder.install(new ExtraCredentialsBasedIdentityCacheMappingModule()),
                binder -> newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(StargateMetadataFactory.class).in(Scopes.SINGLETON),
                new JdbcModule(),
                new StargateModule(),
                new StargateParallelModule());

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(JdbcConnector.class);
    }
}
