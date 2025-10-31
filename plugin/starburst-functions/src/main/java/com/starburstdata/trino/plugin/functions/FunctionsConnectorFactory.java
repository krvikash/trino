/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions;

import com.google.inject.Injector;
import com.starburstdata.trino.plugin.functions.ai.AiModule;
import com.starburstdata.trino.plugin.functions.crypto.CryptoModule;
import com.starburstdata.trino.plugin.functions.io.ResolvingFileSystemModule;
import com.starburstdata.trino.plugin.functions.io.StorageModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.hive.crypto.ParquetEncryptionModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class FunctionsConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "starburst_functions";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = createBootstrap(catalogName, config, context);

        Injector injector = app
                .initialize();

        return injector.getInstance(Connector.class);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = createBootstrap(catalogName, config, context);

        Set<ConfigPropertyMetadata> usedProperties = app
                .quiet()
                .skipErrorReporting()
                .configure();

        return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
    }

    private Bootstrap createBootstrap(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new AiModule(context.getModelConnectionSpecsLoader()),
                new CryptoModule(),
                new JsonModule(),
                new StorageModule(),
                new ResolvingFileSystemModule(context.getOpenTelemetry()),
                new ConnectorContextModule(catalogName, context),
                new ParquetEncryptionModule());

        return app
                .doNotInitializeLogging()
                .loadSecretsPlugins() // starburst-functions-client requires access to secrets.
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config);
    }
}
