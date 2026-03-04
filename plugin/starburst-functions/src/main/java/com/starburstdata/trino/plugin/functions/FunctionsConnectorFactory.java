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
import com.google.inject.Module;
import com.starburstdata.trino.plugin.functions.ai.AiModule;
import com.starburstdata.trino.plugin.functions.io.ResolvingFileSystemModule;
import com.starburstdata.trino.plugin.functions.io.StorageModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.hive.crypto.ParquetEncryptionModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class FunctionsConnectorFactory
        implements ConnectorFactory
{
    private final Module module;

    public FunctionsConnectorFactory()
    {
        this(EMPTY_MODULE);
    }

    public FunctionsConnectorFactory(Module module)
    {
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return "starburst_functions";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new AiModule(context.getModelConnectionSpecsLoader()),
                new JsonModule(),
                new StorageModule(),
                new ResolvingFileSystemModule(context.getOpenTelemetry()),
                new ConnectorContextModule(catalogName, context),
                new ParquetEncryptionModule(),
                module);

        Injector injector = app
                .doNotInitializeLogging()
                .loadSecretsPlugins() // starburst-functions-client requires access to secrets.
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(Connector.class);
    }
}
