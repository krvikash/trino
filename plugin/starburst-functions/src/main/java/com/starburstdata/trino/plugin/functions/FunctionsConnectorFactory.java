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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.AiModelAccessControl;

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
                new AiModule(context.getModelConnectionSpecsLoader()),
                new JsonModule(),
                new StorageModule(context.getTypeManager()),
                new ResolvingFileSystemModule(context.getOpenTelemetry()),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getCurrentNode().getVersion()));
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(AiModelAccessControl.class).toInstance(context.getAiModelAccessControl());
                },
                module);

        Injector injector = app
                .doNotInitializeLogging()
                .loadSecretsPlugins() // starburst-functions-client requires access to secrets.
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(Connector.class);
    }
}
