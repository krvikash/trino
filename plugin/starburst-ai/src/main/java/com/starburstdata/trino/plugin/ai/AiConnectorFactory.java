/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.ai;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.AiModelAccessControl;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class AiConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "starburst_ai";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                new AiModule(),
                binder -> {
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(AiModelAccessControl.class).toInstance(context.getAiModelAccessControl());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .loadSecretsPlugins() // starburst-ai-client requires access to secrets.
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(Connector.class);
    }
}
