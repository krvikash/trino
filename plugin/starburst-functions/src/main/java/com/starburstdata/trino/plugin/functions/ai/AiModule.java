/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.functions.FunctionsConnector;
import com.starburstdata.trino.plugin.functions.FunctionsMetadata;
import com.starburstdata.trino.plugin.functions.StarburstFunctions;
import com.starburstdata.trino.plugin.functions.ai.embedding.GenerateEmbeddingsTableFunction;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.ai.client.AiClientModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.List;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class AiModule
        extends AbstractConfigurationAwareModule
{
    private final ModelConnectionSpecsLoader externalModelConnectionSpecsLoader;

    public AiModule(ModelConnectionSpecsLoader externalModelConnectionSpecsLoader)
    {
        this.externalModelConnectionSpecsLoader = externalModelConnectionSpecsLoader;
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new AiClientModule(externalModelConnectionSpecsLoader));
        binder.bind(FunctionsConnector.class).in(Scopes.SINGLETON);
        binder.bind(FunctionsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(StarburstFunctions.class).in(Scopes.SINGLETON);

        binder.bind(Connector.class).to(FunctionsConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(FunctionsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(StarburstFunctions.class).in(Scopes.SINGLETON);

        var systemTableBinder = newSetBinder(binder, SystemTable.class);
        systemTableBinder.addBinding().to(LanguageModelSystemTable.class).in(Scopes.SINGLETON);
        systemTableBinder.addBinding().to(EmbeddingModelSystemTable.class).in(Scopes.SINGLETON);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().to(GenerateEmbeddingsTableFunction.class).in(Scopes.SINGLETON);
    }

    @Provides
    public static List<FunctionMetadata> getFunctionMetadata(StarburstFunctions functions)
    {
        return functions.getFunctions();
    }
}
