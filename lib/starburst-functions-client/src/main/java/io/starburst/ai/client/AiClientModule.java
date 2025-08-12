/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.ai.client.AiClientConfig.StorageType;
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.bedrock.AwsBedrockEmbeddingCodecsModule;
import io.starburst.ai.client.openai.OpenAiClientFactory;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AiClientModule
        extends AbstractConfigurationAwareModule
{
    private final ModelConnectionSpecsLoader externalModelConnectionSpecsLoader;

    public AiClientModule(ModelConnectionSpecsLoader externalModelConnectionSpecsLoader)
    {
        this.externalModelConnectionSpecsLoader = externalModelConnectionSpecsLoader;
    }

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AiClientConfig.class);
        binder.bind(Executor.class).annotatedWith(ForAiClient.class).to(Key.get(ExecutorService.class, ForAiClient.class));
        binder.install(new AwsBedrockEmbeddingCodecsModule());
        binder.bind(AwsBedrockClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(OpenAiClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(PromptDao.class).to(StaticPromptDao.class).in(Scopes.SINGLETON);
        binder.bind(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ReloadingModelClientProvider.class).withGeneratedName();
        binder.bind(ModelClientProviderWithDao.class).to(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        install(conditionalModule(AiClientConfig.class,
                        aiClientConfig -> aiClientConfig.getStorageType() == StorageType.NONE,
                        innerBinder -> innerBinder.bind(ModelConnectionSpecsLoader.class).toInstance(ModelConnectionSpecsLoader.EMPTY_LOADER)));
        install(conditionalModule(AiClientConfig.class,
                aiClientConfig -> aiClientConfig.getStorageType() == StorageType.FILE,
                new FileBackedModelSpecModule()));
        install(conditionalModule(AiClientConfig.class,
                aiClientConfig -> aiClientConfig.getStorageType() == StorageType.EXTERNAL,
                externalBinder -> {
                    AiClientConfig aiClientConfig = buildConfigObject(AiClientConfig.class);
                    checkState(aiClientConfig.isClientCacheRefreshEnabled(), "Client cache refresh is not enabled");
                    externalBinder.bind(ModelConnectionSpecsLoader.class).toInstance(externalModelConnectionSpecsLoader);
                }));

        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForAiClient.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForAiClient.class));
    }

    @Provides
    @Singleton
    public ModelClientProvider getModelClientProvider(ModelClientProviderWithDao modelClientProviderWithDao)
    {
        return modelClientProviderWithDao;
    }

    @Provides
    @Singleton
    public ModelConnectionSpecDao getModelConnectionSpecDao(ModelClientProviderWithDao modelClientProviderWithDao)
    {
        return modelClientProviderWithDao;
    }

    @Provides
    @Singleton
    @ForAiClient
    public ScheduledExecutorService getScheduledExecutorService()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
    }

    @Provides
    @Singleton
    @ForAiClient
    public ExecutorService getLlmInvokerExecutorService()
    {
        return newCachedThreadPool(daemonThreadsNamed("llm-invoker-executor-%s"));
    }
}
