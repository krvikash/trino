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
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.bedrock.AwsBedrockEmbeddingCodecsModule;
import io.starburst.ai.client.openai.OpenAiClientFactory;
import io.starburst.ai.client.openai.oauth.ForAiOAuth2;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.client.openai.oauth.OAuth2TokenFetcher;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
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
        httpClientBinder(binder).bindHttpClient("oauth2", ForAiOAuth2.class);
        binder.bind(OAuth2TokenFetcher.class).in(Scopes.SINGLETON);
        binder.bind(OAuth2TokenCache.class).in(Scopes.SINGLETON);
        binder.bind(PromptDao.class).to(StaticPromptDao.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TokenUsageListener.class).setDefault().toInstance(TokenUsageListener.NOOP);
        binder.bind(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ReloadingModelClientProvider.class).withGeneratedName();
        binder.bind(ModelClientProviderWithDao.class).to(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        AiClientConfig aiClientConfig = buildConfigObject(AiClientConfig.class);
        switch (aiClientConfig.getStorageType()) {
            case NONE -> binder.bind(ModelConnectionSpecsLoader.class).toInstance(ModelConnectionSpecsLoader.EMPTY_LOADER);
            case FILE -> install(new FileBackedModelSpecModule());
            case EXTERNAL -> {
                checkState(aiClientConfig.isClientCacheRefreshEnabled(), "Client cache refresh is not enabled");
                binder.bind(ModelConnectionSpecsLoader.class).toInstance(externalModelConnectionSpecsLoader);
            }
        }

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
