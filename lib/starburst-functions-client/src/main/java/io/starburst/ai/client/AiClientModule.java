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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.bedrock.AwsBedrockEmbeddingCodecsModule;
import io.starburst.ai.client.openai.OpenAiClientFactory;
import io.starburst.ai.client.openai.oauth.DisableRedirectsFilter;
import io.starburst.ai.client.openai.oauth.ForAiOAuth2;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.client.openai.oauth.OAuth2TokenFetcher;
import io.starburst.ai.client.openai.oauth.OAuthEndpointSsrfFilter;
import io.starburst.ai.client.vertexai.VertexAiClientFactory;
import io.starburst.stargate.security.OAuthServerUrlValidator;
import io.starburst.stargate.security.SsrfEgressOAuthServerUrlValidator;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AiClientModule
        extends AbstractConfigurationAwareModule
{
    private final ModelConnectionSpecsLoader externalModelConnectionSpecsLoader;
    private final Optional<String> tracingScope;

    public AiClientModule(ModelConnectionSpecsLoader externalModelConnectionSpecsLoader)
    {
        this(externalModelConnectionSpecsLoader, Optional.empty());
    }

    public AiClientModule(ModelConnectionSpecsLoader externalModelConnectionSpecsLoader, String tracingScope)
    {
        this(externalModelConnectionSpecsLoader, Optional.of(tracingScope));
    }

    private AiClientModule(ModelConnectionSpecsLoader externalModelConnectionSpecsLoader, Optional<String> tracingScope)
    {
        this.externalModelConnectionSpecsLoader = requireNonNull(externalModelConnectionSpecsLoader, "externalModelConnectionSpecsLoader is null");
        this.tracingScope = requireNonNull(tracingScope, "tracingScope is null");
    }

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AiClientConfig.class);
        binder.bind(Executor.class).annotatedWith(ForAiClient.class).to(Key.get(ExecutorService.class, ForAiClient.class));
        binder.install(new AwsBedrockEmbeddingCodecsModule());
        binder.bind(AwsBedrockClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(OpenAiClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(VertexAiClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(OAuth2TokenCache.class).in(Scopes.SINGLETON);
        binder.bind(PromptDao.class).to(StaticPromptDao.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TokenUsageListener.class).setDefault().toInstance(TokenUsageListener.NOOP);
        binder.bind(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ReloadingModelClientProvider.class).withGeneratedName();
        binder.bind(ModelClientProviderWithDao.class).to(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        AiClientConfig aiClientConfig = buildConfigObject(AiClientConfig.class);

        newOptionalBinder(binder, OAuth2TokenFetcher.class);
        // Token refresh rides on the client cache reload, hence the dependency on cache refresh
        if (aiClientConfig.isOauth2Enabled() && aiClientConfig.isClientCacheRefreshEnabled()) {
            install(new AiOAuth2EnabledModule());
        }
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

    @Provides
    @Singleton
    @ForAiClient
    public Tracer createAiClientTracer(OpenTelemetry openTelemetry, Tracer tracer)
    {
        return tracingScope
                .map(openTelemetry::getTracer)
                .orElse(tracer);
    }

    private static class AiOAuth2EnabledModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            // Token fetches are infrequent and tiny, so keep the idle footprint of this client to a single
            // selector thread.
            httpClientBinder(binder).bindHttpClient("oauth2", ForAiOAuth2.class)
                    .withConfigDefaults(config -> config
                            .setMinThreads(1)
                            .setSelectorCount(1))
                    // SSRF egress guard: block token requests that resolve to internal addresses.
                    .withFilter(OAuthEndpointSsrfFilter.class)
                    .withFilter(DisableRedirectsFilter.class);
            newOptionalBinder(binder, OAuthServerUrlValidator.class).setDefault().to(SsrfEgressOAuthServerUrlValidator.class).in(Scopes.SINGLETON);
            binder.bind(OAuth2TokenFetcher.class).in(Scopes.SINGLETON);
        }
    }
}
