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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.stats.CounterStat;
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.openai.OpenAiClientFactory;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.client.openai.oauth.ResolvedOAuth2Config;
import io.starburst.ai.model.ConnectionInfo;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.starburst.ai.model.ModelConnectionSpec;
import io.starburst.ai.model.ModelConnectionSpecs;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import jakarta.annotation.PostConstruct;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.ai.client.ModelSecretsResolver.resolveConnectionInfo;
import static io.starburst.ai.client.ModelSecretsResolver.resolveOAuth2Secrets;
import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.VertexAiConnectionInfo;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class ReloadingModelClientProvider
        implements ModelClientProviderWithDao
{
    private static final Logger LOG = Logger.get(ReloadingModelClientProvider.class);
    private static final State EMPTY_STATE = new State(new ModelConnectionSpecs(ImmutableList.of()), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    private final PromptDao defaultPromptDao;
    private final ModelConnectionSpecsLoader modelSpecsLoader;
    private final AwsBedrockClientFactory awsBedrockClientFactory;
    private final OpenAiClientFactory openAiClientFactory;
    private final OAuth2TokenCache oauth2TokenCache;
    private final TokenUsageListener tokenUsageListener;

    private final ScheduledExecutorService reloadingExecutor;
    private final AtomicBoolean started = new AtomicBoolean();

    private final long clientTtlMillis;
    private final long clientCacheRefreshIntervalMillis;
    private final boolean clientCacheRefreshEnabled;
    private final AtomicReference<State> state = new AtomicReference<>(EMPTY_STATE);
    private final CounterStat refreshFailures = new CounterStat();
    private final CounterStat clientCreationFailures = new CounterStat();
    private final SecretsResolver secretsResolver;

    private record State(
            ModelConnectionSpecs modelConnectionSpecs,
            Map<Slice, LanguageModelClient> aiClientCache,
            Map<Slice, EmbeddingModelClient> embeddingClientCache,
            Map<Slice, Long> clientCreatedMillis)
    {
        public State
        {
            requireNonNull(modelConnectionSpecs, "modelConnectionSpecs is null");
            requireNonNull(aiClientCache, "aiClientCache is null");
            requireNonNull(embeddingClientCache, "embeddingClientCache is null");
            requireNonNull(clientCreatedMillis, "clientCreatedMillis is null");
        }
    }

    @Inject
    public ReloadingModelClientProvider(
            PromptDao defaultPromptDao,
            ModelConnectionSpecsLoader modelSpecsLoader,
            AwsBedrockClientFactory awsBedrockClientFactory,
            OpenAiClientFactory openAiClientFactory,
            OAuth2TokenCache oauth2TokenCache,
            AiClientConfig config,
            SecretsResolver secretsResolver,
            @ForAiClient ScheduledExecutorService reloadingExecutor,
            TokenUsageListener tokenUsageListener)
    {
        this.defaultPromptDao = requireNonNull(defaultPromptDao, "defaultPromptDao is null");
        this.modelSpecsLoader = requireNonNull(modelSpecsLoader, "modelSpecsLoader is null");
        this.awsBedrockClientFactory = requireNonNull(awsBedrockClientFactory, "awsBedrockClientFactory is null");
        this.openAiClientFactory = requireNonNull(openAiClientFactory, "openAiClientFactory is null");
        this.oauth2TokenCache = requireNonNull(oauth2TokenCache, "oauth2TokenCache is null");
        this.tokenUsageListener = requireNonNull(tokenUsageListener, "tokenUsageListener is null");
        this.clientTtlMillis = config.getClientCacheTtl().toMillis();
        this.clientCacheRefreshIntervalMillis = config.getClientCacheRefreshInterval().toMillis();
        this.clientCacheRefreshEnabled = config.isClientCacheRefreshEnabled();
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.reloadingExecutor = requireNonNull(reloadingExecutor, "reloadingExecutor is null");
        load();
        if (!clientCacheRefreshEnabled && anyOAuth2Spec()) {
            throw new IllegalStateException("ai.client.cache.refresh.enabled must be true when any model uses oauthConfig");
        }
    }

    @PostConstruct
    public void start()
    {
        if (clientCacheRefreshEnabled && started.compareAndSet(false, true)) {
            reloadingExecutor.scheduleWithFixedDelay(this::load, clientCacheRefreshIntervalMillis, clientCacheRefreshIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public LanguageModelClient languageModelClient(Slice modelId)
    {
        LanguageModelClient client = state.get().aiClientCache().get(modelId);
        if (client == null) {
            throw new TrinoException(NOT_FOUND, "Language model client not found for id: %s".formatted(modelId.toStringUtf8()));
        }
        return client;
    }

    @Override
    public EmbeddingModelClient embeddingModelClient(Slice modelId)
    {
        EmbeddingModelClient client = state.get().embeddingClientCache().get(modelId);
        if (client == null) {
            throw new TrinoException(NOT_FOUND, "Embedding model client not found for id: %s".formatted(modelId.toStringUtf8()));
        }
        return client;
    }

    @Override
    public Collection<LanguageModelConnectionSpec> languageModelConnectionSpecs()
    {
        return state.get().modelConnectionSpecs().languageModelConnectionSpecs();
    }

    @Override
    public Collection<EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs()
    {
        return state.get().modelConnectionSpecs().embeddingModelConnectionSpecs();
    }

    @Override
    public LanguageModelConnectionSpec getLanguageModelConnectionSpecById(String id)
    {
        return state.get().modelConnectionSpecs().getLanguageModelConnectionSpecById(id)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Language model connection spec not found for id: %s".formatted(id)));
    }

    @Override
    public EmbeddingModelConnectionSpec getEmbeddingModelConnectionSpecById(String id)
    {
        return state.get().modelConnectionSpecs().getEmbeddingModelConnectionSpecById(id)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Embedding model connection spec not found for id: %s".formatted(id)));
    }

    @Managed
    @Nested
    public CounterStat getRefreshFailures()
    {
        return refreshFailures;
    }

    @Managed
    @Nested
    public CounterStat getClientCreationFailures()
    {
        return clientCreationFailures;
    }

    private synchronized void load()
    {
        try {
            new Loader().load();
            LOG.debug(
                    "Loaded model clients: %s language models, %s embedding models",
                    state.get().aiClientCache().size(),
                    state.get().embeddingClientCache().size());
        }
        catch (Throwable e) {
            LOG.warn(e, "Error refreshing model clients");
            refreshFailures.update(1);
        }
    }

    private class Loader
    {
        private final State currentState;
        private final ModelConnectionSpecs newModelConnectionSpecDao;
        private final long nowMillis = System.currentTimeMillis();
        private final ImmutableMap.Builder<Slice, LanguageModelClient> languageModelClientBuilder = ImmutableMap.builder();
        private final ImmutableMap.Builder<Slice, EmbeddingModelClient> embeddingModelClientBuilder = ImmutableMap.builder();
        private final ImmutableMap.Builder<Slice, Long> clientCreatedMillisBuilder = ImmutableMap.builder();

        public Loader()
        {
            this.currentState = state.get();
            this.newModelConnectionSpecDao = modelSpecsLoader.load();
        }

        public void load()
        {
            // Avoid copying the map when there are no changes. This is the common case.
            if (noChangesForLanguageModelSpecs() && noChangesForEmbeddingModelSpecs()) {
                return;
            }
            newModelConnectionSpecDao.languageModelConnectionSpecs().forEach(this::processLanguageModelClient);
            newModelConnectionSpecDao.embeddingModelConnectionSpecs().forEach(this::processEmbeddingModelClient);
            state.set(new State(newModelConnectionSpecDao, languageModelClientBuilder.buildOrThrow(), embeddingModelClientBuilder.buildOrThrow(), clientCreatedMillisBuilder.buildOrThrow()));
            oauth2TokenCache.retainKeys(collectCurrentOAuth2Configs());
        }

        private boolean noChangesForLanguageModelSpecs()
        {
            Collection<LanguageModelConnectionSpec> newLanguageModelConnectionSpecs = newModelConnectionSpecDao.languageModelConnectionSpecs();
            if (newLanguageModelConnectionSpecs.size() != currentSpecs().languageModelConnectionSpecs().size()) {
                return false;
            }
            return newLanguageModelConnectionSpecs.stream()
                    .allMatch(newModelConnectionSpec -> {
                        Slice id = Slices.utf8Slice(newModelConnectionSpec.id());
                        return currentSpecs().getLanguageModelConnectionSpecById(newModelConnectionSpec.id())
                                .map(spec -> spec.equals(newModelConnectionSpec) &&
                                        resolveConnectionInfo(spec.connectionInfo(), secretsResolver).equals(resolveConnectionInfo(newModelConnectionSpec.connectionInfo(), secretsResolver)) &&
                                        !isExpired(id) &&
                                        !oauth2TokenExpired(newModelConnectionSpec.id(), newModelConnectionSpec.connectionInfo()))
                                .orElse(false);
                    });
        }

        private boolean noChangesForEmbeddingModelSpecs()
        {
            Collection<EmbeddingModelConnectionSpec> newEmbeddingModelConnectionSpecs = newModelConnectionSpecDao.embeddingModelConnectionSpecs();
            if (newEmbeddingModelConnectionSpecs.size() != currentSpecs().embeddingModelConnectionSpecs().size()) {
                return false;
            }
            return newEmbeddingModelConnectionSpecs.stream()
                    .allMatch(newModelConnectionSpec -> {
                        Slice id = Slices.utf8Slice(newModelConnectionSpec.id());
                        return currentSpecs().getEmbeddingModelConnectionSpecById(newModelConnectionSpec.id())
                                .map(spec -> spec.equals(newModelConnectionSpec) &&
                                        resolveConnectionInfo(spec.connectionInfo(), secretsResolver).equals(resolveConnectionInfo(newModelConnectionSpec.connectionInfo(), secretsResolver)) &&
                                        !isExpired(id) &&
                                        !oauth2TokenExpired(newModelConnectionSpec.id(), newModelConnectionSpec.connectionInfo()))
                                .orElse(false);
                    });
        }

        private void processLanguageModelClient(LanguageModelConnectionSpec newModelConnectionSpec)
        {
            Slice id = Slices.utf8Slice(newModelConnectionSpec.id());
            if (currentSpecs().getLanguageModelConnectionSpecById(newModelConnectionSpec.id()).map(spec -> !spec.equals(newModelConnectionSpec)).orElse(false) ||
                    isExpired(id) ||
                    oauth2TokenExpired(newModelConnectionSpec.id(), newModelConnectionSpec.connectionInfo())) {
                try {
                    languageModelClientBuilder.put(id, createLanguageModelClient(newModelConnectionSpec));
                    clientCreatedMillisBuilder.put(id, nowMillis);
                }
                catch (Throwable t) {
                    // Do not throw, any failure in this thread will kill the scheduled refresh
                    LOG.warn(t, "Error creating language model client for id: %s", newModelConnectionSpec.id());
                    clientCreationFailures.update(1);
                }
            }
            else {
                languageModelClientBuilder.put(id, currentState.aiClientCache().get(id));
                clientCreatedMillisBuilder.put(id, currentState.clientCreatedMillis().get(id));
            }
        }

        private void processEmbeddingModelClient(EmbeddingModelConnectionSpec newModelConnectionSpec)
        {
            Slice id = Slices.utf8Slice(newModelConnectionSpec.id());
            if (currentSpecs().getEmbeddingModelConnectionSpecById(newModelConnectionSpec.id()).map(spec -> !spec.equals(newModelConnectionSpec)).orElse(false) ||
                    isExpired(id) ||
                    oauth2TokenExpired(newModelConnectionSpec.id(), newModelConnectionSpec.connectionInfo())) {
                try {
                    embeddingModelClientBuilder.put(id, createEmbeddingModelClient(newModelConnectionSpec));
                    clientCreatedMillisBuilder.put(id, nowMillis);
                }
                catch (Throwable t) {
                    // Do not throw, any failure in this thread will kill the scheduled refresh
                    LOG.warn(t, "Error creating embedding model client for id: %s", newModelConnectionSpec.id());
                    clientCreationFailures.update(1);
                }
            }
            else {
                embeddingModelClientBuilder.put(id, currentState.embeddingClientCache().get(id));
                clientCreatedMillisBuilder.put(id, currentState.clientCreatedMillis().get(id));
            }
        }

        private boolean oauth2TokenExpired(String modelId, ConnectionInfo info)
        {
            if (info instanceof OpenAiConnectionInfo openAi && openAi.oauthConfig().isPresent()) {
                return oauth2TokenCache.isExpired(modelId, resolveOAuth2Secrets(openAi.oauthConfig().get(), secretsResolver));
            }
            return false;
        }

        private Map<String, ResolvedOAuth2Config> collectCurrentOAuth2Configs()
        {
            return Stream.concat(
                            newModelConnectionSpecDao.languageModelConnectionSpecs().stream(),
                            newModelConnectionSpecDao.embeddingModelConnectionSpecs().stream())
                    .filter(spec -> spec.connectionInfo() instanceof OpenAiConnectionInfo openAi && openAi.oauthConfig().isPresent())
                    .collect(toImmutableMap(ModelConnectionSpec::id,
                            spec -> resolveOAuth2Secrets(((OpenAiConnectionInfo) spec.connectionInfo()).oauthConfig().get(), secretsResolver)));
        }

        private ModelConnectionSpecs currentSpecs()
        {
            return currentState.modelConnectionSpecs();
        }

        private boolean isExpired(Slice id)
        {
            return nowMillis - currentState.clientCreatedMillis().getOrDefault(id, 0L) > clientTtlMillis;
        }

        private LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec modelConnectionSpec)
        {
            PromptDao promptDao = new PromptDaoWithOverrides(defaultPromptDao, modelConnectionSpec.prompts());
            return switch (modelConnectionSpec.connectionInfo()) {
                case OpenAiConnectionInfo openAiConnectionInfo -> openAiClientFactory.createLanguageModelClient(modelConnectionSpec, openAiConnectionInfo, promptDao, tokenUsageListener);
                case AwsBedrockConnectionInfo awsBedrockConnectionInfo -> awsBedrockClientFactory.createLanguageModelClient(modelConnectionSpec, awsBedrockConnectionInfo, promptDao, tokenUsageListener);
                case VertexAiConnectionInfo _ -> throw new UnsupportedOperationException("Vertex AI not yet supported");
            };
        }

        private EmbeddingModelClient createEmbeddingModelClient(EmbeddingModelConnectionSpec modelConnectionSpec)
        {
            return switch (modelConnectionSpec.connectionInfo()) {
                case OpenAiConnectionInfo openAiConnectionInfo -> openAiClientFactory.createEmbeddingClient(modelConnectionSpec, openAiConnectionInfo);
                case AwsBedrockConnectionInfo awsBedrockConnectionInfo -> awsBedrockClientFactory.createEmbeddingClient(modelConnectionSpec, awsBedrockConnectionInfo);
                case VertexAiConnectionInfo _ -> throw new UnsupportedOperationException("Vertex AI not yet supported");
            };
        }
    }

    private boolean anyOAuth2Spec()
    {
        ModelConnectionSpecs specs = state.get().modelConnectionSpecs();
        return Stream.concat(
                        specs.languageModelConnectionSpecs().stream().map(LanguageModelConnectionSpec::connectionInfo),
                        specs.embeddingModelConnectionSpecs().stream().map(EmbeddingModelConnectionSpec::connectionInfo))
                .anyMatch(info -> info instanceof OpenAiConnectionInfo openAi && openAi.oauthConfig().isPresent());
    }
}
