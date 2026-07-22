/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.vertexai;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genai.Client;
import com.google.genai.types.HttpOptions;
import com.google.genai.types.HttpRetryOptions;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.units.Duration;
import io.starburst.ai.client.AiClientConfig;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.ForAiClient;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.trino.spi.TrinoException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.ModelSecretsResolver.resolveVertexAiSecrets;
import static io.starburst.ai.model.ConnectionInfo.VertexAiConnectionInfo;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class VertexAiClientFactory
        implements ModelClientFactory<VertexAiConnectionInfo>
{
    private static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    private final SecretsResolver secretsResolver;
    private final Executor executor;
    private final int batchParallelism;
    private final Duration apiTimeout;
    private final int maxRetries;

    @Inject
    public VertexAiClientFactory(SecretsResolver secretsResolver, AiClientConfig config, @ForAiClient Executor executor)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.executor = requireNonNull(executor, "executor is null");
        batchParallelism = config.getBatchParallelism();
        apiTimeout = config.getVertexAiTimeout();
        maxRetries = config.getVertexAiMaxRetries();
    }

    @VisibleForTesting
    HttpOptions httpOptions()
    {
        return buildHttpOptions(apiTimeout, maxRetries);
    }

    @Override
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, VertexAiConnectionInfo connectionInfo, PromptDao promptDao, TokenUsageListener tokenUsageListener)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new VertexAiLanguageModelClient(
                spec.modelName(),
                spec.maxTokens(),
                spec.temperature(),
                spec.topP(),
                promptDao,
                executor,
                batchParallelism,
                createClient(connectionInfo),
                tokenUsageListener);
    }

    @Override
    public EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, VertexAiConnectionInfo connectionInfo)
    {
        // TODO [ENG-20860] Add support for embeddings
        throw new UnsupportedOperationException("Vertex AI embedding support is not implemented");
    }

    @VisibleForTesting
    Client createClient(VertexAiConnectionInfo connectionInfo)
    {
        VertexAiConnectionInfo resolved = resolveVertexAiSecrets(connectionInfo, secretsResolver);
        String serviceAccountKey = resolved.serviceAccountKey()
                .orElseThrow(() -> new TrinoException(INVALID_MODEL_CONFIGURATION, "Vertex AI requires a serviceAccountKey"));
        ServiceAccountCredentials serviceAccountCredentials;
        try {
            serviceAccountCredentials = ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(serviceAccountKey.getBytes(UTF_8)));
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_MODEL_CONFIGURATION, "Invalid Vertex AI service-account key", e);
        }
        GoogleCredentials credentials = serviceAccountCredentials.createScoped(ImmutableList.of(CLOUD_PLATFORM_SCOPE));
        String projectId = resolved.projectId()
                .or(() -> Optional.ofNullable(serviceAccountCredentials.getProjectId()))
                .orElseThrow(() -> new TrinoException(INVALID_MODEL_CONFIGURATION, "Vertex AI projectId is not set and cannot be derived from the service-account key"));
        return Client.builder()
                .vertexAI(true)
                .project(projectId)
                .location(resolved.location())
                .credentials(credentials)
                .httpOptions(buildHttpOptions(apiTimeout, maxRetries, flattenHeaders(resolved.additionalHeaders())))
                .build();
    }

    @VisibleForTesting
    static HttpOptions buildHttpOptions(Duration apiTimeout, int maxRetries)
    {
        return buildHttpOptions(apiTimeout, maxRetries, Map.of());
    }

    @VisibleForTesting
    static HttpOptions buildHttpOptions(Duration apiTimeout, int maxRetries, Map<String, String> headers)
    {
        HttpOptions.Builder builder = HttpOptions.builder()
                .timeout(toIntExact(apiTimeout.toMillis()))
                .retryOptions(HttpRetryOptions.builder()
                        // google-genai's `attempts` counts the initial call as attempt 1, so total attempts = retries + 1
                        .attempts(maxRetries + 1));
        if (!headers.isEmpty()) {
            builder.headers(headers);
        }
        return builder.build();
    }

    // google-genai's HttpOptions only supports single-valued headers, so multi-valued additionalHeaders are joined
    @VisibleForTesting
    static Map<String, String> flattenHeaders(Map<String, List<String>> headers)
    {
        ImmutableMap.Builder<String, String> flattened = ImmutableMap.builder();
        headers.forEach((name, values) -> flattened.put(name, String.join(", ", values)));
        return flattened.buildOrThrow();
    }
}
