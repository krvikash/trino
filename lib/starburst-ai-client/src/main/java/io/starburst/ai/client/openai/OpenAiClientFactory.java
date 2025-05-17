/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai;

import com.google.inject.Inject;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.ConnectionInfo.OpenAiConnectionInfo;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.EmbeddingModelConnectionSpec;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.LanguageModelConnectionSpec;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;

import static java.util.Objects.requireNonNull;

public class OpenAiClientFactory
        implements ModelClientFactory<OpenAiConnectionInfo>
{
    private final SecretsResolver secretsResolver;

    @Inject
    public OpenAiClientFactory(SecretsResolver secretsResolver)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    @Override
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo, PromptDao promptDao, Tracer tracer)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new OpenAiLanguageModelClient(
                spec.modelName(),
                spec.temperature(),
                spec.maxTokens(),
                spec.topP(),
                spec.useDeveloperForSystemRole(),
                promptDao,
                tracer,
                createOpenAiClient(connectionInfo));
    }

    @Override
    public EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo)
    {
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new OpenAiEmbeddingModelClient(spec, createOpenAiClient(connectionInfo));
    }

    private OpenAIClient createOpenAiClient(OpenAiConnectionInfo connectionInfo)
    {
        OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder();
        if (connectionInfo.apiKey().isPresent()) {
            OpenAiConnectionInfo resolvedConnectionInfo = connectionInfo.resolvedConnectionInfo(secretsResolver);
            builder.apiKey(resolvedConnectionInfo.apiKey().orElseThrow());
        }
        connectionInfo.endpoint().ifPresent(builder::baseUrl);
        return builder.build();
    }
}
