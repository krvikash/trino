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
import com.openai.azure.AzureOpenAIServiceVersion;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.ReloadingModelClientProvider;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.ModelSecretsResolver.resolveOpenAiSecrets;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class OpenAiClientFactory
        implements ModelClientFactory<OpenAiConnectionInfo>
{
    private static final Logger LOG = Logger.get(ReloadingModelClientProvider.class);
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
        Optional<AzureOpenAiConnectionInfo> azureOpenAiConnectionInfo = tryExtractAzureOpenAiConnectionInfo(connectionInfo.endpoint());
        OpenAiConnectionInfo updatedConnectionInfo = azureOpenAiConnectionInfo
                .map(azureConnectionInfo -> new OpenAiConnectionInfo(Optional.of(azureConnectionInfo.endpoint()), connectionInfo.apiKey()))
                .orElse(connectionInfo);
        // Ideally model name should be correctly parsed and populated from UI
        String modelName = azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::deployment).orElse(spec.modelName());
        boolean isGeminiEndpoint = updatedConnectionInfo.endpoint()
                .map(endpoint -> endpoint.toLowerCase(ROOT).startsWith("https://generativelanguage.googleapis.com"))
                .orElse(false);
        return new OpenAiLanguageModelClient(
                modelName,
                spec.temperature(),
                spec.maxTokens(),
                spec.topP(),
                spec.useDeveloperForSystemRole(),
                promptDao,
                tracer,
                isGeminiEndpoint,
                createOpenAiClient(updatedConnectionInfo, azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::apiVersion)));
    }

    @Override
    public EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo)
    {
        requireNonNull(connectionInfo, "connectionInfo is null");
        Optional<AzureOpenAiConnectionInfo> azureOpenAiConnectionInfo = tryExtractAzureOpenAiConnectionInfo(connectionInfo.endpoint());
        OpenAiConnectionInfo updatedConnectionInfo = azureOpenAiConnectionInfo
                .map(azureConnectionInfo -> new OpenAiConnectionInfo(Optional.of(azureConnectionInfo.endpoint()), connectionInfo.apiKey()))
                .orElse(connectionInfo);
        String modelName = azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::deployment).orElse(spec.modelName());
        return new OpenAiEmbeddingModelClient(modelName, spec.dimensions(), createOpenAiClient(updatedConnectionInfo, azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::apiVersion)));
    }

    private OpenAIClient createOpenAiClient(OpenAiConnectionInfo connectionInfo, Optional<String> azureOpenAiApiVersion)
    {
        OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder();
        azureOpenAiApiVersion.ifPresent(apiVersion -> builder.azureServiceVersion(AzureOpenAIServiceVersion.fromString(apiVersion)));
        if (connectionInfo.apiKey().isPresent()) {
            OpenAiConnectionInfo resolvedConnectionInfo = resolveOpenAiSecrets(connectionInfo, secretsResolver);
            builder.apiKey(resolvedConnectionInfo.apiKey().orElseThrow());
        }
        connectionInfo.endpoint().ifPresent(builder::baseUrl);
        return builder.build();
    }

    private static Optional<AzureOpenAiConnectionInfo> tryExtractAzureOpenAiConnectionInfo(Optional<String> openAiEndpoint)
    {
        return openAiEndpoint
                .map(endpoint -> endpoint.toLowerCase(ROOT))
                // Copied from https://github.com/openai/openai-java/blob/71cf8abd87f4e7ea4ab658d813499f3e30aee632/openai-java-core/src/main/kotlin/com/openai/core/Utils.kt#L98-L100
                .filter(endpoint -> endpoint.contains(".openai.azure.com")
                        || endpoint.contains(".azure-api.net")
                        || endpoint.contains(".cognitiveservices.azure.com"))
                .flatMap(endpoint -> {
                    String defaultVersion = AzureOpenAIServiceVersion.latestPreviewVersion().value();
                    try {
                        URI uri = new URI(endpoint);
                        String baseUrl = uri.getScheme() + "://" + uri.getAuthority() + "/";

                        // Get deployment
                        String deployment;
                        String path = uri.getPath();
                        if (path.contains("/deployments/")) {
                            int startIndex = path.indexOf("/deployments/") + "/deployments/".length();
                            int endIndex = path.indexOf("/", startIndex);
                            if (endIndex == -1) {
                                throw new TrinoException(INVALID_MODEL_CONFIGURATION, "Invalid Azure OpenAI endpoint - missing deployment");
                            }
                            else {
                                deployment = path.substring(startIndex, endIndex);
                            }
                        }
                        else {
                            throw new TrinoException(INVALID_MODEL_CONFIGURATION, "Invalid Azure OpenAI endpoint - missing deployment");
                        }

                        // Get api version
                        String query = uri.getQuery();
                        if (query == null || query.isEmpty()) {
                            LOG.debug("Using default Azure OpenAI API version: '%s'", defaultVersion);
                            return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deployment, defaultVersion));
                        }
                        Map<String, String> queryParams = Arrays.stream(query.split("&"))
                                .map(param -> param.split("=", 2))
                                .collect(toImmutableMap(a -> a[0], a -> a.length > 1 ? a[1] : ""));
                        if (!queryParams.containsKey("api-version")) {
                            LOG.debug("Using default Azure OpenAI API version: '%s'", defaultVersion);
                            return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deployment, defaultVersion));
                        }
                        return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deployment, queryParams.get("api-version")));
                    }
                    catch (Exception e) {
                        throw new TrinoException(INVALID_MODEL_CONFIGURATION, e);
                    }
                });
    }

    public record AzureOpenAiConnectionInfo(String endpoint, String deployment, String apiVersion)
    {
        public AzureOpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(deployment, "deployment is null");
            requireNonNull(apiVersion, "apiVersion is null");
        }
    }
}
