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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.openai.azure.AzureOpenAIServiceVersion;
import com.openai.azure.credential.AzureApiKeyCredential;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ReasoningEffort;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.ai.client.AiClientConfig;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.ForAiClient;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.ModelSecretsResolver.DUMMY_API_KEY;
import static io.starburst.ai.client.ModelSecretsResolver.resolveOAuth2Secrets;
import static io.starburst.ai.client.ModelSecretsResolver.resolveOpenAiSecrets;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import static io.starburst.ai.model.LlmTrait.STREAMING_TOOL_CALL_SUPPORT;
import static io.starburst.ai.model.StreamingToolCallSupportOption.STREAMING_TOOL_CALL_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class OpenAiClientFactory
        implements ModelClientFactory<OpenAiConnectionInfo>
{
    private static final Logger LOG = Logger.get(OpenAiClientFactory.class);
    // It is assumed that the Azure OpenAI endpoint is of the following formats:
    // {baseUrl}/openai/deployments/{deploymentName}/chat/completions?api-version={version}
    // {baseUrl}/openai/deployments/{deploymentName}/embeddings?api-version={version}
    private static final String AZURE_OPENAI_ENDPOINT_IDENTIFIER = "/openai/deployments/";
    private static final String AZURE_OPENAI_API_VERSION_QUERY_PARAM = "api-version";

    private final SecretsResolver secretsResolver;
    private final Executor executor;
    private final int batchParallelism;
    private final int maxRetries;
    private final Duration timeout;
    private final ObjectMapper objectMapper;
    private final OAuth2TokenCache oauth2TokenCache;

    @Inject
    public OpenAiClientFactory(
            SecretsResolver secretsResolver,
            AiClientConfig config,
            @ForAiClient Executor executor,
            ObjectMapper objectMapper,
            OAuth2TokenCache oauth2TokenCache)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.executor = requireNonNull(executor, "executor is null");
        batchParallelism = config.getBatchParallelism();
        maxRetries = config.getOpenAiMaxRetries();
        timeout = config.getOpenAiTimeout();
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.oauth2TokenCache = requireNonNull(oauth2TokenCache, "oauth2TokenCache is null");
    }

    @Override
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo, PromptDao promptDao, TokenUsageListener tokenUsageListener)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        Optional<AzureOpenAiConnectionInfo> azureOpenAiConnectionInfo = tryExtractAzureOpenAiConnectionInfo(connectionInfo.endpoint());
        OpenAiConnectionInfo updatedConnectionInfo = azureOpenAiConnectionInfo
                .map(azureConnectionInfo -> new OpenAiConnectionInfo(Optional.of(azureConnectionInfo.endpoint()), connectionInfo.apiKey(), connectionInfo.additionalHeaders(), connectionInfo.oauthConfig()))
                .orElse(connectionInfo);
        // Ideally model name should be correctly parsed and populated from UI
        String modelName = azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::deployment).orElse(spec.modelName());
        boolean isStreamingToolCallSupported = spec.traits().getOrDefault(STREAMING_TOOL_CALL_SUPPORT, STREAMING_TOOL_CALL_SUPPORTED.name())
                .equals(STREAMING_TOOL_CALL_SUPPORTED.name());

        OpenAIClient openAiClient = createOpenAiClient(spec.id(), updatedConnectionInfo, azureOpenAiConnectionInfo);
        if (spec.useResponsesApi()) {
            return new OpenAiResponsesLanguageModelClient(
                    modelName,
                    connectionInfo.endpoint(),
                    spec.temperature(),
                    spec.maxTokens(),
                    spec.topP(),
                    spec.useDeveloperForSystemRole(),
                    promptDao,
                    objectMapper,
                    executor,
                    batchParallelism,
                    openAiClient,
                    isStreamingToolCallSupported,
                    spec.reasoningEffort().map(Enum::name).map(ReasoningEffort::of),
                    tokenUsageListener);
        }
        return new OpenAiLanguageModelClient(
                modelName,
                connectionInfo.endpoint(),
                spec.temperature(),
                spec.maxTokens(),
                spec.topP(),
                spec.useDeveloperForSystemRole(),
                promptDao,
                objectMapper,
                executor,
                batchParallelism,
                openAiClient,
                isStreamingToolCallSupported,
                tokenUsageListener);
    }

    @Override
    public EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo)
    {
        requireNonNull(connectionInfo, "connectionInfo is null");
        Optional<AzureOpenAiConnectionInfo> azureOpenAiConnectionInfo = tryExtractAzureOpenAiConnectionInfo(connectionInfo.endpoint());
        OpenAiConnectionInfo updatedConnectionInfo = azureOpenAiConnectionInfo
                .map(azureConnectionInfo -> new OpenAiConnectionInfo(Optional.of(azureConnectionInfo.endpoint()), connectionInfo.apiKey(), connectionInfo.additionalHeaders(), connectionInfo.oauthConfig()))
                .orElse(connectionInfo);
        String modelName = azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::deployment).orElse(spec.modelName());
        return new OpenAiEmbeddingModelClient(modelName, spec.dimensions(), createOpenAiClient(spec.id(), updatedConnectionInfo, azureOpenAiConnectionInfo));
    }

    private OpenAIClient createOpenAiClient(String modelId, OpenAiConnectionInfo connectionInfo, Optional<AzureOpenAiConnectionInfo> azureOpenAiConnectionInfo)
    {
        OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder();
        builder.maxRetries(maxRetries);
        builder.timeout(timeout.toJavaTime());
        azureOpenAiConnectionInfo.ifPresent(info -> {
            if (info.isCustomAzureOpenAiDeployment()) {
                builder.putQueryParam(AZURE_OPENAI_API_VERSION_QUERY_PARAM, info.apiVersion());
            }
            else {
                builder.azureServiceVersion(AzureOpenAIServiceVersion.fromString(info.apiVersion()));
            }
        });
        if (connectionInfo.oauthConfig().isPresent()) {
            if (azureOpenAiConnectionInfo.isPresent()) {
                throw new TrinoException(INVALID_MODEL_CONFIGURATION, "OAuth2 is not supported for Azure OpenAI endpoints; use apiKey");
            }
            String bearer = oauth2TokenCache.accessToken(modelId, resolveOAuth2Secrets(connectionInfo.oauthConfig().get(), secretsResolver));
            // OpenAI SDK requires a non-blank apiKey (see DUMMY_API_KEY reference in ModelSecretsResolver);
            // the real credential is delivered via the Authorization header below.
            builder.apiKey(DUMMY_API_KEY);
            Map<String, List<String>> resolvedHeaders = connectionInfo.additionalHeaders().isEmpty()
                    ? Map.of()
                    : resolveOpenAiSecrets(connectionInfo, secretsResolver).additionalHeaders();
            Map<String, List<String>> merged = new LinkedHashMap<>(resolvedHeaders);
            merged.put("Authorization", List.of(format("Bearer %s", bearer)));
            builder.putAllHeaders(merged);
            connectionInfo.endpoint().ifPresent(builder::baseUrl);
            return builder.build();
        }
        if (connectionInfo.apiKey().isPresent() || !connectionInfo.additionalHeaders().isEmpty()) {
            connectionInfo = resolveOpenAiSecrets(connectionInfo, secretsResolver);
        }
        if (connectionInfo.apiKey().isPresent()) {
            // Which header is set based on the input - https://github.com/openai/openai-java/blob/6f9c7834bb0b099530286e15c6e3ba5df0f779e4/openai-java-core/src/main/kotlin/com/openai/core/ClientOptions.kt#L484-L494
            if (azureOpenAiConnectionInfo.isPresent()) {
                builder.credential(AzureApiKeyCredential.create(connectionInfo.apiKey().get()));
            }
            else {
                builder.apiKey(connectionInfo.apiKey().get());
            }
        }
        if (!connectionInfo.additionalHeaders().isEmpty()) {
            builder.putAllHeaders(connectionInfo.additionalHeaders());
        }
        connectionInfo.endpoint().ifPresent(builder::baseUrl);
        return builder.build();
    }

    @VisibleForTesting
    static Optional<AzureOpenAiConnectionInfo> tryExtractAzureOpenAiConnectionInfo(Optional<String> openAiEndpoint)
    {
        return openAiEndpoint
                .map(endpoint -> endpoint.toLowerCase(ROOT))
                .filter(endpoint -> endpoint.contains(AZURE_OPENAI_ENDPOINT_IDENTIFIER))
                .flatMap(endpoint -> {
                    String defaultVersion = AzureOpenAIServiceVersion.latestPreviewVersion().value();
                    try {
                        URI uri = new URI(endpoint);
                        AzureOpenAiEndpointComponents azureOpenAiEndpointComponents;
                        boolean isCustomAzureOpenAiDeployment = false;
                        // Copied from https://github.com/openai/openai-java/blob/71cf8abd87f4e7ea4ab658d813499f3e30aee632/openai-java-core/src/main/kotlin/com/openai/core/Utils.kt#L98-L100
                        if (endpoint.contains(".openai.azure.com")
                                || endpoint.contains(".azure-api.net")
                                || endpoint.contains(".cognitiveservices.azure.com")) {
                            azureOpenAiEndpointComponents = getOpenAiAzureEndpoint(endpoint);
                        }
                        else {
                            azureOpenAiEndpointComponents = getCustomOpenAiAzureEndpoint(endpoint);
                            isCustomAzureOpenAiDeployment = true;
                        }

                        String deploymentName = azureOpenAiEndpointComponents.deployment;
                        String baseUrl = azureOpenAiEndpointComponents.baseUrl;
                        // Get api version
                        String query = uri.getQuery();
                        if (query == null || query.isEmpty()) {
                            LOG.debug("Using default Azure OpenAI API version: '%s'", defaultVersion);
                            return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deploymentName, defaultVersion, isCustomAzureOpenAiDeployment));
                        }
                        Map<String, String> queryParams = Arrays.stream(query.split("&"))
                                .map(param -> param.split("=", 2))
                                .collect(toImmutableMap(a -> a[0], a -> a.length > 1 ? a[1] : ""));
                        if (!queryParams.containsKey(AZURE_OPENAI_API_VERSION_QUERY_PARAM)) {
                            LOG.debug("Using default Azure OpenAI API version: '%s'", defaultVersion);
                            return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deploymentName, defaultVersion, isCustomAzureOpenAiDeployment));
                        }
                        return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deploymentName, queryParams.get(AZURE_OPENAI_API_VERSION_QUERY_PARAM), isCustomAzureOpenAiDeployment));
                    }
                    catch (Exception e) {
                        throw new TrinoException(INVALID_MODEL_CONFIGURATION, e);
                    }
                });
    }

    private static AzureOpenAiEndpointComponents getOpenAiAzureEndpoint(String endpoint)
    {
        int remainingPathStartIndex = endpoint.indexOf(AZURE_OPENAI_ENDPOINT_IDENTIFIER);
        String baseUrl = endpoint.substring(0, remainingPathStartIndex);
        String remainingPath = endpoint.substring(remainingPathStartIndex);

        String deploymentName;
        int deploymentStartIndex = AZURE_OPENAI_ENDPOINT_IDENTIFIER.length();
        int endIndex = remainingPath.indexOf("/", deploymentStartIndex);
        if (endIndex == -1) {
            throw new TrinoException(INVALID_MODEL_CONFIGURATION, "Invalid Azure OpenAI endpoint - missing deployment");
        }
        deploymentName = remainingPath.substring(deploymentStartIndex, endIndex);
        return new AzureOpenAiEndpointComponents(baseUrl, deploymentName);
    }

    private static AzureOpenAiEndpointComponents getCustomOpenAiAzureEndpoint(String endpoint)
    {
        int remainingLlmPathStartIndex = endpoint.indexOf("/chat/completions");
        int remainingEmbeddingPathStartIndex = endpoint.indexOf("/embeddings");
        if (remainingLlmPathStartIndex == -1 && remainingEmbeddingPathStartIndex == -1) {
            throw new TrinoException(INVALID_MODEL_CONFIGURATION, "Invalid Azure OpenAI endpoint - missing /chat/completions OR /embeddings in the endpoint");
        }
        String baseUrl = endpoint.substring(0, remainingLlmPathStartIndex != -1 ? remainingLlmPathStartIndex : remainingEmbeddingPathStartIndex);
        // deployment is already part of baseUrl
        return new AzureOpenAiEndpointComponents(baseUrl, "");
    }

    private record AzureOpenAiEndpointComponents(String baseUrl, String deployment)
    {
        public AzureOpenAiEndpointComponents
        {
            requireNonNull(baseUrl, "baseUrl is null");
            requireNonNull(deployment, "deployment is null");
        }
    }

    public record AzureOpenAiConnectionInfo(String endpoint, String deployment, String apiVersion, boolean isCustomAzureOpenAiDeployment)
    {
        public AzureOpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(deployment, "deployment is null");
            requireNonNull(apiVersion, "apiVersion is null");
        }
    }
}
