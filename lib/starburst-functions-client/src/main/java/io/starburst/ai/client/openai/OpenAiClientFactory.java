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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.openai.azure.AzureOpenAIServiceVersion;
import com.openai.azure.credential.AzureApiKeyCredential;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.credential.BearerTokenCredential;
import com.openai.models.ReasoningEffort;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.AiClientConfig;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.ForAiClient;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.ModelSecretsResolver.resolveOAuth2Secrets;
import static io.starburst.ai.client.ModelSecretsResolver.resolveOpenAiSecrets;
import static io.starburst.ai.client.tracing.GenAiAttributes.PROVIDER_NAME_AZURE_OPENAI;
import static io.starburst.ai.client.tracing.GenAiAttributes.PROVIDER_NAME_OPENAI;
import static io.starburst.ai.model.LlmTrait.STREAMING_TOOL_CALL_SUPPORT;
import static io.starburst.ai.model.StreamingToolCallSupportOption.STREAMING_TOOL_CALL_SUPPORTED;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class OpenAiClientFactory
        implements ModelClientFactory<OpenAiConnectionInfo>
{
    private static final Logger LOG = Logger.get(OpenAiClientFactory.class);
    // It is assumed that the Azure OpenAI endpoint is of the following formats:
    // {baseUrl}/openai/deployments/{deploymentName}/chat/completions?api-version={version}
    // {baseUrl}/openai/deployments/{deploymentName}/embeddings?api-version={version}
    // {baseUrl}/openai/responses?api-version={version}
    private static final String AZURE_OPENAI_ENDPOINT_IDENTIFIER = "/openai/deployments/";
    private static final String AZURE_OPENAI_RESPONSES_ENDPOINT_PATH = "/openai/responses";
    private static final String AZURE_OPENAI_API_VERSION_QUERY_PARAM = "api-version";
    // Copied from https://github.com/openai/openai-java/blob/71cf8abd87f4e7ea4ab658d813499f3e30aee632/openai-java-core/src/main/kotlin/com/openai/core/Utils.kt#L98-L100
    private static final Set<String> AZURE_HOST_SUFFIXES = ImmutableSet.of(".openai.azure.com", ".azure-api.net", ".cognitiveservices.azure.com");

    private final SecretsResolver secretsResolver;
    private final Executor executor;
    private final int batchParallelism;
    private final int maxRetries;
    private final Duration timeout;
    private final ObjectMapper objectMapper;
    private final OAuth2TokenCache oauth2TokenCache;
    private final Tracer tracer;
    private final boolean includeMessagesInTracing;

    @Inject
    public OpenAiClientFactory(
            SecretsResolver secretsResolver,
            AiClientConfig config,
            @ForAiClient Executor executor,
            ObjectMapper objectMapper,
            OAuth2TokenCache oauth2TokenCache,
            @ForAiClient Tracer tracer)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.executor = requireNonNull(executor, "executor is null");
        batchParallelism = config.getBatchParallelism();
        maxRetries = config.getOpenAiMaxRetries();
        timeout = config.getOpenAiTimeout();
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.oauth2TokenCache = requireNonNull(oauth2TokenCache, "oauth2TokenCache is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.includeMessagesInTracing = config.isTracingIncludeMessagesEnabled();
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
        String modelName = azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::deployment)
                .filter(deployment -> !deployment.isBlank())
                .orElse(spec.modelName());
        boolean isStreamingToolCallSupported = spec.traits().getOrDefault(STREAMING_TOOL_CALL_SUPPORT, STREAMING_TOOL_CALL_SUPPORTED.name())
                .equals(STREAMING_TOOL_CALL_SUPPORTED.name());

        OpenAIClient openAiClient = createOpenAiClient(spec.id(), updatedConnectionInfo, azureOpenAiConnectionInfo);
        String providerName = providerName(azureOpenAiConnectionInfo);
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
                    tracer,
                    providerName,
                    includeMessagesInTracing,
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
                tracer,
                providerName,
                includeMessagesInTracing,
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
        String modelName = azureOpenAiConnectionInfo.map(AzureOpenAiConnectionInfo::deployment)
                .filter(deployment -> !deployment.isBlank())
                .orElse(spec.modelName());
        return new OpenAiEmbeddingModelClient(modelName, spec.dimensions(), createOpenAiClient(spec.id(), updatedConnectionInfo, azureOpenAiConnectionInfo));
    }

    private static String providerName(Optional<AzureOpenAiConnectionInfo> azureOpenAiConnectionInfo)
    {
        return azureOpenAiConnectionInfo.isPresent() ? PROVIDER_NAME_AZURE_OPENAI : PROVIDER_NAME_OPENAI;
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
            boolean oauthNotSupported = azureOpenAiConnectionInfo
                    .map(info -> !info.isResponsesApiEndpoint())
                    .orElse(false);
            if (oauthNotSupported) {
                throw new TrinoException(INVALID_MODEL_CONFIGURATION, "OAuth2 is not supported for Azure OpenAI deployment endpoints; use apiKey. OAuth2 is supported for the Azure Foundry Responses API endpoint shape.");
            }
            String bearer = oauth2TokenCache.accessToken(modelId, resolveOAuth2Secrets(connectionInfo.oauthConfig().get(), secretsResolver));
            // credential() sets the Authorization header; a header set via putAllHeaders() below would take
            // precedence at build() time, but additionalHeaders() can never contain "Authorization" here
            // (enforced by OpenAiConnectionInfo's compact constructor).
            builder.credential(BearerTokenCredential.create(bearer));
            if (!connectionInfo.additionalHeaders().isEmpty()) {
                builder.putAllHeaders(resolveOpenAiSecrets(connectionInfo, secretsResolver).additionalHeaders());
            }
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
                .flatMap(endpoint -> {
                    String defaultVersion = AzureOpenAIServiceVersion.latestPreviewVersion().value();
                    try {
                        URI uri = new URI(endpoint);
                        AzureOpenAiEndpointComponents azureOpenAiEndpointComponents;
                        boolean isCustomAzureOpenAiDeployment = false;
                        boolean isResponsesApiEndpoint = false;
                        if (isKnownAzureHost(uri)) {
                            if (isAzureResponsesApiPath(uri.getPath())) {
                                azureOpenAiEndpointComponents = getAzureResponsesEndpoint(endpoint);
                                isResponsesApiEndpoint = true;
                            }
                            else if (endpoint.contains(AZURE_OPENAI_ENDPOINT_IDENTIFIER)) {
                                azureOpenAiEndpointComponents = getOpenAiAzureEndpoint(endpoint);
                            }
                            else {
                                return Optional.empty();
                            }
                        }
                        else if (endpoint.contains(AZURE_OPENAI_ENDPOINT_IDENTIFIER)) {
                            azureOpenAiEndpointComponents = getCustomOpenAiAzureEndpoint(endpoint);
                            isCustomAzureOpenAiDeployment = true;
                        }
                        else {
                            return Optional.empty();
                        }

                        String deploymentName = azureOpenAiEndpointComponents.deployment;
                        String baseUrl = azureOpenAiEndpointComponents.baseUrl;
                        // Get api version
                        String query = uri.getQuery();
                        if (query == null || query.isEmpty()) {
                            LOG.debug("Using default Azure OpenAI API version: '%s'", defaultVersion);
                            return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deploymentName, defaultVersion, isCustomAzureOpenAiDeployment, isResponsesApiEndpoint));
                        }
                        Map<String, String> queryParams = Arrays.stream(query.split("&"))
                                .map(param -> param.split("=", 2))
                                .collect(toImmutableMap(a -> a[0], a -> a.length > 1 ? a[1] : ""));
                        if (!queryParams.containsKey(AZURE_OPENAI_API_VERSION_QUERY_PARAM)) {
                            LOG.debug("Using default Azure OpenAI API version: '%s'", defaultVersion);
                            return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deploymentName, defaultVersion, isCustomAzureOpenAiDeployment, isResponsesApiEndpoint));
                        }
                        return Optional.of(new AzureOpenAiConnectionInfo(baseUrl, deploymentName, queryParams.get(AZURE_OPENAI_API_VERSION_QUERY_PARAM), isCustomAzureOpenAiDeployment, isResponsesApiEndpoint));
                    }
                    catch (Exception e) {
                        throw new TrinoException(INVALID_MODEL_CONFIGURATION, e);
                    }
                });
    }

    private static boolean isKnownAzureHost(URI uri)
    {
        String host = uri.getHost();
        if (host == null) {
            return false;
        }
        return AZURE_HOST_SUFFIXES.stream().anyMatch(host::endsWith);
    }

    private static boolean isAzureResponsesApiPath(String path)
    {
        if (path == null) {
            return false;
        }
        String trimmedPath = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return trimmedPath.equals(AZURE_OPENAI_RESPONSES_ENDPOINT_PATH);
    }

    private static AzureOpenAiEndpointComponents getAzureResponsesEndpoint(String endpoint)
    {
        int pathStartIndex = endpoint.indexOf(AZURE_OPENAI_RESPONSES_ENDPOINT_PATH);
        String baseUrl = endpoint.substring(0, pathStartIndex);
        return new AzureOpenAiEndpointComponents(baseUrl, "");
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

    public record AzureOpenAiConnectionInfo(String endpoint, String deployment, String apiVersion, boolean isCustomAzureOpenAiDeployment, boolean isResponsesApiEndpoint)
    {
        public AzureOpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(deployment, "deployment is null");
            requireNonNull(apiVersion, "apiVersion is null");
        }
    }
}
