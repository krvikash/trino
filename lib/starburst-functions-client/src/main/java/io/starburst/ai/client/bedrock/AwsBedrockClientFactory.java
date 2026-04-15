/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.google.inject.Inject;
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
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeBaseClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.model.ModelErrorException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.AiClientErrorCode.UNSUPPORTED_MODEL;
import static io.starburst.ai.client.ModelSecretsResolver.resolveBedrockSecrets;
import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.LlmTrait.PROMPT_CACHING_SUPPORT;
import static io.starburst.ai.model.LlmTrait.STREAMING_TOOL_CALL_SUPPORT;
import static io.starburst.ai.model.PromptCachingSupportOption.PROMPT_CACHING_SUPPORTED;
import static io.starburst.ai.model.StreamingToolCallSupportOption.STREAMING_TOOL_CALL_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class AwsBedrockClientFactory
        implements ModelClientFactory<AwsBedrockConnectionInfo>
{
    private static final Logger log = Logger.get(AwsBedrockClientFactory.class);
    private final Map<String, AwsEmbeddingCodec.Factory> awsEmbeddingCodecFactories;
    private final SecretsResolver secretsResolver;
    private final Executor executor;
    private final int batchParallelism;
    private final Duration socketTimeout;
    private final Duration apiTimeout;
    private final int maxRetries;

    @Inject
    public AwsBedrockClientFactory(Map<String, AwsEmbeddingCodec.Factory> awsEmbeddingCodecFactories, SecretsResolver secretsResolver, AiClientConfig config, @ForAiClient Executor executor)
    {
        this.awsEmbeddingCodecFactories = requireNonNull(awsEmbeddingCodecFactories, "awsEmbeddingCodecFactories is null");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.executor = requireNonNull(executor, "executor is null");
        batchParallelism = config.getBatchParallelism();
        socketTimeout = config.getBedrockSocketTimeout();
        apiTimeout = config.getBedrockApiTimeout();
        maxRetries = config.getBedrockMaxRetries();
    }

    @Override
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, AwsBedrockConnectionInfo connectionInfo, PromptDao promptDao, TokenUsageListener tokenUsageListener)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        boolean isStreamingToolCallSupported = spec.traits().getOrDefault(STREAMING_TOOL_CALL_SUPPORT, STREAMING_TOOL_CALL_SUPPORTED.name())
                .equals(STREAMING_TOOL_CALL_SUPPORTED.name());
        boolean isPromptCachingSupported = PROMPT_CACHING_SUPPORTED.name().equals(spec.traits().get(PROMPT_CACHING_SUPPORT));
        return new AwsBedrockLanguageModelClient(
                spec.modelName(),
                connectionInfo.endpoint(),
                spec.maxTokens(),
                spec.temperature(),
                spec.topP(),
                promptDao,
                executor,
                batchParallelism,
                createBedrockClient(connectionInfo),
                createBedrockAsyncClient(connectionInfo),
                isStreamingToolCallSupported,
                isPromptCachingSupported,
                tokenUsageListener);
    }

    @Override
    public EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, AwsBedrockConnectionInfo connectionInfo)
    {
        requireNonNull(spec, "spec is null");
        return new AwsBedrockEmbeddingModelClient(resolveModelName(spec), createAwsEmbeddingCodec(spec), createBedrockClient(connectionInfo));
    }

    private static String resolveModelName(EmbeddingModelConnectionSpec spec)
    {
        return spec.inferenceProfile().orElse(spec.modelName());
    }

    private AwsEmbeddingCodec createAwsEmbeddingCodec(EmbeddingModelConnectionSpec spec)
    {
        AwsEmbeddingCodec.Factory factory = awsEmbeddingCodecFactories.get(spec.modelName());
        if (factory == null) {
            throw new TrinoException(UNSUPPORTED_MODEL, "Model not supported: %s ".formatted(spec.modelName()));
        }
        return factory.create(spec);
    }

    private BedrockRuntimeClient createBedrockClient(AwsBedrockConnectionInfo connectionInfo)
    {
        BedrockRuntimeClientBuilder clientBuilder = BedrockRuntimeClient.builder();
        populateClientBuilder(clientBuilder, connectionInfo);
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                .socketTimeout(socketTimeout.toJavaTime());
        clientBuilder.httpClientBuilder(httpClientBuilder);
        return clientBuilder.build();
    }

    private BedrockRuntimeAsyncClient createBedrockAsyncClient(AwsBedrockConnectionInfo connectionInfo)
    {
        BedrockRuntimeAsyncClientBuilder clientBuilder = BedrockRuntimeAsyncClient.builder();
        populateClientBuilder(clientBuilder, connectionInfo);
        NettyNioAsyncHttpClient.Builder nettyClient = NettyNioAsyncHttpClient.builder()
                .readTimeout(socketTimeout.toJavaTime());
        clientBuilder.httpClientBuilder(nettyClient);
        return clientBuilder.build();
    }

    private void populateClientBuilder(BedrockRuntimeBaseClientBuilder clientBuilder, AwsBedrockConnectionInfo connectionInfo)
    {
        clientBuilder.credentialsProvider(getCredentialsProvider(connectionInfo));
        connectionInfo.region().ifPresent(region ->
                clientBuilder.region(Region.of(region)));

        RetryCondition customRetryCondition = (context) -> {
            Throwable exception = context.exception();
            log.warn(exception, "Exception in Bedrock client, checking if should retry");
            // Retry on default retryable conditions
            // Note: this method is deprecated but there is currently no replacement in the RetryStrategy api
            if (RetryCondition.defaultRetryCondition().shouldRetry(context)) {
                return true;
            }
            // Retry on ModelErrorException with 424 status code
            if (exception instanceof ModelErrorException modelErrorException) {
                return modelErrorException.statusCode() == 424;
            }
            return false;
        };

        connectionInfo.endpoint().ifPresent(endpoint -> {
            if (!endpoint.trim().isEmpty()) {
                try {
                    clientBuilder.endpointOverride(new URI(endpoint));
                }
                catch (URISyntaxException e) {
                    throw new TrinoException(INVALID_MODEL_CONFIGURATION, e);
                }
            }
        });

        ClientOverrideConfiguration.Builder clientOverrideConfigurationBuilder = ClientOverrideConfiguration.builder()
                .retryPolicy(RetryPolicy.builder()
                        .numRetries(maxRetries)
                        .retryCondition(customRetryCondition)
                        .build())
                .apiCallTimeout(apiTimeout.toJavaTime());
        if (!connectionInfo.additionalHeaders().isEmpty()) {
            AwsBedrockConnectionInfo resolvedConnectionInfo = resolveBedrockSecrets(connectionInfo, secretsResolver);
            resolvedConnectionInfo.additionalHeaders().forEach(clientOverrideConfigurationBuilder::putHeader);
        }
        clientBuilder.overrideConfiguration(clientOverrideConfigurationBuilder.build());
    }

    private AwsCredentialsProvider getCredentialsProvider(AwsBedrockConnectionInfo connectionInfo)
    {
        if (connectionInfo.isUseAnonymousCredentials()) {
            return AnonymousCredentialsProvider.create();
        }
        AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();
        if (connectionInfo.awsAccessKey().isPresent() && connectionInfo.awsSecretKey().isPresent()) {
            AwsBedrockConnectionInfo resolvedConnectionInfo = resolveBedrockSecrets(connectionInfo, secretsResolver);
            awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(resolvedConnectionInfo.awsAccessKey().orElseThrow(), resolvedConnectionInfo.awsSecretKey().orElseThrow()));
        }
        if (connectionInfo.iamRole().isPresent()) {
            StsAssumeRoleCredentialsProvider.Builder assumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder();
            StsClientBuilder stsClient = StsClient.builder()
                    .credentialsProvider(awsCredentialsProvider);
            connectionInfo.region().ifPresent(region -> stsClient.region(Region.of(region)));

            AssumeRoleRequest.Builder assumeRoleRequest = AssumeRoleRequest.builder();
            assumeRoleRequest.roleSessionName("starburst-ai-session");
            assumeRoleRequest.roleArn(connectionInfo.iamRole().orElseThrow());
            connectionInfo.externalId().ifPresent(assumeRoleRequest::externalId);

            assumeRoleCredentialsProvider
                    .stsClient(stsClient.build())
                    .refreshRequest(assumeRoleRequest.build())
                    .asyncCredentialUpdateEnabled(true);
            awsCredentialsProvider = assumeRoleCredentialsProvider.build();
        }
        return awsCredentialsProvider;
    }
}
