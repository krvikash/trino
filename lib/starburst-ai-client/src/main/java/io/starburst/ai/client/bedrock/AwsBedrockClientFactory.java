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
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.ConnectionInfo.AwsBedrockConnectionInfo;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.EmbeddingModelConnectionSpec;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.LanguageModelConnectionSpec;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Map;

import static io.starburst.ai.client.AiClientErrorCode.UNSUPPORTED_MODEL;
import static java.util.Objects.requireNonNull;

public class AwsBedrockClientFactory
        implements ModelClientFactory<AwsBedrockConnectionInfo>
{
    private final Map<String, AwsEmbeddingCodec.Factory> awsEmbeddingCodecFactories;

    @Inject
    public AwsBedrockClientFactory(Map<String, AwsEmbeddingCodec.Factory> awsEmbeddingCodecFactories)
    {
        this.awsEmbeddingCodecFactories = requireNonNull(awsEmbeddingCodecFactories, "awsEmbeddingCodecFactories is null");
    }

    @Override
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, AwsBedrockConnectionInfo connectionInfo, PromptDao promptDao, Tracer tracer)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new AwsBedrockLanguageModelClient(
                spec.modelName(),
                spec.maxTokens(),
                spec.temperature(),
                spec.topP(),
                promptDao,
                tracer,
                createBedrockClient(connectionInfo));
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

    private static BedrockRuntimeClient createBedrockClient(AwsBedrockConnectionInfo connectionInfo)
    {
        BedrockRuntimeClientBuilder clientBuilder = BedrockRuntimeClient.builder();

        AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();
        if (connectionInfo.awsAccessKey().isPresent() && connectionInfo.awsSecretKey().isPresent()) {
            awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(connectionInfo.awsAccessKey().orElseThrow(), connectionInfo.awsSecretKey().orElseThrow()));
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
        clientBuilder.credentialsProvider(awsCredentialsProvider);
        connectionInfo.region().ifPresent(region ->
                clientBuilder.region(Region.of(region)));

        return clientBuilder.build();
    }
}
