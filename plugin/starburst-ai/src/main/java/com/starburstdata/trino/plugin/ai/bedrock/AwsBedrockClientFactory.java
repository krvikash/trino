/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.trino.plugin.ai.bedrock;

import com.google.inject.Inject;
import com.starburstdata.trino.plugin.ai.ConnectionInfo.AwsBedrockConnectionInfo;
import com.starburstdata.trino.plugin.ai.EmbeddingModelClient;
import com.starburstdata.trino.plugin.ai.EmbeddingModelConnectionSpec;
import com.starburstdata.trino.plugin.ai.LanguageModelClient;
import com.starburstdata.trino.plugin.ai.LanguageModelConnectionSpec;
import com.starburstdata.trino.plugin.ai.ModelClientFactory;
import com.starburstdata.trino.plugin.ai.PromptProvider;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;

import static com.starburstdata.trino.plugin.ai.AiErrorCode.UNSUPPORTED_MODEL;
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
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, AwsBedrockConnectionInfo connectionInfo, PromptProvider promptProvider, Tracer tracer)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new AwsBedrockLanguageModelClient(
                spec.modelName(),
                spec.maxTokens(),
                spec.temperature(),
                spec.topP(),
                promptProvider,
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

        connectionInfo.iamRole().ifPresentOrElse(
                role -> clientBuilder.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(request -> request
                                .roleArn(role)
                                .externalId(connectionInfo.externalId().orElse(null)))
                        .asyncCredentialUpdateEnabled(true)
                        .build()),
                () -> connectionInfo.awsAccessKey().ifPresent(accessKey ->
                        connectionInfo.awsSecretKey().ifPresent(awsSecretKey ->
                                clientBuilder.credentialsProvider(
                                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, awsSecretKey))))));

        connectionInfo.region().ifPresent(region ->
                clientBuilder.region(Region.of(region)));

        return clientBuilder.build();
    }
}
