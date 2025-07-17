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

import com.google.common.collect.ImmutableMap;
import io.starburst.ai.model.ConnectionInfo;

import java.util.Map;
import java.util.Optional;

import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;

public final class ModelSecretsResolver
{
    private ModelSecretsResolver() {}

    static ConnectionInfo resolveConnectionInfo(ConnectionInfo connectionInfo, io.airlift.configuration.secrets.SecretsResolver secretsResolver)
    {
        return switch (connectionInfo) {
            case AwsBedrockConnectionInfo awsBedrockConnectionInfo -> resolveBedrockSecrets(awsBedrockConnectionInfo, secretsResolver);
            case OpenAiConnectionInfo openAiConnectionInfo -> resolveOpenAiSecrets(openAiConnectionInfo, secretsResolver);
        };
    }

    public static AwsBedrockConnectionInfo resolveBedrockSecrets(AwsBedrockConnectionInfo connectionInfo, io.airlift.configuration.secrets.SecretsResolver secretsResolver)
    {
        if (connectionInfo.awsAccessKey().isEmpty() || connectionInfo.awsSecretKey().isEmpty()) {
            return connectionInfo;
        }
        Map<String, String> resolvedSecrets = secretsResolver
                .getResolvedConfiguration(ImmutableMap.of("awsAccessKey", connectionInfo.awsAccessKey().get(), "awsSecretKey", connectionInfo.awsSecretKey().get()));
        Optional<String> awsAccessKey = Optional.of(resolvedSecrets.get("awsAccessKey"));
        Optional<String> awsSecretKey = Optional.of(resolvedSecrets.get("awsSecretKey"));
        return new AwsBedrockConnectionInfo(awsAccessKey, awsSecretKey, connectionInfo.region(), connectionInfo.iamRole(), connectionInfo.externalId());
    }

    public static OpenAiConnectionInfo resolveOpenAiSecrets(OpenAiConnectionInfo connectionInfo, io.airlift.configuration.secrets.SecretsResolver secretsResolver)
    {
        return connectionInfo.apiKey().map(key ->
                        new OpenAiConnectionInfo(connectionInfo.endpoint(),
                                Optional.of(secretsResolver.getResolvedConfiguration(ImmutableMap.of("apiKey", key)).get("apiKey"))))
            .orElse(connectionInfo);
    }
}
