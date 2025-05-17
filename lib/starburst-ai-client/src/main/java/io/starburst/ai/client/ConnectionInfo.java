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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "provider")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConnectionInfo.OpenAiConnectionInfo.class, name = "OPENAI"),
        @JsonSubTypes.Type(value = ConnectionInfo.AwsBedrockConnectionInfo.class, name = "AWS_BEDROCK")
})
public sealed interface ConnectionInfo
        permits ConnectionInfo.OpenAiConnectionInfo, ConnectionInfo.AwsBedrockConnectionInfo
{
    ConnectionInfo resolvedConnectionInfo(SecretsResolver secretsResolver);

    record OpenAiConnectionInfo(Optional<String> endpoint, Optional<String> apiKey)
            implements ConnectionInfo
    {
        public OpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(apiKey, "apiKey is null");
        }

        @Override
        public OpenAiConnectionInfo resolvedConnectionInfo(SecretsResolver secretsResolver)
        {
            return apiKey().map(key -> new OpenAiConnectionInfo(endpoint(), Optional.of(secretsResolver.getResolvedConfiguration(ImmutableMap.of("apiKey", key)).get("apiKey"))))
                    .orElse(this);
        }
    }

    record AwsBedrockConnectionInfo(
            Optional<String> awsAccessKey,
            Optional<String> awsSecretKey,
            Optional<String> region,
            Optional<String> iamRole,
            Optional<String> externalId)
            implements ConnectionInfo
    {
        public AwsBedrockConnectionInfo
        {
            requireNonNull(awsAccessKey, "awsAccessKey is null");
            requireNonNull(awsSecretKey, "awsSecretKey is null");
            requireNonNull(region, "region is null");
            requireNonNull(iamRole, "iamRole is null");
            requireNonNull(externalId, "externalId is null");
        }

        @Override
        public AwsBedrockConnectionInfo resolvedConnectionInfo(SecretsResolver secretsResolver)
        {
            if (awsAccessKey().isEmpty() || awsSecretKey().isEmpty()) {
                return this;
            }
            Map<String, String> resolvedSecrets = secretsResolver.getResolvedConfiguration(ImmutableMap.of("awsAccessKey", awsAccessKey().get(), "awsSecretKey", awsSecretKey().get()));
            Optional<String> awsAccessKey = Optional.of(resolvedSecrets.get("awsAccessKey"));
            Optional<String> awsSecretKey = Optional.of(resolvedSecrets.get("awsSecretKey"));
            return new AwsBedrockConnectionInfo(awsAccessKey, awsSecretKey, region(), iamRole(), externalId());
        }
    }
}
