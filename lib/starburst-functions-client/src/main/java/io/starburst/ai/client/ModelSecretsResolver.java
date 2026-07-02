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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.starburst.ai.client.openai.oauth.ResolvedOAuth2Config;
import io.starburst.ai.model.ConnectionInfo;
import io.starburst.ai.model.ConnectionInfo.OAuth2Config;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.OAuth2Config.validateTokenUrl;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;

public final class ModelSecretsResolver
{
    public static final String DUMMY_API_KEY = "dummy";

    private ModelSecretsResolver() {}

    static ConnectionInfo resolveConnectionInfo(ConnectionInfo connectionInfo, SecretsResolver secretsResolver)
    {
        return switch (connectionInfo) {
            case AwsBedrockConnectionInfo awsBedrockConnectionInfo -> resolveBedrockSecrets(awsBedrockConnectionInfo, secretsResolver);
            case OpenAiConnectionInfo openAiConnectionInfo -> resolveOpenAiSecrets(openAiConnectionInfo, secretsResolver);
        };
    }

    public static AwsBedrockConnectionInfo resolveBedrockSecrets(AwsBedrockConnectionInfo connectionInfo, SecretsResolver secretsResolver)
    {
        if (connectionInfo.additionalHeaders().isEmpty() && (connectionInfo.awsAccessKey().isEmpty() || connectionInfo.awsSecretKey().isEmpty())) {
            return connectionInfo;
        }
        Optional<String> awsAccessKey = connectionInfo.awsAccessKey().map(accessKey -> secretsResolver
                .getResolvedConfiguration(ImmutableMap.of("awsAccessKey", accessKey)).get("awsAccessKey"));
        Optional<String> awsSecretKey = connectionInfo.awsSecretKey().map(secretKey -> secretsResolver
                .getResolvedConfiguration(ImmutableMap.of("awsSecretKey", secretKey)).get("awsSecretKey"));
        return new AwsBedrockConnectionInfo(
                awsAccessKey,
                awsSecretKey,
                connectionInfo.region(),
                connectionInfo.iamRole(),
                connectionInfo.isUseAnonymousCredentials(),
                connectionInfo.externalId(),
                connectionInfo.endpoint(),
                resolveSecretHeaderValues(connectionInfo.additionalHeaders(), secretsResolver));
    }

    public static OpenAiConnectionInfo resolveOpenAiSecrets(OpenAiConnectionInfo connectionInfo, SecretsResolver secretsResolver)
    {
        if (connectionInfo.oauthConfig().isPresent()) {
            return new OpenAiConnectionInfo(
                    connectionInfo.endpoint(),
                    Optional.empty(),
                    resolveSecretHeaderValues(connectionInfo.additionalHeaders(), secretsResolver),
                    connectionInfo.oauthConfig());
        }
        return connectionInfo.apiKey().map(key ->
                        new OpenAiConnectionInfo(
                                connectionInfo.endpoint(),
                                Optional.of(secretsResolver.getResolvedConfiguration(ImmutableMap.of("apiKey", key)).get("apiKey")),
                                resolveSecretHeaderValues(connectionInfo.additionalHeaders(), secretsResolver),
                                Optional.empty()))
                // Pass DUMMY_API_KEY as OpenAI sdk mandatorily requires an API key https://github.com/openai/openai-java/blob/71cf8abd87f4e7ea4ab658d813499f3e30aee632/openai-java-core/src/main/kotlin/com/openai/core/ClientOptions.kt#L297
                .orElseGet(() -> new OpenAiConnectionInfo(
                        connectionInfo.endpoint(),
                        Optional.of(DUMMY_API_KEY),
                        resolveSecretHeaderValues(connectionInfo.additionalHeaders(), secretsResolver),
                        Optional.empty()));
    }

    public static ResolvedOAuth2Config resolveOAuth2Secrets(OAuth2Config config, SecretsResolver secretsResolver)
    {
        String tokenUrl = resolveOne(secretsResolver, "tokenUrl", config.tokenUrl());
        validateTokenUrl(tokenUrl);
        String clientId = resolveOne(secretsResolver, "clientId", config.clientId());
        String clientSecret = resolveOne(secretsResolver, "clientSecret", config.clientSecret());
        Optional<String> scope = config.scope().map(value -> resolveOne(secretsResolver, "scope", value));
        Optional<String> audience = config.audience().map(value -> resolveOne(secretsResolver, "audience", value));
        return new ResolvedOAuth2Config(config.grantType(), tokenUrl, clientId, clientSecret, scope, audience);
    }

    private static String resolveOne(SecretsResolver secretsResolver, String key, String value)
    {
        return secretsResolver.getResolvedConfiguration(ImmutableMap.of(key, value)).getOrDefault(key, value);
    }

    private static Map<String, List<String>> resolveSecretHeaderValues(Map<String, List<String>> headers, SecretsResolver secretsResolver)
    {
        ImmutableMap.Builder<String, List<String>> resolvedHeaders = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            entry.getValue().forEach(value -> builder.add(secretsResolver.getResolvedConfiguration(ImmutableMap.of(entry.getKey(), value)).getOrDefault(entry.getKey(), value)));
            resolvedHeaders.put(entry.getKey(), builder.build());
        }
        return resolvedHeaders.buildOrThrow();
    }
}
