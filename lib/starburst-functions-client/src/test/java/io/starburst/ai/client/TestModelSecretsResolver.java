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
import io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import io.starburst.ai.model.ConnectionInfo.OAuth2Config;
import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;
import io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestModelSecretsResolver
{
    @Test
    void testBedrockSecretResolution()
    {
        AwsBedrockConnectionInfo awsBedrockConnectionInfo = new AwsBedrockConnectionInfo(
                Optional.of("${TESTING:ACCESS_KEY_SECRET}"),
                Optional.of("${TESTING:SECRET_KEY_SECRET}"),
                Optional.of("us-east-1"),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(
                        "Authorization", ImmutableList.of("${TESTING:AUTHORIZATION_TOKEN}", "${TESTING:BASIC}"),
                        "Custom-Header", ImmutableList.of("CustomValue"),
                        "Mixed-Sensitivity", ImmutableList.of("NotSensitive", "${TESTING:SENSITIVE_VALUE}")));

        SecretsResolver secretsResolver = new SecretsResolver(
                ImmutableMap.of("testing", new TestingSecretsProvider(ImmutableMap.of(
                        "ACCESS_KEY_SECRET", "MYACCESS",
                        "SECRET_KEY_SECRET", "MYSECRET",
                        "AUTHORIZATION_TOKEN", "bearer token123",
                        "BASIC", "Basic cGFzc3dvcmQ=",
                        "SENSITIVE_VALUE", "secret"))));

        AwsBedrockConnectionInfo resolvedConnectionInfo = ModelSecretsResolver.resolveBedrockSecrets(awsBedrockConnectionInfo, secretsResolver);
        assertThat(resolvedConnectionInfo.awsAccessKey()).contains("MYACCESS");
        assertThat(resolvedConnectionInfo.awsSecretKey()).contains("MYSECRET");
        assertThat(resolvedConnectionInfo.region()).contains("us-east-1");
        assertThat(resolvedConnectionInfo.additionalHeaders()).containsExactlyEntriesOf(ImmutableMap.of(
                "Authorization", ImmutableList.of("bearer token123", "Basic cGFzc3dvcmQ="),
                "Custom-Header", ImmutableList.of("CustomValue"),
                "Mixed-Sensitivity", ImmutableList.of("NotSensitive", "secret")));
    }

    @Test
    void testOpenAiSecretResolution()
    {
        OpenAiConnectionInfo openAiConnectionInfo = new OpenAiConnectionInfo(
                Optional.of("https://api.openai.com/v1"),
                Optional.of("${TESTING:API_KEY}"),
                ImmutableMap.of(
                        "Authorization", ImmutableList.of("${TESTING:AUTHORIZATION_TOKEN}", "${TESTING:BASIC}"),
                        "Custom-Header", ImmutableList.of("CustomValue"),
                        "Mixed-Sensitivity", ImmutableList.of("NotSensitive", "${TESTING:SENSITIVE_VALUE}")),
                Optional.empty());

        SecretsResolver secretsResolver = new SecretsResolver(
                ImmutableMap.of("testing", new TestingSecretsProvider(ImmutableMap.of(
                        "API_KEY", "MYAAPIKEY",
                        "AUTHORIZATION_TOKEN", "bearer token123",
                        "BASIC", "Basic cGFzc3dvcmQ=",
                        "SENSITIVE_VALUE", "secret"))));
        OpenAiConnectionInfo resolvedConnectionInfo = ModelSecretsResolver.resolveOpenAiSecrets(openAiConnectionInfo, secretsResolver);
        assertThat(resolvedConnectionInfo.apiKey()).contains("MYAAPIKEY");
        assertThat(resolvedConnectionInfo.additionalHeaders()).containsExactlyEntriesOf(ImmutableMap.of(
                "Authorization", ImmutableList.of("bearer token123", "Basic cGFzc3dvcmQ="),
                "Custom-Header", ImmutableList.of("CustomValue"),
                "Mixed-Sensitivity", ImmutableList.of("NotSensitive", "secret")));
    }

    @Test
    void testOAuth2SecretResolution()
    {
        OAuth2Config config = new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "https://idp.example/token",
                "static-client-id",
                "${TESTING:OAUTH_CLIENT_SECRET}",
                Optional.of("${TESTING:OAUTH_SCOPE}"),
                Optional.of("audience-1"));

        SecretsResolver secretsResolver = new SecretsResolver(
                ImmutableMap.of("testing", new TestingSecretsProvider(ImmutableMap.of(
                        "OAUTH_CLIENT_SECRET", "the-real-secret",
                        "OAUTH_SCOPE", "resolved-scope"))));

        ResolvedOAuth2Config resolved = ModelSecretsResolver.resolveOAuth2Secrets(config, secretsResolver);
        assertThat(resolved.tokenUrl()).isEqualTo("https://idp.example/token");
        assertThat(resolved.clientId()).isEqualTo("static-client-id");
        assertThat(resolved.clientSecret()).isEqualTo("the-real-secret");
        assertThat(resolved.scope()).contains("resolved-scope");
        assertThat(resolved.audience()).contains("audience-1");
        assertThat(resolved.grantType()).isEqualTo(OAuth2GrantType.CLIENT_CREDENTIALS);
    }
}
