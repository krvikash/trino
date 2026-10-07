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

import com.google.common.collect.ImmutableMap;
import com.openai.azure.AzureOpenAIServiceVersion;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.configuration.secrets.env.EnvironmentVariableSecretProvider;
import io.airlift.json.JsonMapperProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.ai.client.AiClientConfig;
import io.starburst.ai.client.StaticPromptDao;
import io.starburst.ai.client.TestingUtils;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.model.ConnectionInfo.OAuth2Config;
import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;
import io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.ai.client.openai.OpenAiClientFactory.tryExtractAzureOpenAiConnectionInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenAiClientFactory
{
    @Test
    void testAzureOpenAiUrlFormat()
    {
        assertThat(tryExtractAzureOpenAiConnectionInfo(Optional.of("https://api.openai.com/v1")))
                .isEmpty();
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/o4-mini-deployment/chat/completions?api-version=2025-01-01-preview")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://test-domain.openai.azure.com",
                                "o4-mini-deployment",
                                "2025-01-01-preview",
                                false,
                                false)));
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/text-embedding-3-small-deployment/embeddings?api-version=2023-05-15")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://test-domain.openai.azure.com",
                                "text-embedding-3-small-deployment",
                                "2023-05-15",
                                false,
                                false)));
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments")))
                .isEmpty();
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/gpt/v2/gpt-4o-mini-2023-07-18/chat/completions?api-version=2023-12-01-preview")))
                .isEmpty();
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/gpt/libsupport/openai/deployments/gpt-4o-2024-11-20/chat/completions?api-version=2024-12-01-preview")))
                .isEqualTo(Optional.of(new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                        "https://test.domain.com/gpt/libsupport/openai/deployments/gpt-4o-2024-11-20",
                        "",
                        "2024-12-01-preview",
                        true,
                        false)));
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/gpt/libsupport/openai/deployments/text-embedding-ada-002/embeddings?api-version=2024-10-15")))
                .isEqualTo(Optional.of(new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                        "https://test.domain.com/gpt/libsupport/openai/deployments/text-embedding-ada-002",
                        "",
                        "2024-10-15",
                        true,
                        false)));
        assertThatThrownBy(() -> tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/gpt/libsupport/openai/deployments/text-embedding-ada-002/incorrectPath?api-version=2024-10-15")))
                .hasMessageContaining("Invalid Azure OpenAI endpoint - missing /chat/completions OR /embeddings in the endpoint");
        assertThatThrownBy(() -> tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/")))
                .hasMessageContaining("Invalid Azure OpenAI endpoint - missing deployment");
    }

    @Test
    void testAzureOpenAiResponsesApiUrlFormat()
    {
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://sp-azure-openai-mid.openai.azure.com/openai/responses?api-version=2025-04-01-preview")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://sp-azure-openai-mid.openai.azure.com",
                                "",
                                "2025-04-01-preview",
                                false,
                                true)));
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://sp-azure-openai-mid.cognitiveservices.azure.com/openai/responses")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://sp-azure-openai-mid.cognitiveservices.azure.com",
                                "",
                                AzureOpenAIServiceVersion.latestPreviewVersion().value(),
                                false,
                                true)));
        // Trailing slash is tolerated.
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/responses/?api-version=2025-04-01-preview")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://test-domain.openai.azure.com",
                                "",
                                "2025-04-01-preview",
                                false,
                                true)));
        // Only known Azure hostnames are recognized for this shape; arbitrary/custom hosts are left alone.
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/openai/responses?api-version=2025-04-01-preview")))
                .isEmpty();
        // A non-terminal /openai/responses path segment on a known Azure host, with no /openai/deployments/
        // segment either, matches neither recognized shape.
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/responses/extra?api-version=2025-04-01-preview")))
                .isEmpty();
    }

    @Test
    void testAzureHostIsMatchedOnParsedHost()
    {
        // An Azure host in the userinfo does not make the real host (attacker.example) a known Azure host.
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com@attacker.example/openai/responses?api-version=2025-04-01-preview")))
                .isEmpty();
        // The known-host match is anchored at a domain boundary, so a suffixed host is not a known Azure host.
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com.attacker.example/openai/responses?api-version=2025-04-01-preview")))
                .isEmpty();
        // The same applies to the classic deployment shape, which then falls back to the custom deployment tier
        // (isCustomAzureOpenAiDeployment = true) rather than being treated as a first-party Azure endpoint.
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com@attacker.example/openai/deployments/o4-mini-deployment/chat/completions?api-version=2025-01-01-preview")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://test-domain.openai.azure.com@attacker.example/openai/deployments/o4-mini-deployment",
                                "",
                                "2025-01-01-preview",
                                true,
                                false)));
    }

    @Test
    void testOAuth2RejectedForClassicAzureDeploymentShape()
    {
        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("env", new EnvironmentVariableSecretProvider()));
        AiClientConfig aiClientConfig = new AiClientConfig();
        OAuth2TokenCache oauth2TokenCache = TestingUtils.createTestOAuth2TokenCache(aiClientConfig);
        OpenAiClientFactory factory = new OpenAiClientFactory(
                secretsResolver,
                aiClientConfig,
                directExecutor(),
                new JsonMapperProvider().get(),
                oauth2TokenCache,
                OpenTelemetry.noop().getTracer("test"));

        OAuth2Config oauthConfig = new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token",
                "client-id",
                "${ENV:CLIENT_SECRET}",
                Optional.of("https://cognitiveservices.azure.com/.default"),
                Optional.empty());
        OpenAiConnectionInfo connectionInfo = new OpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/gpt-4o-mini/chat/completions?api-version=2025-01-01-preview"),
                Optional.empty(),
                Map.of(),
                Optional.of(oauthConfig));
        LanguageModelConnectionSpec spec = new LanguageModelConnectionSpec(
                "model",
                "gpt-4o-mini",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                Map.of(),
                connectionInfo,
                false,
                Optional.empty());

        // The rejection must happen before any OAuth2 token fetch is attempted - the test OAuth2TokenCache
        // throws UnsupportedOperationException if its HTTP client is ever invoked.
        assertThatThrownBy(() -> factory.createLanguageModelClient(spec, connectionInfo, new StaticPromptDao(), TokenUsageListener.NOOP))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("OAuth2 is not supported for Azure OpenAI deployment endpoints");
    }
}
