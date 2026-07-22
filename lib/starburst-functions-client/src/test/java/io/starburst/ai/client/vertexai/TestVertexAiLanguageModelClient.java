/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.vertexai;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.genai.Client;
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import io.airlift.configuration.secrets.SecretsResolver;
import io.starburst.ai.client.AiClientConfig;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.ModelBackend;
import io.starburst.ai.client.ModelType;
import io.starburst.ai.client.StaticPromptDao;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.TokenUsageContext;
import io.starburst.ai.client.TokenUsageListener;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.buildConfig;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toContents;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toTokenUsage;
import static io.starburst.ai.model.ConnectionInfo.VertexAiConnectionInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVertexAiLanguageModelClient
{
    @Test
    void testBuildConfigMapsSpecParameters()
    {
        GenerateContentConfig config = buildConfig(
                ImmutableList.of("You are a helpful assistant"),
                Optional.of(1024),
                Optional.of(0.2f),
                Optional.of(0.9f));

        assertThat(config.maxOutputTokens()).contains(1024);
        assertThat(config.temperature()).contains(0.2f);
        assertThat(config.topP()).contains(0.9f);
        assertThat(config.systemInstruction()).isPresent();
        assertThat(config.systemInstruction().orElseThrow().text()).isEqualTo("You are a helpful assistant");
    }

    @Test
    void testBuildConfigOmitsAbsentParameters()
    {
        GenerateContentConfig config = buildConfig(
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(config.maxOutputTokens()).isEmpty();
        assertThat(config.temperature()).isEmpty();
        assertThat(config.topP()).isEmpty();
        assertThat(config.systemInstruction()).isEmpty();
    }

    @Test
    void testToContentsMapsRoles()
    {
        List<Content> contents = toContents(ImmutableList.of(
                new LlmMessage(USER, "hello"),
                new LlmMessage(ASSISTANT, "hi there")));

        assertThat(contents).hasSize(2);
        assertThat(contents.get(0).role()).contains("user");
        assertThat(contents.get(0).text()).isEqualTo("hello");
        assertThat(contents.get(1).role()).contains("model");
        assertThat(contents.get(1).text()).isEqualTo("hi there");
    }

    @Test
    void testToTokenUsageMapsMetadata()
    {
        GenerateContentResponseUsageMetadata usage = GenerateContentResponseUsageMetadata.builder()
                .promptTokenCount(12)
                .candidatesTokenCount(34)
                .cachedContentTokenCount(5)
                .thoughtsTokenCount(7)
                .totalTokenCount(53)
                .build();

        TokenUsage tokenUsage = toTokenUsage(usage, "gemini-1.5-pro");

        assertThat(tokenUsage.inputTokens()).isEqualTo(12L);
        assertThat(tokenUsage.outputTokens()).isEqualTo(34L);
        assertThat(tokenUsage.cacheReadInputTokens()).isEqualTo(5L);
        assertThat(tokenUsage.cacheCreationInputTokens()).isEqualTo(0L);
        assertThat(tokenUsage.reasoningOutputTokens()).isEqualTo(7L);
        assertThat(tokenUsage.modelName()).isEqualTo("gemini-1.5-pro");
        assertThat(tokenUsage.endpoint()).isEmpty();
        assertThat(tokenUsage.modelType()).isEqualTo(ModelType.LANGUAGE);
        assertThat(tokenUsage.modelBackend()).isEqualTo(ModelBackend.VERTEX_AI);
    }

    @Test
    void testToTokenUsageDefaultsMissingCountsToZero()
    {
        GenerateContentResponseUsageMetadata usage = GenerateContentResponseUsageMetadata.builder()
                .promptTokenCount(3)
                .build();

        TokenUsage tokenUsage = toTokenUsage(usage, "gemini-1.5-flash");

        assertThat(tokenUsage.inputTokens()).isEqualTo(3L);
        assertThat(tokenUsage.outputTokens()).isEqualTo(0L);
        assertThat(tokenUsage.cacheReadInputTokens()).isEqualTo(0L);
        assertThat(tokenUsage.reasoningOutputTokens()).isEqualTo(0L);
    }

    @Test
    void testGenerateCompletionWithToolsThrows()
            throws IOException
    {
        VertexAiLanguageModelClient client = createClient();

        assertThatThrownBy(() -> client.generateCompletionWithTools(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), TokenUsageContext.EMPTY))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Vertex AI tool-calling is not yet implemented");

        assertThatThrownBy(() -> client.generateCompletionWithTools(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), _ -> {}, () -> false, TokenUsageContext.EMPTY))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Vertex AI tool-calling is not yet implemented");
    }

    private static VertexAiLanguageModelClient createClient()
            throws IOException
    {
        String serviceAccountKey = Resources.toString(getResource("vertex-ai-service-account.json"), StandardCharsets.UTF_8);
        VertexAiConnectionInfo connectionInfo = new VertexAiConnectionInfo(
                Optional.of(serviceAccountKey),
                Optional.of("my-project"),
                "us-central1",
                ImmutableMap.of());
        VertexAiClientFactory factory = new VertexAiClientFactory(new SecretsResolver(ImmutableMap.of()), new AiClientConfig(), directExecutor());
        Client vertexClient = factory.createClient(connectionInfo);
        return new VertexAiLanguageModelClient(
                "gemini-1.5-pro",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new StaticPromptDao(),
                directExecutor(),
                1,
                vertexClient,
                TokenUsageListener.NOOP);
    }
}
