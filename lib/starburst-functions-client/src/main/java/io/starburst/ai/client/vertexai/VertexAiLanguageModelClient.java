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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.genai.Client;
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.Part;
import io.starburst.ai.client.AbstractLanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.ModelBackend;
import io.starburst.ai.client.ModelType;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.TokenUsageContext;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.client.ToolDefinition;
import io.starburst.ai.client.ToolUseResponse;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.MessageRole.USER;
import static java.util.Objects.requireNonNull;

public class VertexAiLanguageModelClient
        extends AbstractLanguageModelClient
{
    private final String modelName;
    private final Optional<Integer> maxTokens;
    private final Optional<Float> temperature;
    private final Optional<Float> topP;
    private final Client client;

    public VertexAiLanguageModelClient(
            String modelName,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            PromptDao promptDao,
            Executor executor,
            int batchParallelism,
            Client client,
            TokenUsageListener tokenUsageListener)
    {
        super(promptDao, executor, batchParallelism, tokenUsageListener);
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, String prompt, TokenUsageContext context)
    {
        return generateCompletion(systemPrompts, ImmutableList.of(new LlmMessage(USER, prompt)), context);
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> messages, TokenUsageContext context)
    {
        GenerateContentConfig config = buildConfig(systemPrompts, maxTokens, temperature, topP);
        GenerateContentResponse response = client.models.generateContent(modelName, toContents(messages), config);
        response.usageMetadata().ifPresent(usage -> reportTokenUsage(context, toTokenUsage(usage, modelName)));
        String text = response.text();
        if (text == null) {
            throw new TrinoException(AI_CLIENT_ERROR, "No response from Vertex AI model %s".formatted(modelName));
        }
        return text;
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, TokenUsageContext context)
    {
        // Tool-calling is implemented in Stage 4.
        throw new UnsupportedOperationException("Vertex AI tool-calling is not yet implemented");
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled, TokenUsageContext context)
    {
        // Tool-calling is implemented in Stage 4.
        throw new UnsupportedOperationException("Vertex AI tool-calling is not yet implemented");
    }

    @VisibleForTesting
    static GenerateContentConfig buildConfig(List<String> systemPrompts, Optional<Integer> maxTokens, Optional<Float> temperature, Optional<Float> topP)
    {
        GenerateContentConfig.Builder builder = GenerateContentConfig.builder();
        maxTokens.ifPresent(builder::maxOutputTokens);
        temperature.ifPresent(builder::temperature);
        topP.ifPresent(builder::topP);
        if (!systemPrompts.isEmpty()) {
            List<Part> parts = systemPrompts.stream()
                    .map(Part::fromText)
                    .collect(toImmutableList());
            builder.systemInstruction(Content.builder().parts(parts).build());
        }
        return builder.build();
    }

    @VisibleForTesting
    static List<Content> toContents(List<LlmMessage> messages)
    {
        return messages.stream()
                .map(VertexAiLanguageModelClient::toContent)
                .collect(toImmutableList());
    }

    private static Content toContent(LlmMessage message)
    {
        String role = switch (message.role()) {
            case USER -> "user";
            case ASSISTANT -> "model";
        };
        return Content.builder()
                .role(role)
                .parts(Part.fromText(message.content()))
                .build();
    }

    @VisibleForTesting
    static TokenUsage toTokenUsage(GenerateContentResponseUsageMetadata usage, String modelName)
    {
        return new TokenUsage(
                tokenCount(usage.promptTokenCount()),
                tokenCount(usage.candidatesTokenCount()),
                tokenCount(usage.cachedContentTokenCount()),
                0L,
                tokenCount(usage.thoughtsTokenCount()),
                modelName,
                Optional.empty(),
                ModelType.LANGUAGE,
                ModelBackend.VERTEX_AI);
    }

    private static long tokenCount(Optional<Integer> value)
    {
        return value.map(Integer::longValue).orElse(0L);
    }
}
