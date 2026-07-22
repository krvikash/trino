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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genai.Client;
import com.google.genai.types.Content;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.Part;
import com.google.genai.types.Schema;
import com.google.genai.types.Tool;
import io.airlift.json.ObjectMapperProvider;
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
import java.util.Map;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

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
        GenerateContentConfig config = buildConfig(systemPrompts, maxTokens, temperature, topP, toTools(tools));
        try {
            GenerateContentResponse response = client.models.generateContent(modelName, toContents(messages), config);
            response.usageMetadata().ifPresent(usage -> reportTokenUsage(context, toTokenUsage(usage, modelName)));
            return parseToolResponse(response);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute Vertex AI tool-calling request for model %s".formatted(modelName), e);
        }
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled, TokenUsageContext context)
    {
        ToolUseResponse response = generateCompletionWithTools(systemPrompts, messages, tools, context);
        output.accept(response.textResponse());
        return response;
    }

    @VisibleForTesting
    static GenerateContentConfig buildConfig(List<String> systemPrompts, Optional<Integer> maxTokens, Optional<Float> temperature, Optional<Float> topP)
    {
        return buildConfig(systemPrompts, maxTokens, temperature, topP, ImmutableList.of());
    }

    @VisibleForTesting
    static GenerateContentConfig buildConfig(List<String> systemPrompts, Optional<Integer> maxTokens, Optional<Float> temperature, Optional<Float> topP, List<Tool> tools)
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
        if (!tools.isEmpty()) {
            builder.tools(tools);
        }
        return builder.build();
    }

    @VisibleForTesting
    static List<Tool> toTools(List<ToolDefinition<?>> tools)
    {
        if (tools.isEmpty()) {
            return ImmutableList.of();
        }
        List<FunctionDeclaration> declarations = tools.stream()
                .map(VertexAiLanguageModelClient::toFunctionDeclaration)
                .collect(toImmutableList());
        return ImmutableList.of(Tool.builder().functionDeclarations(declarations).build());
    }

    private static FunctionDeclaration toFunctionDeclaration(ToolDefinition<?> tool)
    {
        return FunctionDeclaration.builder()
                .name(tool.getName())
                .description(tool.getDescription())
                .parameters(toSchema(tool.getInputSchema()))
                .build();
    }

    @VisibleForTesting
    static Schema toSchema(JsonNode node)
    {
        Schema.Builder builder = Schema.builder();
        if (node.has("type")) {
            builder.type(Ascii.toUpperCase(node.get("type").asText()));
        }
        if (node.has("description")) {
            builder.description(node.get("description").asText());
        }
        if (node.has("enum")) {
            ImmutableList.Builder<String> values = ImmutableList.builder();
            node.get("enum").forEach(value -> values.add(value.asText()));
            builder.enum_(values.build());
        }
        if (node.has("properties")) {
            ImmutableMap.Builder<String, Schema> properties = ImmutableMap.builder();
            node.get("properties").fields().forEachRemaining(entry -> properties.put(entry.getKey(), toSchema(entry.getValue())));
            builder.properties(properties.buildOrThrow());
        }
        if (node.has("required")) {
            ImmutableList.Builder<String> required = ImmutableList.builder();
            node.get("required").forEach(value -> required.add(value.asText()));
            builder.required(required.build());
        }
        if (node.has("items")) {
            builder.items(toSchema(node.get("items")));
        }
        return builder.build();
    }

    @VisibleForTesting
    static ToolUseResponse parseToolResponse(GenerateContentResponse response)
    {
        StringBuilder textResponse = new StringBuilder();
        for (Part part : Optional.ofNullable(response.parts()).orElse(ImmutableList.of())) {
            part.text().ifPresent(textResponse::append);
        }
        List<ToolUseResponse.ToolCall> toolCalls = Optional.ofNullable(response.functionCalls()).orElse(ImmutableList.of()).stream()
                .map(VertexAiLanguageModelClient::toToolCall)
                .collect(toImmutableList());
        return new ToolUseResponse(textResponse.toString(), toolCalls);
    }

    @VisibleForTesting
    static ToolUseResponse.ToolCall toToolCall(FunctionCall functionCall)
    {
        String name = functionCall.name()
                .orElseThrow(() -> new TrinoException(AI_CLIENT_ERROR, "Vertex AI function call is missing a name"));
        Map<String, Object> args = functionCall.args().orElse(ImmutableMap.of());
        return new ToolUseResponse.ToolCall(
                functionCall.id().orElse(name),
                name,
                OBJECT_MAPPER.valueToTree(args));
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
