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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.openai.client.OpenAIClient;
import com.openai.core.JsonValue;
import com.openai.core.http.StreamResponse;
import com.openai.helpers.ChatCompletionAccumulator;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionChunk;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessage;
import com.openai.models.chat.completions.ChatCompletionStreamOptions;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.completions.CompletionUsage;
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

import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class OpenAiLanguageModelClient
        extends AbstractOpenAiClient<ChatCompletion>
{
    private final Optional<Float> temperature;
    private final Optional<Integer> maxTokens;
    private final Optional<Float> topP;
    private final boolean useDeveloperForSystemRole;
    private final String modelName;
    private final Optional<String> endpoint;
    private final boolean isGeminiEndpoint;
    private final OpenAIClient client;
    private final ObjectMapper objectMapper;

    public OpenAiLanguageModelClient(
            String modelName,
            Optional<String> endpoint,
            Optional<Float> temperature,
            Optional<Integer> maxTokens,
            Optional<Float> topP,
            boolean useDeveloperForSystemRole,
            PromptDao promptDao,
            ObjectMapper objectMapper,
            Executor executor,
            int batchParallelism,
            boolean isGeminiEndpoint,
            OpenAIClient client,
            boolean isToolStreamingSupported,
            TokenUsageListener tokenUsageListener)
    {
        super(promptDao, executor, batchParallelism, isToolStreamingSupported, tokenUsageListener);
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.useDeveloperForSystemRole = useDeveloperForSystemRole;
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.isGeminiEndpoint = isGeminiEndpoint;
        this.client = requireNonNull(client, "client is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> llmMessages, TokenUsageContext context)
    {
        ChatCompletion response = execute(() -> client.chat().completions().create(buildChatCompletionCreateParams(systemPrompts, llmMessages).build()), context);
        ChatCompletionMessage message = response.choices().stream()
                .map(ChatCompletion.Choice::message)
                .findFirst()
                .orElseThrow(() -> new TrinoException(AI_CLIENT_ERROR, "No response from AI model"));

        if (message.refusal().isPresent()) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + message.refusal());
        }

        return message.content().orElse("");
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools,
            TokenUsageContext context)
    {
        ChatCompletionCreateParams.Builder builder = buildChatCompletionCreateParams(systemPrompts, messages);
        tools.forEach(tool -> builder.addTool(toOpenAiTool(tool)));

        ChatCompletion response = execute(() -> client.chat().completions().create(builder.build()), context);

        return parseToolResponse(response);
    }

    @Override
    protected ChatCompletion streamToolResponse(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled)
    {
        ChatCompletionCreateParams.Builder builder = buildChatCompletionCreateParams(systemPrompts, messages);
        tools.forEach(tool -> builder.addTool(toOpenAiTool(tool)));
        builder.streamOptions(ChatCompletionStreamOptions.builder().includeUsage(true).build());
        return stream(builder.build(), output, isCancelled);
    }

    private ChatCompletion stream(ChatCompletionCreateParams params, Consumer<String> output, Supplier<Boolean> isCancelled)
    {
        ChatCompletionAccumulator chatCompletionAccumulator = ChatCompletionAccumulator.create();
        try (StreamResponse<ChatCompletionChunk> streamResponse =
                client.chat().completions().createStreaming(params)) {
            streamResponse.stream()
                    .takeWhile(_ -> !isCancelled.get())
                    .peek(chatCompletionAccumulator::accumulate)
                    .filter(completion -> !completion.choices().isEmpty())
                    .map(completion -> completion.choices().getFirst())
                    .flatMap(choice -> choice.delta().content().stream())
                    .filter(content -> !content.isEmpty())
                    .forEach(output);
        }
        catch (Exception e) {
            if (isCancelled.get()) {
                return null;
            }
            throw toTrinoException(e);
        }
        if (isCancelled.get()) {
            return null;
        }
        return chatCompletionAccumulator.chatCompletion();
    }

    private ChatCompletionCreateParams.Builder buildChatCompletionCreateParams(List<String> systemPrompts, List<LlmMessage> llmMessages)
    {
        ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder()
                .model(modelName);
        if (!isGeminiEndpoint) {
            builder.seed(SEED);
        }
        temperature.ifPresent(builder::temperature);
        topP.ifPresent(builder::topP);
        maxTokens.ifPresent(builder::maxTokens);

        if (useDeveloperForSystemRole) {
            systemPrompts.forEach(builder::addSystemMessage);
        }
        else {
            systemPrompts.forEach(builder::addDeveloperMessage);
        }

        llmMessages.forEach(llmMessage -> {
            switch (llmMessage.role()) {
                case USER -> builder.addUserMessage(llmMessage.content());
                case ASSISTANT -> builder.addMessage(createAssistantMessage(llmMessage.content()));
            }
        });
        return builder;
    }

    @Override
    protected Optional<TokenUsage> extractTokenUsage(ChatCompletion response)
    {
        return response.usage()
                .map(u -> new TokenUsage(
                        u.promptTokens(),
                        u.completionTokens(),
                        u.promptTokensDetails()
                                .flatMap(CompletionUsage.PromptTokensDetails::cachedTokens)
                                .orElse(0L),
                        0L,
                        u.completionTokensDetails()
                                .flatMap(CompletionUsage.CompletionTokensDetails::reasoningTokens)
                                .orElse(0L),
                        modelName,
                        endpoint,
                        ModelType.LANGUAGE,
                        ModelBackend.OPENAI));
    }

    private static ChatCompletionTool toOpenAiTool(ToolDefinition<?> toolDef)
    {
        try {
            JsonNode schema = toolDef.getInputSchema();
            FunctionParameters.Builder parametersBuilder = FunctionParameters.builder();
            schema.properties().forEach(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                parametersBuilder.putAdditionalProperty(key, JsonValue.fromJsonNode(value));
            });

            return ChatCompletionTool.builder()
                    .type(JsonValue.from("function"))
                    .function(FunctionDefinition.builder()
                            .name(toolDef.getName())
                            .description(toolDef.getDescription())
                            .parameters(parametersBuilder.build())
                            // .strict(true) // Guarantees arguments will be generated that match the schema, but strict mode seems to prevent
                            // usage of tools with optional parameters:
                            // Caused by: com.openai.errors.BadRequestException: 400: Invalid schema for function 'search': In context=(), 'required' is required to be supplied and to be an array including every key in properties. Missing 'max_results'.

                            .build())
                    .build();
        }
        catch (Exception e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to convert tool definition to OpenAI tool call format", e);
        }
    }

    @Override
    protected ToolUseResponse parseToolResponse(ChatCompletion response)
    {
        ChatCompletionMessage message = response.choices().stream()
                .map(ChatCompletion.Choice::message)
                .findFirst()
                .orElseThrow(() -> new TrinoException(AI_CLIENT_ERROR, "No response from AI model"));

        if (message.refusal().isPresent()) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + message.refusal());
        }

        ImmutableList.Builder<ToolUseResponse.ToolCall> toolCallBuilder = ImmutableList.builder();
        message.toolCalls().ifPresent(toolCallList ->
                toolCallList.forEach(toolCall -> {
                    try {
                        // Parse the function arguments as JSON
                        String argumentsJson = toolCall.function().arguments();
                        JsonNode inputNode = objectMapper.readTree(argumentsJson);

                        toolCallBuilder.add(new ToolUseResponse.ToolCall(
                                toolCall.id(),
                                toolCall.function().name(),
                                inputNode));
                    }
                    catch (JsonProcessingException e) {
                        throw new TrinoException(AI_CLIENT_ERROR, "Failed to parse tool call arguments", e);
                    }
                }));

        return new ToolUseResponse(message.content().orElse(""), toolCallBuilder.build());
    }

    private static ChatCompletionAssistantMessageParam createAssistantMessage(String content)
    {
        return ChatCompletionAssistantMessageParam.builder()
                .role(JsonValue.from("assistant"))
                .content(content)
                .build();
    }
}
