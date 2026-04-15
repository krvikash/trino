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
import com.openai.errors.OpenAIInvalidDataException;
import com.openai.models.Reasoning;
import com.openai.models.ReasoningEffort;
import com.openai.models.responses.EasyInputMessage;
import com.openai.models.responses.FunctionTool;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCompletedEvent;
import com.openai.models.responses.ResponseCreateParams;
import com.openai.models.responses.ResponseFunctionToolCall;
import com.openai.models.responses.ResponseInputItem;
import com.openai.models.responses.ResponseOutputMessage;
import com.openai.models.responses.ResponseOutputRefusal;
import com.openai.models.responses.ResponseOutputText;
import com.openai.models.responses.ResponseStreamEvent;
import io.airlift.log.Logger;
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
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class OpenAiResponsesLanguageModelClient
        extends AbstractOpenAiClient<Response>
{
    private static final Logger log = Logger.get(OpenAiResponsesLanguageModelClient.class);

    private final Optional<Float> temperature;
    private final Optional<Integer> maxTokens;
    private final Optional<Float> topP;
    private final boolean useDeveloperForSystemRole;
    private final String modelName;
    private final Optional<String> endpoint;
    private final OpenAIClient client;
    private final Optional<ReasoningEffort> reasoningEffort;
    private final ObjectMapper objectMapper;

    public OpenAiResponsesLanguageModelClient(
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
            OpenAIClient client,
            boolean isToolStreamingSupported,
            Optional<ReasoningEffort> reasoningEffort,
            TokenUsageListener tokenUsageListener)
    {
        super(promptDao, executor, batchParallelism, isToolStreamingSupported, tokenUsageListener);
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.useDeveloperForSystemRole = useDeveloperForSystemRole;
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.client = requireNonNull(client, "client is null");
        this.reasoningEffort = requireNonNull(reasoningEffort, "reasoningEffort is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> llmMessages, TokenUsageContext context)
    {
        Response response = execute(() -> client.responses().create(buildResponseCreateParams(systemPrompts, llmMessages).build()), context);

        List<ResponseOutputMessage.Content> contents = response.output().stream()
                .flatMap(item -> item.message().stream())
                .flatMap(message -> message.content().stream())
                .collect(toImmutableList());
        throwOnRefusals(contents);

        return contents.stream()
                .flatMap(content -> content.outputText().stream())
                .map(ResponseOutputText::text)
                .collect(Collectors.joining());
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools,
            TokenUsageContext context)
    {
        ResponseCreateParams.Builder builder = buildResponseCreateParams(systemPrompts, messages);
        tools.forEach(tool -> builder.addTool(toOpenAiTool(tool)));

        Response response = execute(() -> client.responses().create(builder.build()), context);

        return parseToolResponse(response);
    }

    @Override
    protected Response streamToolResponse(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled)
    {
        ResponseCreateParams.Builder builder = buildResponseCreateParams(systemPrompts, messages);
        tools.forEach(tool -> builder.addTool(toOpenAiTool(tool)));
        return stream(builder.build(), output, isCancelled);
    }

    private Response stream(ResponseCreateParams params, Consumer<String> output, Supplier<Boolean> isCancelled)
    {
        try (StreamResponse<ResponseStreamEvent> streamResponse =
                     client.responses().createStreaming(params)) {
            return streamResponse.stream()
                    .takeWhile(_ -> !isCancelled.get())
                    .peek(event -> {
                        if (event.outputTextDelta().isPresent()) {
                            output.accept(event.outputTextDelta().get().delta());
                        }
                    })
                    .filter(ResponseStreamEvent::isCompleted)
                    .map(ResponseStreamEvent::asCompleted)
                    .map(ResponseCompletedEvent::response)
                    .findFirst().orElseThrow(() -> new TrinoException(AI_CLIENT_ERROR, "No completion event received from streaming response"));
        }
        catch (Exception e) {
            if (isCancelled.get()) {
                return null;
            }
            throw new TrinoException(AI_CLIENT_ERROR, "Error occurred during streaming response", e);
        }
    }

    private ResponseCreateParams.Builder buildResponseCreateParams(List<String> systemPrompts, List<LlmMessage> llmMessages)
    {
        ResponseCreateParams.Builder builder = ResponseCreateParams.builder()
                .model(modelName);
        temperature.ifPresent(builder::temperature);
        topP.ifPresent(builder::topP);
        maxTokens.ifPresent(builder::maxOutputTokens);
        reasoningEffort.ifPresent(effort -> builder.reasoning(Reasoning.builder().effort(effort).build()));

        ImmutableList.Builder<ResponseInputItem> inputItems = ImmutableList.builder();
        systemPrompts.forEach(systemPrompt -> inputItems.add(ResponseInputItem.ofEasyInputMessage(EasyInputMessage.builder()
                .role(useDeveloperForSystemRole ? EasyInputMessage.Role.DEVELOPER : EasyInputMessage.Role.SYSTEM)
                .content(systemPrompt)
                .build())));

        llmMessages.forEach(llmMessage -> {
            switch (llmMessage.role()) {
                case USER -> inputItems.add(ResponseInputItem.ofEasyInputMessage(EasyInputMessage.builder()
                        .role(EasyInputMessage.Role.USER)
                        .content(llmMessage.content())
                        .build()));
                case ASSISTANT -> inputItems.add(ResponseInputItem.ofEasyInputMessage(EasyInputMessage.builder()
                        .role(EasyInputMessage.Role.ASSISTANT)
                        .content(llmMessage.content())
                        .build()));
            }
        });
        builder.inputOfResponse(inputItems.build());
        return builder;
    }

    private static FunctionTool toOpenAiTool(ToolDefinition<?> toolDef)
    {
        try {
            JsonNode schema = toolDef.getInputSchema();
            FunctionTool.Parameters.Builder parametersBuilder = FunctionTool.Parameters.builder();
            schema.properties().forEach(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                parametersBuilder.putAdditionalProperty(key, JsonValue.fromJsonNode(value));
            });

            return FunctionTool.builder()
                    .name(toolDef.getName())
                    .description(toolDef.getDescription())
                    // strict is required to be set in FunctionTool, but strict=true prevents
                    // usage of tools with optional parameters:
                    // Caused by: com.openai.errors.BadRequestException: 400: Invalid schema
                    .strict(false)
                    .parameters(parametersBuilder.build()).build();
        }
        catch (Exception e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to convert tool definition to OpenAI tool call format", e);
        }
    }

    @Override
    protected Optional<TokenUsage> extractTokenUsage(Response response)
    {
        return response.usage()
                .map(u -> new TokenUsage(
                        u.inputTokens(),
                        u.outputTokens(),
                        safeTokenCount(() -> u.inputTokensDetails().cachedTokens()),
                        0L,
                        safeTokenCount(() -> u.outputTokensDetails().reasoningTokens()),
                        modelName,
                        endpoint,
                        ModelType.LANGUAGE,
                        ModelBackend.OPENAI));
    }

    private static long safeTokenCount(LongSupplier supplier)
    {
        try {
            return supplier.getAsLong();
        }
        catch (OpenAIInvalidDataException e) {
            log.warn(e, "Failed to read token count from response details, defaulting to 0");
            return 0L;
        }
    }

    @Override
    protected ToolUseResponse parseToolResponse(Response response)
    {
        StringBuilder message = new StringBuilder();
        ImmutableList.Builder<ToolUseResponse.ToolCall> toolCallBuilder = ImmutableList.builder();
        response.output().forEach(item -> {
            if (item.isFunctionCall()) {
                try {
                    ResponseFunctionToolCall functionCall = item.asFunctionCall();
                    JsonNode inputNode = objectMapper.readTree(functionCall.arguments());
                    toolCallBuilder.add(new ToolUseResponse.ToolCall(
                            functionCall.callId(),
                            functionCall.name(),
                            inputNode));
                }
                catch (JsonProcessingException e) {
                    throw new TrinoException(AI_CLIENT_ERROR, "Failed to parse tool call arguments", e);
                }
            }
            if (item.message().isPresent()) {
                List<ResponseOutputMessage.Content> contents = item.message().get().content();
                throwOnRefusals(contents);
                item.message().stream()
                        .flatMap(outputMessage -> outputMessage.content().stream())
                        .flatMap(content -> content.outputText().stream())
                        .map(ResponseOutputText::text)
                        .forEach(message::append);
            }
        });
        return new ToolUseResponse(message.toString(), toolCallBuilder.build());
    }

    private void throwOnRefusals(List<ResponseOutputMessage.Content> contents)
    {
        String refusal = contents.stream()
                .flatMap(content -> content.refusal().stream())
                .map(ResponseOutputRefusal::refusal)
                .collect(Collectors.joining("\n"));
        if (!refusal.isBlank()) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + refusal);
        }
    }
}
