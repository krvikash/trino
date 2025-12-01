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
import com.openai.errors.InternalServerException;
import com.openai.errors.RateLimitException;
import com.openai.helpers.ChatCompletionAccumulator;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionChunk;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessage;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.completions.CompletionUsage;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.json.ObjectMapperProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.AbstractLanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.ToolDefinition;
import io.starburst.ai.client.ToolUseResponse;
import io.trino.spi.TrinoException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPENAI_RESPONSE_SERVICE_TIER;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_SEED;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_ID;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_SYSTEM;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiOperationNameIncubatingValues.CHAT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiSystemIncubatingValues.OPENAI;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.MessageRole.USER;
import static java.util.Objects.requireNonNull;

public class OpenAiLanguageModelClient
        extends AbstractLanguageModelClient
{
    private static final int SEED = 37;
    private static final RetryPolicy<ChatCompletion> RATE_LIMIT_RETRY_POLICY = RetryPolicy.<ChatCompletion>builder()
            .handleIf(OpenAiLanguageModelClient::isRetryable)
            .withMaxRetries(4)
            .withBackoff(Duration.ofMillis(500), Duration.ofMinutes(2))
            .withJitter(0.25)
            .build();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final Optional<Float> temperature;
    private final Optional<Integer> maxTokens;
    private final Optional<Float> topP;
    private final boolean useDeveloperForSystemRole;
    private final Tracer tracer;
    private final String modelName;
    private final boolean isGeminiEndpoint;
    private final OpenAIClient client;

    public OpenAiLanguageModelClient(
            String modelName,
            Optional<Float> temperature,
            Optional<Integer> maxTokens,
            Optional<Float> topP,
            boolean useDeveloperForSystemRole,
            PromptDao promptDao,
            Executor executor,
            int batchParallelism,
            Tracer tracer,
            boolean isGeminiEndpoint,
            OpenAIClient client)
    {
        super(promptDao, executor, batchParallelism);
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.useDeveloperForSystemRole = useDeveloperForSystemRole;
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.isGeminiEndpoint = isGeminiEndpoint;
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, String prompt)
    {
        return generateCompletion(systemPrompts, ImmutableList.of(new LlmMessage(USER, prompt)));
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> llmMessages)
    {
        ChatCompletion response = getChatCompletion(() -> client.chat().completions().create(buildChatCompletionCreateParams(systemPrompts, llmMessages).build()));
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
            List<ToolDefinition<?>> tools)
    {
        ChatCompletionCreateParams.Builder builder = buildChatCompletionCreateParams(systemPrompts, messages);
        tools.forEach(tool -> builder.addTool(toOpenAiTool(tool)));

        ChatCompletion response = getChatCompletion(() -> Failsafe.with(RATE_LIMIT_RETRY_POLICY).get(() -> client.chat().completions().create(builder.build())));

        return parseOpenAiToolResponse(response);
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> llmMessages,
            List<ToolDefinition<?>> tools,
            Consumer<String> output)
    {
        // This is necessary because Gemini's tool call streaming does not follow the OpenAI spec.
        // See https://discuss.ai.google.dev/t/gemini-openai-compatibility-issue-with-tool-call-streaming/59886
        // TODO: Remove this once LLM traits are merged: https://github.com/starburstdata/starburst-enterprise/pull/16310
        if (isGeminiEndpoint) {
            ToolUseResponse response = generateCompletionWithTools(systemPrompts, llmMessages, tools);
            output.accept(response.textResponse());
            return response;
        }
        ChatCompletionCreateParams.Builder builder = buildChatCompletionCreateParams(systemPrompts, llmMessages);
        tools.forEach(tool -> builder.addTool(toOpenAiTool(tool)));

        ChatCompletion response = getChatCompletion(() -> stream(builder.build(), output));

        return parseOpenAiToolResponse(response);
    }

    private ChatCompletion stream(ChatCompletionCreateParams params, Consumer<String> output)
    {
        ChatCompletionAccumulator chatCompletionAccumulator = ChatCompletionAccumulator.create();
        try (StreamResponse<ChatCompletionChunk> streamResponse =
                     client.chat().completions().createStreaming(params)) {
            streamResponse.stream()
                    .peek(chatCompletionAccumulator::accumulate)
                    .filter(completion -> !completion.choices().isEmpty())
                    .map(completion -> completion.choices().getFirst())
                    .flatMap(choice -> choice.delta().content().stream())
                    .filter(content -> !content.isEmpty())
                    .forEach(output);
        }
        catch (Exception e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Error occurred during streaming chat completion", e);
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

    ChatCompletion getChatCompletion(Supplier<ChatCompletion> getOpenAiResponse)
    {
        Span span = tracer.spanBuilder(CHAT + " " + modelName)
                .setAttribute(GEN_AI_OPERATION_NAME, CHAT)
                .setAttribute(GEN_AI_SYSTEM, OPENAI)
                .setAttribute(GEN_AI_REQUEST_MODEL, modelName)
                .setAttribute(GEN_AI_REQUEST_SEED, SEED)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (var _ = span.makeCurrent()) {
            ChatCompletion response = Failsafe.with(RATE_LIMIT_RETRY_POLICY).get(getOpenAiResponse::get);

            span.setAttribute(GEN_AI_RESPONSE_ID, response.id());
            span.setAttribute(GEN_AI_RESPONSE_MODEL, response.model());
            span.setAttribute(GEN_AI_OPENAI_RESPONSE_SERVICE_TIER, response.serviceTier()
                    .map(ChatCompletion.ServiceTier::value)
                    .map(ChatCompletion.ServiceTier.Value::name)
                    .orElse(""));
            span.setAttribute(GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT, response.systemFingerprint().orElse(""));
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, response.usage().map(CompletionUsage::promptTokens).orElse(0L));
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, response.usage().map(CompletionUsage::completionTokens).orElse(0L));

            return response;
        }
        catch (RuntimeException e) {
            span.setStatus(ERROR, e.getMessage());
            span.recordException(e);
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute AI request with tools", e);
        }
        finally {
            span.end();
        }
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
                            //.strict(true) // Guarantees arguments will be generated that match the schema, but strict mode seems to prevent
                            // usage of tools with optional parameters:
                            // Caused by: com.openai.errors.BadRequestException: 400: Invalid schema for function 'search': In context=(), 'required' is required to be supplied and to be an array including every key in properties. Missing 'max_results'.

                            .build())
                    .build();
        }
        catch (Exception e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to convert tool definition to OpenAI tool call format", e);
        }
    }

    private ToolUseResponse parseOpenAiToolResponse(ChatCompletion response)
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
                        JsonNode inputNode = OBJECT_MAPPER.readTree(argumentsJson);

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

    private static boolean isRetryable(Throwable t)
    {
        return t instanceof RateLimitException || t instanceof InternalServerException;
    }
}
