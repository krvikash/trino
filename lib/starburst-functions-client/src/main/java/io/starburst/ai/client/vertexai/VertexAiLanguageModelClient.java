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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genai.Client;
import com.google.genai.ResponseStream;
import com.google.genai.types.Content;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.FunctionResponse;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.Part;
import com.google.genai.types.Schema;
import com.google.genai.types.ThinkingConfig;
import com.google.genai.types.Tool;
import io.airlift.json.JsonMapperProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
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
import io.starburst.ai.client.tracing.AiTracing;
import io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort;
import io.trino.spi.TrinoException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_OPERATION_NAME;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_PROVIDER_NAME;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_REQUEST_MODEL;
import static io.starburst.ai.client.tracing.GenAiAttributes.OPERATION_NAME_CHAT;
import static io.starburst.ai.client.tracing.GenAiAttributes.PROVIDER_NAME_VERTEX_AI;
import static java.util.Objects.requireNonNull;

public class VertexAiLanguageModelClient
        extends AbstractLanguageModelClient
{
    private static final ObjectMapper OBJECT_MAPPER = new JsonMapperProvider().get();
    // Documented sentinel telling Gemini 3.x this function-call part was not model-generated, so it
    // should not require (or validate) a real thoughtSignature on it.
    // https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking/thought-signatures#using-rest-or-manual-handling
    private static final String SKIP_THOUGHT_SIGNATURE_VALIDATOR = "skip_thought_signature_validator";

    private final String modelName;
    private final Optional<Integer> maxTokens;
    private final Optional<Float> temperature;
    private final Optional<Float> topP;
    private final Optional<ReasoningEffort> reasoningEffort;
    private final Client client;
    private final Tracer tracer;
    private final boolean includeMessagesInTracing;

    public VertexAiLanguageModelClient(
            String modelName,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            Optional<ReasoningEffort> reasoningEffort,
            PromptDao promptDao,
            Executor executor,
            int batchParallelism,
            Client client,
            Tracer tracer,
            boolean includeMessagesInTracing,
            TokenUsageListener tokenUsageListener)
    {
        super(promptDao, executor, batchParallelism, tokenUsageListener);
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.reasoningEffort = requireNonNull(reasoningEffort, "reasoningEffort is null");
        this.client = requireNonNull(client, "client is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.includeMessagesInTracing = includeMessagesInTracing;
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, String prompt, TokenUsageContext context)
    {
        return generateCompletion(systemPrompts, ImmutableList.of(new LlmMessage(USER, Optional.of(prompt), ImmutableList.of(), ImmutableList.of())), context);
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> messages, TokenUsageContext context)
    {
        GenerateContentConfig config = buildConfig(systemPrompts, maxTokens, temperature, topP, reasoningEffort);
        GenerateContentResponse response = generateContent(systemPrompts, messages, config, context);
        String text = response.text();
        if (text == null) {
            throw new TrinoException(AI_CLIENT_ERROR, "No response from Vertex AI model %s".formatted(modelName));
        }
        return text;
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, TokenUsageContext context)
    {
        GenerateContentConfig config = buildConfig(systemPrompts, maxTokens, temperature, topP, reasoningEffort, toTools(tools));
        try {
            GenerateContentResponse response = generateContent(systemPrompts, messages, config, context);
            return parseToolResponse(response);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute Vertex AI tool-calling request for model %s".formatted(modelName), e);
        }
    }

    private GenerateContentResponse generateContent(List<String> systemPrompts, List<LlmMessage> messages, GenerateContentConfig config, TokenUsageContext context)
    {
        Span span = startChatSpan();
        return AiTracing.withTracing(span, () -> {
            GenerateContentResponse response = client.models.generateContent(modelName, toContents(messages), config);
            response.usageMetadata().ifPresent(usage -> {
                TokenUsage tokenUsage = toTokenUsage(usage, modelName);
                AiTracing.recordUsage(span, tokenUsage);
                reportTokenUsage(context, tokenUsage);
            });
            if (includeMessagesInTracing) {
                try {
                    // generateCompletion() never calls parseToolResponse, so a parse
                    // failure must not fail an otherwise-successful response.
                    AiTracing.recordMessages(span, systemPrompts, messages, parseToolResponse(response));
                }
                catch (RuntimeException e) {
                    log.warn(e, "Failed to record AI messages in trace");
                }
            }
            return response;
        });
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled, TokenUsageContext context)
    {
        GenerateContentConfig config = buildConfig(systemPrompts, maxTokens, temperature, topP, reasoningEffort, toTools(tools));
        try {
            return streamContent(systemPrompts, messages, config, output, isCancelled, context);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute Vertex AI streaming tool-calling request for model %s".formatted(modelName), e);
        }
    }

    private ToolUseResponse streamContent(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            GenerateContentConfig config,
            Consumer<String> output,
            Supplier<Boolean> isCancelled,
            TokenUsageContext context)
    {
        // generateContentStream() both issues the request and blocks reading its first chunk, so a
        // caller that has already cancelled is spared a billable request it would discard.
        if (isCancelled.get()) {
            return new ToolUseResponse("", ImmutableList.of());
        }
        Span span = startChatSpan();
        return AiTracing.withTracing(span, () -> {
            StreamedContent streamed;
            try (ResponseStream<GenerateContentResponse> stream = client.models.generateContentStream(modelName, toContents(messages), config)) {
                streamed = accumulateStream(stream, output, isCancelled);
            }
            streamed.usageMetadata().ifPresent(usage -> {
                TokenUsage tokenUsage = toTokenUsage(usage, modelName);
                AiTracing.recordUsage(span, tokenUsage);
                reportTokenUsage(context, tokenUsage);
            });
            if (includeMessagesInTracing) {
                AiTracing.recordMessages(span, systemPrompts, messages, streamed.response());
            }
            return streamed.response();
        });
    }

    /**
     * Folds the chunks of a streamed response into a single {@link ToolUseResponse}, forwarding each
     * chunk's text to {@code output} as it arrives. Cancelling stops consuming the stream and keeps what
     * arrived so far, so the returned text always matches what the caller was handed through
     * {@code output}.
     *
     * <p>Cancellation takes effect at a chunk boundary, not mid-read: {@code ResponseStream} reads one
     * chunk ahead on a blocking {@code BufferedReader}, so the read for the following chunk is already
     * in flight by the time a chunk reaches this loop. Interrupting that read would mean closing the
     * stream from another thread, which the OpenAI client here does not do either; bounding cancellation
     * latency below one chunk belongs to the streaming layer as a whole rather than this provider.
     */
    @VisibleForTesting
    static StreamedContent accumulateStream(Iterable<GenerateContentResponse> chunks, Consumer<String> output, Supplier<Boolean> isCancelled)
    {
        StringBuilder textResponse = new StringBuilder();
        ImmutableList.Builder<ToolUseResponse.ToolCall> toolCalls = ImmutableList.builder();
        Optional<GenerateContentResponseUsageMetadata> usageMetadata = Optional.empty();
        Iterator<GenerateContentResponse> iterator = chunks.iterator();
        while (!isCancelled.get() && iterator.hasNext()) {
            GenerateContentResponse chunk = iterator.next();
            // Each chunk restates the cumulative usage, so the last one seen carries the totals.
            if (chunk.usageMetadata().isPresent()) {
                usageMetadata = chunk.usageMetadata();
            }
            // Parse per chunk rather than per part so streaming and non-streaming share one
            // interpretation of a response, including the part-to-thoughtSignature pairing.
            ToolUseResponse parsed = parseToolResponse(chunk);
            if (!parsed.textResponse().isEmpty()) {
                textResponse.append(parsed.textResponse());
                output.accept(parsed.textResponse());
            }
            toolCalls.addAll(parsed.toolCalls());
        }
        return new StreamedContent(new ToolUseResponse(textResponse.toString(), toolCalls.build()), usageMetadata);
    }

    @VisibleForTesting
    record StreamedContent(ToolUseResponse response, Optional<GenerateContentResponseUsageMetadata> usageMetadata)
    {
        StreamedContent
        {
            requireNonNull(response, "response is null");
            requireNonNull(usageMetadata, "usageMetadata is null");
        }
    }

    private Span startChatSpan()
    {
        return tracer.spanBuilder(OPERATION_NAME_CHAT + " " + modelName)
                .setAttribute(GEN_AI_OPERATION_NAME, OPERATION_NAME_CHAT)
                .setAttribute(GEN_AI_PROVIDER_NAME, PROVIDER_NAME_VERTEX_AI)
                .setAttribute(GEN_AI_REQUEST_MODEL, modelName)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
    }

    @VisibleForTesting
    static GenerateContentConfig buildConfig(
            List<String> systemPrompts,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            Optional<ReasoningEffort> reasoningEffort)
    {
        return buildConfig(systemPrompts, maxTokens, temperature, topP, reasoningEffort, ImmutableList.of());
    }

    @VisibleForTesting
    static GenerateContentConfig buildConfig(
            List<String> systemPrompts,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            Optional<ReasoningEffort> reasoningEffort,
            List<Tool> tools)
    {
        GenerateContentConfig.Builder builder = GenerateContentConfig.builder();
        maxTokens.ifPresent(builder::maxOutputTokens);
        temperature.ifPresent(builder::temperature);
        topP.ifPresent(builder::topP);
        reasoningEffort.ifPresent(effort -> builder.thinkingConfig(ThinkingConfig.builder().thinkingLevel(effort.name()).build()));
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
        ImmutableList.Builder<ToolUseResponse.ToolCall> toolCalls = ImmutableList.builder();
        // Iterate parts (not response.functionCalls(), which flattens and loses the part-to-signature
        // pairing) so each function call keeps the thoughtSignature Gemini 3.x attached to its own part.
        for (Part part : Optional.ofNullable(response.parts()).orElse(ImmutableList.of())) {
            // TODO: a response without a function call may carry a thoughtSignature on its last part,
            //  which is dropped here. Omitting it never errors (unlike a function-call signature) but costs
            //  cross-turn reasoning quality. Preserving it needs textResponse and LlmMessage.content to
            //  carry the signature, plus the downstream session-storage models to persist it.

            // A thought part carries the model's reasoning summary in text(), not assistant output, so
            // exclude it as GenerateContentResponse.text() does. Gemini returns these only when
            // ThinkingConfig.includeThoughts is set, which this client does not set.
            if (!part.thought().orElse(false)) {
                part.text().ifPresent(textResponse::append);
            }
            part.functionCall().ifPresent(functionCall -> toolCalls.add(toToolCall(functionCall, part.thoughtSignature())));
        }
        return new ToolUseResponse(textResponse.toString(), toolCalls.build());
    }

    @VisibleForTesting
    static ToolUseResponse.ToolCall toToolCall(FunctionCall functionCall, Optional<byte[]> thoughtSignature)
    {
        String name = functionCall.name()
                .orElseThrow(() -> new TrinoException(AI_CLIENT_ERROR, "Vertex AI function call is missing a name"));
        Map<String, Object> args = functionCall.args().orElse(ImmutableMap.of());
        return new ToolUseResponse.ToolCall(
                functionCall.id().orElse(name),
                name,
                OBJECT_MAPPER.valueToTree(args),
                thoughtSignature.map(signature -> Base64.getEncoder().encodeToString(signature)));
    }

    private static Map<String, Object> jsonToObjectMap(JsonNode node)
    {
        if (node == null || node.isNull()) {
            return ImmutableMap.of();
        }
        return OBJECT_MAPPER.convertValue(node, new TypeReference<>() {});
    }

    @VisibleForTesting
    static List<Content> toContents(List<LlmMessage> messages)
    {
        Map<String, String> toolCallIdNameMap = messages.stream()
                .filter(m -> m.role() == ASSISTANT)
                .flatMap(m -> m.toolCalls().stream())
                .collect(toImmutableMap(ToolUseResponse.ToolCall::id, ToolUseResponse.ToolCall::name));

        return messages.stream()
                .map(message -> toContent(message, toolCallIdNameMap))
                .collect(toImmutableList());
    }

    private static Content toContent(LlmMessage message, Map<String, String> toolCallIdNameMap)
    {
        String role = switch (message.role()) {
            case USER, TOOL_RESPONSE -> "user";
            case ASSISTANT -> "model";
        };
        ImmutableList.Builder<Part> parts = ImmutableList.builder();
        switch (message.role()) {
            case USER -> parts.add(Part.fromText(message.content().orElseThrow()));
            case ASSISTANT -> {
                message.content().ifPresent(text -> parts.add(Part.fromText(text)));
                List<ToolUseResponse.ToolCall> toolCalls = message.toolCalls();
                for (int i = 0; i < toolCalls.size(); i++) {
                    ToolUseResponse.ToolCall toolCall = toolCalls.get(i);
                    Part.Builder part = Part.builder()
                            .functionCall(FunctionCall.builder()
                                    .id(toolCall.id())
                                    .name(toolCall.name())
                                    .args(jsonToObjectMap(toolCall.input())));
                    Optional<byte[]> signature = toolCall.thoughtSignature()
                            .map(encoded -> Base64.getDecoder().decode(encoded));
                    // Gemini 3.x validates only the first function-call part of each step: it must carry
                    // a signature or the follow-up request 400s. Re-attach the model's own when captured;
                    // otherwise fall back to the skip sentinel for that first call (app-synthesized, or
                    // persisted before this field existed). Gemini signs only the first call of a parallel
                    // group, so later unsigned calls are replayed with no signature, exactly as received.
                    // https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking/thought-signatures
                    if (signature.isPresent()) {
                        part.thoughtSignature(signature.orElseThrow());
                    }
                    else if (i == 0) {
                        part.thoughtSignature(SKIP_THOUGHT_SIGNATURE_VALIDATOR.getBytes(StandardCharsets.UTF_8));
                    }
                    parts.add(part.build());
                }
            }
            case TOOL_RESPONSE -> {
                for (LlmMessage.ToolResponse response : message.toolResponse()) {
                    String name = toolCallIdNameMap.get(response.toolUseId());
                    if (name == null) {
                        throw new TrinoException(
                                AI_CLIENT_ERROR,
                                "No matching tool call found for tool response id " + response.toolUseId());
                    }
                    parts.add(Part.builder()
                            .functionResponse(FunctionResponse.builder()
                                    .id(response.toolUseId())
                                    .name(name)
                                    .response(jsonToObjectMap(response.responseJson())))
                            .build());
                }
            }
        }
        return Content.builder()
                .role(role)
                .parts(parts.build())
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
