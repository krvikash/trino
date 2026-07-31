/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
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
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.protocols.json.internal.unmarshall.document.DocumentUnmarshaller;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.AccessDeniedException;
import software.amazon.awssdk.services.bedrockruntime.model.CachePointBlock;
import software.amazon.awssdk.services.bedrockruntime.model.CachePointType;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDeltaEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamMetadataEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.InternalServerException;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ModelErrorException;
import software.amazon.awssdk.services.bedrockruntime.model.ModelNotReadyException;
import software.amazon.awssdk.services.bedrockruntime.model.ModelTimeoutException;
import software.amazon.awssdk.services.bedrockruntime.model.ResourceNotFoundException;
import software.amazon.awssdk.services.bedrockruntime.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.bedrockruntime.model.ServiceUnavailableException;
import software.amazon.awssdk.services.bedrockruntime.model.StopReason;
import software.amazon.awssdk.services.bedrockruntime.model.SystemContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ThrottlingException;
import software.amazon.awssdk.services.bedrockruntime.model.Tool;
import software.amazon.awssdk.services.bedrockruntime.model.ToolInputSchema;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolSpecification;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlockDelta;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlockStart;
import software.amazon.awssdk.services.bedrockruntime.model.ValidationException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.MessageRole.TOOL_RESPONSE;
import static io.starburst.ai.client.MessageRole.USER;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

public class AwsBedrockLanguageModelClient
        extends AbstractLanguageModelClient
{
    private static final Set<StopReason> ERROR_STOP_REASONS = ImmutableSet.of(
            StopReason.UNKNOWN_TO_SDK_VERSION,
            StopReason.GUARDRAIL_INTERVENED,
            StopReason.CONTENT_FILTERED,
            StopReason.MAX_TOKENS,
            StopReason.MODEL_CONTEXT_WINDOW_EXCEEDED,
            StopReason.MALFORMED_MODEL_OUTPUT,
            StopReason.MALFORMED_TOOL_USE);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final Logger log = Logger.get(AwsBedrockLanguageModelClient.class);

    static final int MIN_CACHE_POINT_CHARS = 5_000;
    static final int MAX_CACHE_POINT_CHARS = 100_000;

    private final Optional<Integer> maxTokens;
    private final Optional<Float> temperature;
    private final Optional<Float> topP;
    private final String modelName;
    private final Optional<String> endpoint;
    private final BedrockRuntimeClient client;
    private final BedrockRuntimeAsyncClient asyncClient;
    private final boolean isToolStreamingSupported;
    private final boolean isPromptCachingSupported;

    public AwsBedrockLanguageModelClient(
            String modelName,
            Optional<String> endpoint,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            PromptDao promptDao,
            Executor executor,
            int batchParallelism,
            BedrockRuntimeClient client,
            BedrockRuntimeAsyncClient asyncClient,
            boolean isToolStreamingSupported,
            boolean isPromptCachingSupported,
            TokenUsageListener tokenUsageListener)
    {
        super(promptDao, executor, batchParallelism, tokenUsageListener);
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.client = requireNonNull(client, "client is null");
        this.asyncClient = requireNonNull(asyncClient, "asyncClient is null");
        this.isToolStreamingSupported = isToolStreamingSupported;
        this.isPromptCachingSupported = isPromptCachingSupported;
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, String prompt, TokenUsageContext context)
    {
        return generateCompletion(systemPrompts, ImmutableList.of(new LlmMessage(USER, Optional.of(prompt), ImmutableList.of(), ImmutableList.of())), context);
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> messages, TokenUsageContext context)
    {
        List<SystemContentBlock> systemContentBlocks = systemPrompts.stream()
                .map(SystemContentBlock::fromText)
                .collect(toImmutableList());

        ConverseResponse response = getConverseResponse(
                modelName,
                () -> client.converse(request -> initializeConverseRequestBuilder(
                        request,
                        systemContentBlocks,
                        modelName,
                        messages,
                        ImmutableList.of())),
                context);

        if (response.stopReason() != null && (ERROR_STOP_REASONS.contains(response.stopReason()) || response.stopReason() == StopReason.TOOL_USE)) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + response.stopReasonAsString());
        }

        log.debug("Bedrock token usage: %s", response.usage());
        return parseBedrockToolResponse(response).textResponse();
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools,
            TokenUsageContext context)
    {
        List<SystemContentBlock> systemContentBlocks = systemPrompts.stream()
                .map(SystemContentBlock::fromText)
                .collect(toImmutableList());

        List<Tool> bedrockTools = tools.stream()
                .map(this::toBedrockTool)
                .collect(toImmutableList());

        ConverseResponse response = getConverseResponse(
                modelName,
                () -> client.converse(request -> initializeConverseRequestBuilder(
                        request,
                        systemContentBlocks,
                        modelName,
                        messages,
                        bedrockTools)),
                context);
        if (response.stopReason() != null && ERROR_STOP_REASONS.contains(response.stopReason())) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + response.stopReasonAsString());
        }

        log.debug("Bedrock token usage: %s", response.usage());
        return parseBedrockToolResponse(response);
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools,
            Consumer<String> output,
            Supplier<Boolean> isCancelled,
            TokenUsageContext context)
    {
        if (!isToolStreamingSupported) {
            ToolUseResponse response = generateCompletionWithTools(systemPrompts, messages, tools, context);
            output.accept(response.textResponse());
            return response;
        }
        List<SystemContentBlock> systemContentBlocks = systemPrompts.stream()
                .map(SystemContentBlock::fromText)
                .collect(toImmutableList());

        List<Tool> bedrockTools = tools.stream()
                .map(this::toBedrockTool)
                .collect(toImmutableList());

        ConverseResponse response = getConverseResponse(
                modelName,
                () -> {
                    ConverseResponse.Builder builder = ConverseResponse.builder();

                    Message.Builder messageBuilder = Message.builder()
                            .role(ConversationRole.ASSISTANT);
                    StreamResponseVisitor visitor = new StreamResponseVisitor(output, builder, isCancelled);
                    ConverseStreamResponseHandler responseStreamHandler = ConverseStreamResponseHandler.builder()
                            .subscriber(visitor)
                            .build();
                    try {
                        asyncClient.converseStream(
                                request -> initializeConverseStreamRequestBuilder(
                                        request,
                                        systemContentBlocks,
                                        modelName,
                                        messages,
                                        bedrockTools),
                                responseStreamHandler).get();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TrinoException(AI_CLIENT_ERROR, "Streaming was interrupted", e);
                    }
                    catch (ExecutionException e) {
                        if (isCancelled.get()) {
                            List<ContentBlock> contentBlocks = visitor.getContentBlocks();
                            return builder
                                    .output(v -> v.message(messageBuilder.content(contentBlocks).build()))
                                    .build();
                        }
                        if (e.getCause() instanceof RuntimeException) {
                            throw toTrinoException((RuntimeException) e.getCause());
                        }
                        throw new TrinoException(AI_CLIENT_ERROR, "Failed to stream response from Bedrock model", e);
                    }
                    List<ContentBlock> contentBlocks = visitor.getContentBlocks();
                    return builder
                            .output(v -> v.message(messageBuilder.content(contentBlocks).build()))
                            .build();
                },
                context);
        if (response.stopReason() != null && ERROR_STOP_REASONS.contains(response.stopReason())) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + response.stopReasonAsString());
        }

        log.debug("Bedrock token usage: %s", response.usage());
        return parseBedrockToolResponse(response);
    }

    private ConverseResponse getConverseResponse(
            String modelName,
            Supplier<ConverseResponse> getBedrockResponse,
            TokenUsageContext context)
    {
        try {
            ConverseResponse response = getBedrockResponse.get();
            reportTokenUsage(context, new TokenUsage(
                    usageField(response, usage -> usage.inputTokens()),
                    usageField(response, usage -> usage.outputTokens()),
                    usageField(response, usage -> usage.cacheReadInputTokens()),
                    usageField(response, usage -> usage.cacheWriteInputTokens()),
                    0L,
                    modelName,
                    endpoint,
                    ModelType.LANGUAGE,
                    ModelBackend.AWS_BEDROCK));
            return response;
        }
        catch (RuntimeException e) {
            throw toTrinoException(e);
        }
    }

    private static int usageField(ConverseResponse response, Function<software.amazon.awssdk.services.bedrockruntime.model.TokenUsage, Integer> extractor)
    {
        return Optional.ofNullable(response.usage()).flatMap(usage -> Optional.ofNullable(extractor.apply(usage))).orElse(0);
    }

    private static TrinoException toTrinoException(RuntimeException ex)
    {
        return switch (ex) {
            case AccessDeniedException e -> new TrinoException(PERMISSION_DENIED, "Bedrock access denied", e);
            case ResourceNotFoundException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock model not found", e);
            case ValidationException e -> new TrinoException(INVALID_MODEL_CONFIGURATION, "Bedrock request failed validation", e);
            case ServiceQuotaExceededException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock service quota exceeded", e);
            case ModelNotReadyException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock model not ready", e);
            case ModelErrorException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock model error", e);
            case StsException e -> new TrinoException(AI_CLIENT_ERROR, "AWS STS error occurred", e);
            case ThrottlingException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock request was throttled", e);
            case ModelTimeoutException e -> new TrinoException(AI_CLIENT_ERROR, "Request to Bedrock model timed out", e);
            case ServiceUnavailableException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock service unavailable", e);
            case InternalServerException e -> new TrinoException(AI_CLIENT_ERROR, "Bedrock internal server error", e);
            case TrinoException e -> e;
            default -> new TrinoException(AI_CLIENT_ERROR, "Failed to execute AI request", ex);
        };
    }

    private record PreparedRequest(List<SystemContentBlock> systemBlocks, List<Message> messages) {}

    private PreparedRequest prepareRequest(List<SystemContentBlock> systemContentBlocks, List<LlmMessage> messages)
    {
        List<SystemContentBlock> systemBlocks = addSystemCachePoint(systemContentBlocks, isPromptCachingSupported);
        return new PreparedRequest(systemBlocks, buildMessagesWithCachePoint(messages, systemBlocks, isPromptCachingSupported));
    }

    private void initializeConverseRequestBuilder(
            ConverseRequest.Builder request,
            List<SystemContentBlock> systemContentBlocks,
            String modelName,
            List<LlmMessage> messages,
            List<Tool> bedrockTools)
    {
        PreparedRequest prepared = prepareRequest(systemContentBlocks, messages);
        if (!prepared.systemBlocks().isEmpty()) {
            request.system(prepared.systemBlocks());
        }
        request
                .modelId(modelName)
                .messages(prepared.messages())
                .inferenceConfig(config -> config
                        .maxTokens(maxTokens.orElse(null))
                        .temperature(temperature.orElse(null))
                        .topP(topP.orElse(null)));
        if (!bedrockTools.isEmpty()) {
            request.toolConfig(config -> config.tools(bedrockTools));
        }
    }

    private void initializeConverseStreamRequestBuilder(
            ConverseStreamRequest.Builder request,
            List<SystemContentBlock> systemContentBlocks,
            String modelName,
            List<LlmMessage> messages,
            List<Tool> bedrockTools)
    {
        PreparedRequest prepared = prepareRequest(systemContentBlocks, messages);
        if (!prepared.systemBlocks().isEmpty()) {
            request.system(prepared.systemBlocks());
        }
        request
                .modelId(modelName)
                .messages(prepared.messages())
                .inferenceConfig(config -> config
                        .maxTokens(maxTokens.orElse(null))
                        .temperature(temperature.orElse(null))
                        .topP(topP.orElse(null)));
        if (!bedrockTools.isEmpty()) {
            request.toolConfig(config -> config.tools(bedrockTools));
        }
    }

    static List<SystemContentBlock> addSystemCachePoint(
            List<SystemContentBlock> systemContentBlocks,
            boolean isPromptCachingSupported)
    {
        if (!isPromptCachingSupported) {
            return systemContentBlocks;
        }

        int totalChars = systemContentBlocks.stream()
                .mapToInt(block -> block.text() != null ? block.text().length() : 0)
                .sum();
        if (totalChars < MIN_CACHE_POINT_CHARS) {
            return systemContentBlocks;
        }
        return ImmutableList.<SystemContentBlock>builder()
                .addAll(systemContentBlocks)
                .add(SystemContentBlock.fromCachePoint(
                        CachePointBlock.builder().type(CachePointType.DEFAULT).build()))
                .build();
    }

    static int messageChars(LlmMessage message)
    {
        return switch (message.role()) {
            case USER -> message.content().orElseThrow().length();
            case TOOL_RESPONSE -> message.toolResponse().stream().mapToInt(toolResponse -> toolResponse.responseJson().toString().length() + toolResponse.toolUseId().length()).sum();
            case ASSISTANT -> message.content().orElse("").length()
                    + message.toolCalls().stream().mapToInt(toolCall -> toolCall.name().length() + toolCall.input().toString().length() + toolCall.id().length()).sum();
        };
    }

    /**
     * In order to make use of Bedrock's caching, a cache point in the current prompt must match
     * the exact token sequence for which a cache point was previously written. We use two sliding
     * cache points (CP2 and CP3), in addition to the fixed system one (CP1), to ensure that we get
     * cache hits across turns while still advancing the cache point positions as the conversation grows.
     * <p>
     * The idea is to have stable cache point positions across turns: when a new candidate appears,
     * CP2 takes CP3's former position (cache hit — it was written there last turn) and CP3 moves
     * to the new message (cache write). Between qualifying turns both positions are unchanged,
     * so both hit.
     */
    static List<Message> buildMessagesWithCachePoint(
            List<LlmMessage> messages,
            List<SystemContentBlock> systemBlocks,
            boolean isPromptCachingSupported)
    {
        if (messages.isEmpty()) {
            return ImmutableList.of();
        }

        int cp2Index = -1;
        int cp3Index = -1;

        if (isPromptCachingSupported) {
            int systemPromptChars = systemBlocks.stream()
                    .mapToInt(block -> block.text() != null ? block.text().length() : 0)
                    .sum();
            int charsAtCachePoint = systemPromptChars >= MIN_CACHE_POINT_CHARS ? systemPromptChars : 0;
            List<Integer> candidateIndexes = new ArrayList<>();
            int running = systemPromptChars;
            for (int i = 0; i < messages.size(); i++) {
                running += messageChars(messages.get(i));
                if (running > MAX_CACHE_POINT_CHARS) {
                    break;
                }
                if (running - charsAtCachePoint < MIN_CACHE_POINT_CHARS) {
                    continue;
                }
                if (messages.get(i).role() == USER || messages.get(i).role() == TOOL_RESPONSE) {
                    candidateIndexes.add(i);
                    charsAtCachePoint = running;
                }
            }
            if (candidateIndexes.size() > 1) {
                cp2Index = candidateIndexes.get(candidateIndexes.size() - 2);
                cp3Index = candidateIndexes.get(candidateIndexes.size() - 1);
            }
            else if (candidateIndexes.size() == 1) {
                cp2Index = candidateIndexes.getLast();
            }
        }

        // Build message list
        ImmutableList.Builder<Message> result = ImmutableList.builder();
        for (int i = 0; i < messages.size(); i++) {
            LlmMessage message = messages.get(i);
            ImmutableList.Builder<ContentBlock> bedrockContentBlockBuilder = ImmutableList.builder();
            switch (message.role()) {
                case TOOL_RESPONSE -> {
                    for (LlmMessage.ToolResponse toolResponse : message.toolResponse()) {
                        ToolResultContentBlock contentBlock = ToolResultContentBlock.builder()
                                .json(jsonNodeToDocument(toolResponse.responseJson()))
                                .build();
                        ToolResultBlock toolResultBlock = ToolResultBlock.builder()
                                .toolUseId(toolResponse.toolUseId())
                                .content(contentBlock)
                                .build();
                        bedrockContentBlockBuilder.add(ContentBlock.fromToolResult(toolResultBlock));
                    }
                }
                case USER -> bedrockContentBlockBuilder.add(ContentBlock.fromText(message.content().orElseThrow()));
                case ASSISTANT -> {
                    message.content().ifPresent(contentBlock -> bedrockContentBlockBuilder.add(ContentBlock.fromText(contentBlock)));
                    for (ToolUseResponse.ToolCall toolCall : message.toolCalls()) {
                        bedrockContentBlockBuilder.add(ContentBlock.fromToolUse(ToolUseBlock.builder()
                                .toolUseId(toolCall.id())
                                .name(toolCall.name())
                                .input(jsonNodeToDocument(toolCall.input()))
                                .build()));
                    }
                }
            }

            if (i == cp2Index || i == cp3Index) {
                bedrockContentBlockBuilder.add(ContentBlock.fromCachePoint(CachePointBlock.builder().type(CachePointType.DEFAULT).build()));
            }
            result.add(Message.builder()
                    .role(toConversationRole(message))
                    .content(bedrockContentBlockBuilder.build()).build());
        }
        return result.build();
    }

    private Tool toBedrockTool(ToolDefinition<?> toolDef)
    {
        return Tool.builder()
                .toolSpec(ToolSpecification.builder()
                        .name(toolDef.getName())
                        .description(toolDef.getDescription())
                        .inputSchema(ToolInputSchema.builder().json(jsonNodeToDocument(toolDef.getInputSchema())).build())
                        .build())
                .build();
    }

    private static Document jsonNodeToDocument(JsonNode node)
    {
        if (node == null || node.isNull()) {
            return Document.fromNull();
        }
        else if (node.isObject()) {
            Document.MapBuilder mapBuilder = Document.mapBuilder();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                mapBuilder.putDocument(entry.getKey(), jsonNodeToDocument(entry.getValue()));
            }
            return mapBuilder.build();
        }
        else if (node.isArray()) {
            ImmutableList.Builder<Document> documentList = ImmutableList.builder();
            for (JsonNode element : node) {
                documentList.add(jsonNodeToDocument(element));
            }
            return Document.fromList(documentList.build());
        }
        else if (node.isBoolean()) {
            return Document.fromBoolean(node.asBoolean());
        }
        else if (node.isIntegralNumber()) {
            return Document.fromNumber(node.asLong());
        }
        else if (node.isNumber()) {
            return Document.fromNumber(node.asDouble());
        }
        else if (node.isTextual()) {
            return Document.fromString(node.asText());
        }
        else {
            // Fallback for any other types
            return Document.fromString(node.toString());
        }
    }

    private static ToolUseResponse parseBedrockToolResponse(ConverseResponse response)
    {
        StringBuilder textResponse = new StringBuilder();
        ImmutableList.Builder<ToolUseResponse.ToolCall> toolCallBuilder = ImmutableList.builder();

        for (ContentBlock block : response.output().message().content()) {
            if (block.text() != null) {
                textResponse.append(block.text());
            }
            else if (block.toolUse() != null) {
                ToolUseBlock toolUse = block.toolUse();
                try {
                    JsonNode inputNode = documentToJsonNode(toolUse.input());
                    toolCallBuilder.add(new ToolUseResponse.ToolCall(
                            toolUse.toolUseId(),
                            toolUse.name(),
                            inputNode));
                }
                catch (Exception e) {
                    throw new TrinoException(AI_CLIENT_ERROR, "Failed to parse tool use input", e);
                }
            }
        }

        return new ToolUseResponse(textResponse.toString(), toolCallBuilder.build());
    }

    private static JsonNode documentToJsonNode(Document document)
    {
        if (document == null) {
            return OBJECT_MAPPER.nullNode();
        }

        if (document.isNull()) {
            return OBJECT_MAPPER.nullNode();
        }
        else if (document.isBoolean()) {
            return OBJECT_MAPPER.valueToTree(document.asBoolean());
        }
        else if (document.isNumber()) {
            return OBJECT_MAPPER.valueToTree(document.asNumber());
        }
        else if (document.isString()) {
            return OBJECT_MAPPER.valueToTree(document.asString());
        }
        else if (document.isList()) {
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (Document item : document.asList()) {
                arrayNode.add(documentToJsonNode(item));
            }
            return arrayNode;
        }
        else if (document.isMap()) {
            ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
            document.asMap().forEach((key, value) ->
                    objectNode.set(key, documentToJsonNode(value)));
            return objectNode;
        }
        else {
            throw new TrinoException(AI_CLIENT_ERROR, "Unknown AWS Document type: " + document.getClass());
        }
    }

    private static ConversationRole toConversationRole(LlmMessage message)
    {
        return switch (message.role()) {
            case USER, TOOL_RESPONSE -> ConversationRole.USER;
            case ASSISTANT -> ConversationRole.ASSISTANT;
        };
    }

    private static class StreamResponseVisitor
            implements ConverseStreamResponseHandler.Visitor
    {
        private final Consumer<String> output;
        private final StringBuilder responseChunksText;
        private final List<ToolUseBlock> responseChunksTools;
        private final StringBuilder currentToolArgs;
        private final ConverseResponse.Builder responseBuilder;
        private final Supplier<Boolean> isCancelled;

        private String currentToolName;
        private String currentToolUseId;

        public StreamResponseVisitor(Consumer<String> output, ConverseResponse.Builder responseBuilder, Supplier<Boolean> isCancelled)
        {
            this.output = output;
            this.responseBuilder = responseBuilder;
            this.isCancelled = isCancelled;
            this.responseChunksText = new StringBuilder();
            this.responseChunksTools = new ArrayList<>();
            this.currentToolArgs = new StringBuilder();
        }

        @Override
        public void visitContentBlockStart(ContentBlockStartEvent chunk)
        {
            ToolUseBlockStart toolUse = chunk.start().toolUse();
            if (toolUse != null) {
                if (currentToolName != null || currentToolUseId != null) {
                    log.warn("Starting new tool block with incomplete previous tool data");
                }
                currentToolName = toolUse.name();
                currentToolUseId = toolUse.toolUseId();
            }
        }

        @Override
        public void visitContentBlockStop(ContentBlockStopEvent event)
        {
            if (currentToolName != null && currentToolUseId != null) {
                try {
                    String toolArgsJson = currentToolArgs.toString();
                    Document document;
                    if (toolArgsJson.isBlank()) {
                        document = Document.fromNull();
                    }
                    else {
                        software.amazon.awssdk.protocols.jsoncore.JsonNode node =
                                software.amazon.awssdk.protocols.jsoncore.JsonNode.parser().parse(toolArgsJson);
                        document = node.visit(new DocumentUnmarshaller());
                    }
                    responseChunksTools.add(
                            ToolUseBlock.builder()
                                    .name(currentToolName)
                                    .toolUseId(currentToolUseId)
                                    .input(document)
                                    .build());
                }
                catch (Exception e) {
                    log.error(e, "Error parsing tool input JSON");
                    throw new TrinoException(AI_CLIENT_ERROR, "Failed to parse tool use input", e);
                }
                finally {
                    currentToolName = null;
                    currentToolUseId = null;
                    currentToolArgs.setLength(0);
                }
            }
        }

        @Override
        public void visitContentBlockDelta(ContentBlockDeltaEvent chunk)
        {
            if (isCancelled.get()) {
                throw new RuntimeException("Chat cancelled");
            }
            ToolUseBlockDelta toolUse = chunk.delta().toolUse();
            if (toolUse != null) {
                currentToolArgs.append(toolUse.input());
            }
            String text = chunk.delta().text();
            if (text != null) {
                output.accept(text);
                responseChunksText.append(text);
            }
        }

        @Override
        public void visitMetadata(ConverseStreamMetadataEvent metadata)
        {
            responseBuilder.usage(metadata.usage());
        }

        @Override
        public void visitMessageStop(MessageStopEvent stop)
        {
            responseBuilder.stopReason(stop.stopReason());
        }

        public List<ContentBlock> getContentBlocks()
        {
            List<ContentBlock> contentBlocks = new ArrayList<>();
            if (!responseChunksText.isEmpty()) {
                contentBlocks.add(ContentBlock.fromText(responseChunksText.toString()));
            }
            responseChunksTools.stream()
                    .map(ContentBlock::fromToolUse)
                    .forEach(contentBlocks::add);
            return contentBlocks;
        }
    }
}
