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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.AbstractLanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.ToolDefinition;
import io.starburst.ai.client.ToolUseResponse;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.StopReason;
import software.amazon.awssdk.services.bedrockruntime.model.SystemContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.Tool;
import software.amazon.awssdk.services.bedrockruntime.model.ToolInputSchema;
import software.amazon.awssdk.services.bedrockruntime.model.ToolSpecification;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlock;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_SYSTEM;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiOperationNameIncubatingValues.CHAT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiSystemIncubatingValues.AWS_BEDROCK;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.MessageRole.USER;
import static java.util.Objects.requireNonNull;

public class AwsBedrockLanguageModelClient
        extends AbstractLanguageModelClient
{
    private static final Set<StopReason> ERROR_STOP_REASONS = ImmutableSet.of(
            StopReason.UNKNOWN_TO_SDK_VERSION,
            StopReason.GUARDRAIL_INTERVENED,
            StopReason.CONTENT_FILTERED,
            StopReason.MAX_TOKENS);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final Optional<Integer> maxTokens;
    private final Optional<Float> temperature;
    private final Optional<Float> topP;
    private final Tracer tracer;
    private final String modelName;
    private final BedrockRuntimeClient client;

    public AwsBedrockLanguageModelClient(
            String modelName,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            PromptDao promptDao,
            Executor executor,
            int batchParallelism,
            Tracer tracer,
            BedrockRuntimeClient client)
    {
        super(promptDao, executor, batchParallelism);
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, String prompt)
    {
        return generateCompletion(systemPrompts, ImmutableList.of(new LlmMessage(USER, prompt)));
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, List<LlmMessage> messages)
    {
        List<SystemContentBlock> systemContentBlocks = systemPrompts.stream()
                .map(SystemContentBlock::fromText)
                .collect(toImmutableList());

        ConverseResponse response = getConverseResponse(
                CHAT + " " + modelName,
                systemContentBlocks,
                modelName,
                messages,
                ImmutableList.of());

        List<ContentBlock> contentBlocks = response.output().message().content();
        if (response.stopReason() != null && (ERROR_STOP_REASONS.contains(response.stopReason()) || response.stopReason() == StopReason.TOOL_USE)) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + response.stopReasonAsString());
        }
        if (contentBlocks == null || contentBlocks.isEmpty() || response.output().message().content().getFirst().text() == null) {
            return "";
        }

        return response.output().message().content().getFirst().text();
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools)
    {
        List<SystemContentBlock> systemContentBlocks = systemPrompts.stream()
                .map(SystemContentBlock::fromText)
                .collect(toImmutableList());

        List<Tool> bedrockTools = tools.stream()
                .map(this::toBedrockTool)
                .collect(toImmutableList());

        ConverseResponse response = getConverseResponse(
                CHAT + " " + modelName + " (with tools)",
                systemContentBlocks,
                modelName,
                messages,
                bedrockTools);
        if (response.stopReason() != null && ERROR_STOP_REASONS.contains(response.stopReason())) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + response.stopReasonAsString());
        }

        return parseBedrockToolResponse(response);
    }

    private ConverseResponse getConverseResponse(
            String spanName,
            List<SystemContentBlock> systemContentBlocks,
            String modelName,
            List<LlmMessage> messages,
            List<Tool> bedrockTools)
    {
        Span span = tracer.spanBuilder(spanName)
                .setAttribute(GEN_AI_OPERATION_NAME, CHAT)
                .setAttribute(GEN_AI_SYSTEM, AWS_BEDROCK)
                .setAttribute(GEN_AI_REQUEST_MODEL, modelName)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (var _ = span.makeCurrent()) {
            ConverseResponse response = client.converse(request -> initializeConverseRequestBuilder(request,
                    systemContentBlocks,
                    modelName,
                    messages,
                    bedrockTools));
            span.setAttribute(GEN_AI_RESPONSE_MODEL, modelName);
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, Optional.ofNullable(response.usage().inputTokens()).orElse(0));
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, Optional.ofNullable(response.usage().outputTokens()).orElse(0));

            return response;
        }
        catch (RuntimeException e) {
            span.setStatus(ERROR, e.getMessage());
            span.recordException(e);
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute AI request", e);
        }
        finally {
            span.end();
        }
    }

    private void initializeConverseRequestBuilder(
            ConverseRequest.Builder request,
            List<SystemContentBlock> systemContentBlocks,
            String modelName,
            List<LlmMessage> messages,
            List<Tool> bedrockTools)
    {
        if (!systemContentBlocks.isEmpty()) {
            request.system(systemContentBlocks);
        }
        request
                .modelId(modelName)
                .messages(messages.stream()
                        .map(message -> Message.builder()
                                .role(toConversationRole(message))
                                .content(ContentBlock.fromText(message.content()))
                                .build())
                        .collect(toImmutableList()))
                .inferenceConfig(config -> config
                        .maxTokens(maxTokens.orElse(null))
                        .temperature(temperature.orElse(null))
                        .topP(topP.orElse(null)));
        if (!bedrockTools.isEmpty()) {
            request.toolConfig(config -> config.tools(bedrockTools));
        }
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
            case USER -> ConversationRole.USER;
            case ASSISTANT -> ConversationRole.ASSISTANT;
        };
    }
}
