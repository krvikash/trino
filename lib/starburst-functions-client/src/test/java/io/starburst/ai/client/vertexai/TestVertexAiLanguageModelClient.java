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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genai.types.Candidate;
import com.google.genai.types.Content;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.Part;
import com.google.genai.types.Schema;
import com.google.genai.types.Tool;
import io.starburst.ai.client.InternalToolDefinition;
import io.starburst.ai.client.JsonSchemaParameterType;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.ModelBackend;
import io.starburst.ai.client.ModelType;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.ToolDefinition;
import io.starburst.ai.client.ToolParameter;
import io.starburst.ai.client.ToolResult;
import io.starburst.ai.client.ToolUseResponse;
import io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.StreamedContent;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.accumulateStream;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.buildConfig;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.parseToolResponse;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toContents;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toSchema;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toTokenUsage;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toToolCall;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.toTools;
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
                new LlmMessage(USER, Optional.of("hello"), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(ASSISTANT, Optional.of("hi there"), ImmutableList.of(), ImmutableList.of())));

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
    void testToSchemaMapsObjectSchema()
    {
        ToolDefinition<String> tool = new FixtureTool();

        Schema schema = toSchema(tool.getInputSchema());

        assertThat(schema.type().orElseThrow().toString()).isEqualTo("OBJECT");
        assertThat(schema.properties().orElseThrow()).containsKeys("operation", "value");
        assertThat(schema.required().orElseThrow()).containsExactly("operation");

        Schema operation = schema.properties().orElseThrow().get("operation");
        assertThat(operation.type().orElseThrow().toString()).isEqualTo("STRING");
        assertThat(operation.description()).contains("the operation to run");
        assertThat(operation.enum_().orElseThrow()).containsExactly("add", "subtract");

        Schema value = schema.properties().orElseThrow().get("value");
        assertThat(value.type().orElseThrow().toString()).isEqualTo("INTEGER");
    }

    @Test
    void testToSchemaMapsNestedArrayAndObjectSchemas()
    {
        ToolDefinition<String> tool = new NestedFixtureTool();

        Schema schema = toSchema(tool.getInputSchema());
        Schema usersSchema = schema.properties().orElseThrow().get("users");

        assertThat(usersSchema.type().orElseThrow().toString()).isEqualTo("ARRAY");
        Schema userItemSchema = usersSchema.items().orElseThrow();
        assertThat(userItemSchema.type().orElseThrow().toString()).isEqualTo("OBJECT");
        assertThat(userItemSchema.properties().orElseThrow()).containsKeys("name", "age");
        assertThat(userItemSchema.required().orElseThrow()).containsExactly("name");
    }

    @Test
    void testToToolsBuildsFunctionDeclarations()
    {
        List<Tool> tools = toTools(ImmutableList.of(new FixtureTool()));

        assertThat(tools).hasSize(1);
        List<FunctionDeclaration> declarations = tools.get(0).functionDeclarations().orElseThrow();
        assertThat(declarations).hasSize(1);
        FunctionDeclaration declaration = declarations.get(0);
        assertThat(declaration.name()).contains("fixture");
        assertThat(declaration.description()).contains("a fixture tool");
        assertThat(declaration.parameters().orElseThrow().type().orElseThrow().toString()).isEqualTo("OBJECT");
    }

    @Test
    void testToToolsBuildsMultipleFunctionDeclarations()
    {
        List<Tool> tools = toTools(ImmutableList.of(new FixtureTool(), new NestedFixtureTool()));

        assertThat(tools).hasSize(1);
        List<FunctionDeclaration> declarations = tools.get(0).functionDeclarations().orElseThrow();
        assertThat(declarations).hasSize(2);
        assertThat(declarations.stream().map(FunctionDeclaration::name)).containsExactlyInAnyOrder(Optional.of("fixture"), Optional.of("create_users"));
    }

    @Test
    void testToToolsEmptyReturnsEmptyList()
    {
        assertThat(toTools(ImmutableList.of())).isEmpty();
    }

    @Test
    void testBuildConfigSetsToolsWhenPresent()
    {
        List<Tool> tools = toTools(ImmutableList.of(new FixtureTool()));

        GenerateContentConfig config = buildConfig(ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), tools);

        assertThat(config.tools().orElseThrow()).isEqualTo(tools);
    }

    @Test
    void testToToolCallMapsNameAndArgs()
    {
        FunctionCall functionCall = FunctionCall.builder()
                .id("call-1")
                .name("fixture")
                .args(ImmutableMap.of("operation", "add", "value", 7))
                .build();

        ToolUseResponse.ToolCall toolCall = toToolCall(functionCall, Optional.empty());

        assertThat(toolCall.id()).isEqualTo("call-1");
        assertThat(toolCall.name()).isEqualTo("fixture");
        assertThat(toolCall.input().get("operation").asText()).isEqualTo("add");
        assertThat(toolCall.input().get("value").asInt()).isEqualTo(7);
    }

    @Test
    void testToToolCallFallsBackToNameWhenIdAbsent()
    {
        FunctionCall functionCall = FunctionCall.builder()
                .name("fixture")
                .build();

        ToolUseResponse.ToolCall toolCall = toToolCall(functionCall, Optional.empty());

        assertThat(toolCall.id()).isEqualTo("fixture");
        assertThat(toolCall.name()).isEqualTo("fixture");
        assertThat(toolCall.input().isObject()).isTrue();
        assertThat(toolCall.input()).isEmpty();
    }

    @Test
    void testToToolCallThrowsWhenNameAbsent()
    {
        FunctionCall functionCall = FunctionCall.builder().build();

        assertThatThrownBy(() -> toToolCall(functionCall, Optional.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("missing a name");
    }

    @Test
    void testParseToolResponseKeepsThoughtSignaturePerFunctionCallPart()
    {
        byte[] signatureOne = "sig-one".getBytes(StandardCharsets.UTF_8);
        byte[] signatureTwo = "sig-two".getBytes(StandardCharsets.UTF_8);
        GenerateContentResponse response = geminiResponseWithParallelToolCalls("thinking...", signatureOne, signatureTwo);

        ToolUseResponse parsed = parseToolResponse(response);

        assertThat(parsed.textResponse()).isEqualTo("thinking...");
        assertThat(parsed.toolCalls()).hasSize(2);
        assertThat(parsed.toolCalls().getFirst().thoughtSignature()).isEqualTo(Optional.of(Base64.getEncoder().encodeToString(signatureOne)));
        assertThat(parsed.toolCalls().get(1).thoughtSignature()).isEqualTo(Optional.of(Base64.getEncoder().encodeToString(signatureTwo)));
    }

    @Test
    void testToContentsReEmitsThoughtSignatureOnFunctionCallPart()
    {
        byte[] signature = "gemini-thought-signature".getBytes(StandardCharsets.UTF_8);
        ToolUseResponse.ToolCall toolCall = new ToolUseResponse.ToolCall(
                "call-1",
                "fixture",
                JsonNodeFactory.instance.objectNode().put("operation", "add"),
                Optional.of(Base64.getEncoder().encodeToString(signature)));
        LlmMessage assistant = new LlmMessage(ASSISTANT, Optional.empty(), ImmutableList.of(), ImmutableList.of(toolCall));

        List<Content> contents = toContents(ImmutableList.of(assistant));

        Part functionCallPart = onlyFunctionCallPart(contents.getFirst());
        assertThat(functionCallPart.thoughtSignature()).isPresent();
        assertThat(functionCallPart.thoughtSignature().orElseThrow()).isEqualTo(signature);
    }

    /**
     * Covers the first function-call part of a step that carries no real signature: an app-synthesized
     * ToolRequest or one persisted before thoughtSignature existed. Gemini validates the first call of
     * each step, so the documented skip sentinel must go out rather than an absent thoughtSignature.
     */
    @Test
    void testToContentsFallsBackToSkipSentinelWhenThoughtSignatureAbsent()
    {
        ToolUseResponse.ToolCall toolCall = new ToolUseResponse.ToolCall(
                "call-1",
                "fixture",
                JsonNodeFactory.instance.objectNode().put("operation", "add"),
                Optional.empty());
        LlmMessage assistant = new LlmMessage(ASSISTANT, Optional.empty(), ImmutableList.of(), ImmutableList.of(toolCall));

        List<Content> contents = toContents(ImmutableList.of(assistant));

        Part functionCallPart = onlyFunctionCallPart(contents.getFirst());
        assertThat(functionCallPart.thoughtSignature()).isPresent();
        assertThat(new String(functionCallPart.thoughtSignature().orElseThrow(), StandardCharsets.UTF_8)).isEqualTo("skip_thought_signature_validator");
    }

    /**
     * Gemini attaches a thought signature only to the first call of a parallel group. A later unsigned
     * call is not app-synthesized, so it must be replayed with no signature (not the skip sentinel) to
     * match what the model returned, while the signed first call still carries its signature.
     */
    @Test
    void testToContentsOmitsSignatureForLaterUnsignedParallelCall()
    {
        byte[] signature = "gemini-thought-signature".getBytes(StandardCharsets.UTF_8);
        ToolUseResponse.ToolCall signedCall = new ToolUseResponse.ToolCall(
                "call-1",
                "fixture",
                JsonNodeFactory.instance.objectNode().put("operation", "add"),
                Optional.of(Base64.getEncoder().encodeToString(signature)));
        ToolUseResponse.ToolCall unsignedCall = new ToolUseResponse.ToolCall(
                "call-2",
                "fixture",
                JsonNodeFactory.instance.objectNode().put("operation", "subtract"),
                Optional.empty());
        LlmMessage assistant = new LlmMessage(ASSISTANT, Optional.empty(), ImmutableList.of(), ImmutableList.of(signedCall, unsignedCall));

        List<Content> contents = toContents(ImmutableList.of(assistant));

        List<Part> functionCallParts = functionCallParts(contents.getFirst());
        assertThat(functionCallParts).hasSize(2);
        assertThat(functionCallParts.getFirst().thoughtSignature()).isPresent();
        assertThat(functionCallParts.getFirst().thoughtSignature().orElseThrow()).isEqualTo(signature);
        assertThat(functionCallParts.get(1).thoughtSignature()).isEmpty();
    }

    @Test
    void testAccumulateStreamForwardsEachChunkAndJoinsText()
    {
        List<GenerateContentResponse> chunks = ImmutableList.of(
                textChunk("Hello"),
                textChunk(", "),
                textChunk("world"));
        ImmutableList.Builder<String> streamed = ImmutableList.builder();

        StreamedContent content = accumulateStream(chunks, streamed::add, () -> false);

        assertThat(streamed.build()).containsExactly("Hello", ", ", "world");
        assertThat(content.response().textResponse()).isEqualTo("Hello, world");
        assertThat(content.response().toolCalls()).isEmpty();
    }

    @Test
    void testAccumulateStreamSkipsEmptyChunks()
    {
        List<GenerateContentResponse> chunks = ImmutableList.of(
                textChunk("Hello"),
                GenerateContentResponse.builder().build(),
                textChunk(" world"));
        ImmutableList.Builder<String> streamed = ImmutableList.builder();

        StreamedContent content = accumulateStream(chunks, streamed::add, () -> false);

        assertThat(streamed.build()).containsExactly("Hello", " world");
        assertThat(content.response().textResponse()).isEqualTo("Hello world");
    }

    /**
     * Gemini streams a function call in a chunk of its own, with the thought signature on the same part.
     * The signature must survive accumulation, since the follow-up request 400s without it.
     */
    @Test
    void testAccumulateStreamCollectsToolCallsWithThoughtSignatures()
    {
        byte[] signature = "sig-one".getBytes(StandardCharsets.UTF_8);
        List<GenerateContentResponse> chunks = ImmutableList.of(
                textChunk("Let me check."),
                GenerateContentResponse.builder()
                        .candidates(Candidate.builder().content(Content.builder()
                                .role("model")
                                .parts(ImmutableList.of(Part.builder()
                                        .functionCall(FunctionCall.builder().id("c1").name("fixture").args(ImmutableMap.of("operation", "add")))
                                        .thoughtSignature(signature)
                                        .build()))))
                        .build());
        ImmutableList.Builder<String> streamed = ImmutableList.builder();

        StreamedContent content = accumulateStream(chunks, streamed::add, () -> false);

        assertThat(streamed.build()).containsExactly("Let me check.");
        assertThat(content.response().textResponse()).isEqualTo("Let me check.");
        assertThat(content.response().toolCalls()).hasSize(1);
        ToolUseResponse.ToolCall toolCall = content.response().toolCalls().getFirst();
        assertThat(toolCall.name()).isEqualTo("fixture");
        assertThat(toolCall.thoughtSignature()).isEqualTo(Optional.of(Base64.getEncoder().encodeToString(signature)));
    }

    /**
     * Vertex AI restates the cumulative usage on every chunk, so only the last one reports the totals.
     */
    @Test
    void testAccumulateStreamKeepsUsageFromLastChunkThatCarriesIt()
    {
        List<GenerateContentResponse> chunks = ImmutableList.of(
                usageChunk("Hello", 12, 1),
                usageChunk(" world", 12, 4),
                textChunk("!"));

        StreamedContent content = accumulateStream(chunks, _ -> {}, () -> false);

        assertThat(content.response().textResponse()).isEqualTo("Hello world!");
        assertThat(content.usageMetadata()).isPresent();
        assertThat(content.usageMetadata().orElseThrow().promptTokenCount()).contains(12);
        assertThat(content.usageMetadata().orElseThrow().candidatesTokenCount()).contains(4);
    }

    @Test
    void testAccumulateStreamWithoutUsageMetadata()
    {
        StreamedContent content = accumulateStream(ImmutableList.of(textChunk("Hello")), _ -> {}, () -> false);

        assertThat(content.usageMetadata()).isEmpty();
    }

    /**
     * Cancelling stops consuming the stream, and the returned text stays consistent with what the caller
     * already received through the output consumer.
     */
    @Test
    void testAccumulateStreamStopsOnCancellation()
    {
        List<GenerateContentResponse> chunks = ImmutableList.of(
                textChunk("Hello"),
                textChunk(" world"),
                textChunk(" again"));
        ImmutableList.Builder<String> streamed = ImmutableList.builder();
        AtomicInteger consumed = new AtomicInteger();

        StreamedContent content = accumulateStream(chunks, streamed::add, () -> consumed.getAndIncrement() >= 2);

        assertThat(streamed.build()).containsExactly("Hello", " world");
        assertThat(content.response().textResponse()).isEqualTo("Hello world");
    }

    @Test
    void testAccumulateStreamCancelledBeforeFirstChunk()
    {
        ImmutableList.Builder<String> streamed = ImmutableList.builder();

        StreamedContent content = accumulateStream(ImmutableList.of(textChunk("Hello")), streamed::add, () -> true);

        assertThat(streamed.build()).isEmpty();
        assertThat(content.response().textResponse()).isEmpty();
        assertThat(content.response().toolCalls()).isEmpty();
    }

    /**
     * Cancelling must stop pulling from the stream rather than draining it, so a chunk source that fails
     * when read past the cancellation point still yields a response.
     */
    @Test
    void testAccumulateStreamStopsPullingChunksOnCancellation()
    {
        AtomicInteger pulled = new AtomicInteger();
        Iterable<GenerateContentResponse> chunks = () -> new AbstractIterator<>()
        {
            @Override
            protected GenerateContentResponse computeNext()
            {
                int index = pulled.getAndIncrement();
                if (index >= 2) {
                    throw new AssertionError("Pulled chunk %s after cancellation".formatted(index));
                }
                return textChunk("chunk-" + index);
            }
        };

        StreamedContent content = accumulateStream(chunks, _ -> {}, () -> pulled.get() >= 2);

        assertThat(content.response().textResponse()).isEqualTo("chunk-0chunk-1");
        assertThat(pulled.get()).isEqualTo(2);
    }

    private static GenerateContentResponse textChunk(String text)
    {
        return GenerateContentResponse.builder()
                .candidates(Candidate.builder().content(Content.builder()
                        .role("model")
                        .parts(ImmutableList.of(Part.fromText(text)))))
                .build();
    }

    private static GenerateContentResponse usageChunk(String text, int promptTokens, int candidatesTokens)
    {
        return textChunk(text).toBuilder()
                .usageMetadata(GenerateContentResponseUsageMetadata.builder()
                        .promptTokenCount(promptTokens)
                        .candidatesTokenCount(candidatesTokens))
                .build();
    }

    private static GenerateContentResponse geminiResponseWithParallelToolCalls(String text, byte[] signatureOne, byte[] signatureTwo)
    {
        Content content = Content.builder()
                .role("model")
                .parts(ImmutableList.of(
                        Part.fromText(text),
                        Part.builder()
                                .functionCall(FunctionCall.builder().id("c1").name("fixture").args(ImmutableMap.of("operation", "add")))
                                .thoughtSignature(signatureOne)
                                .build(),
                        Part.builder()
                                .functionCall(FunctionCall.builder().id("c2").name("fixture").args(ImmutableMap.of("operation", "subtract")))
                                .thoughtSignature(signatureTwo)
                                .build()))
                .build();
        return GenerateContentResponse.builder()
                .candidates(Candidate.builder().content(content).build())
                .build();
    }

    private static Part onlyFunctionCallPart(Content content)
    {
        return content.parts().orElseThrow().stream()
                .filter(part -> part.functionCall().isPresent())
                .findFirst()
                .orElseThrow();
    }

    private static List<Part> functionCallParts(Content content)
    {
        return content.parts().orElseThrow().stream()
                .filter(part -> part.functionCall().isPresent())
                .collect(toImmutableList());
    }

    private static class FixtureTool
            extends InternalToolDefinition<String>
    {
        FixtureTool()
        {
            super("fixture",
                    "a fixture tool",
                    ImmutableList.of(
                            new ToolParameter("operation", JsonSchemaParameterType.STRING, "the operation to run", ImmutableList.of("add", "subtract"), true),
                            new ToolParameter("value", JsonSchemaParameterType.INTEGER, "the operand", ImmutableList.of(), false)));
        }

        @Override
        protected ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.success("ok");
        }
    }

    private static class NestedFixtureTool
            extends InternalToolDefinition<String>
    {
        NestedFixtureTool()
        {
            super("create_users",
                    "creates multiple users",
                    ImmutableList.of(
                            new ToolParameter(
                                    "users",
                                    JsonSchemaParameterType.ARRAY,
                                    "list of users",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(),
                                    Optional.of(
                                            new ToolParameter(
                                                    "user",
                                                    JsonSchemaParameterType.OBJECT,
                                                    "a single user",
                                                    ImmutableList.of(),
                                                    false,
                                                    ImmutableList.of(
                                                            new ToolParameter("name", JsonSchemaParameterType.STRING, "user name", true),
                                                            new ToolParameter("age", JsonSchemaParameterType.INTEGER, "user age", false)),
                                                    Optional.empty())))));
        }

        @Override
        protected ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.success("ok");
        }
    }
}
