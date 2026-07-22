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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genai.types.Content;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.vertexai.VertexAiLanguageModelClient.buildConfig;
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

        ToolUseResponse.ToolCall toolCall = toToolCall(functionCall);

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

        ToolUseResponse.ToolCall toolCall = toToolCall(functionCall);

        assertThat(toolCall.id()).isEqualTo("fixture");
        assertThat(toolCall.name()).isEqualTo("fixture");
        assertThat(toolCall.input().isObject()).isTrue();
        assertThat(toolCall.input()).isEmpty();
    }

    @Test
    void testToToolCallThrowsWhenNameAbsent()
    {
        FunctionCall functionCall = FunctionCall.builder().build();

        assertThatThrownBy(() -> toToolCall(functionCall))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("missing a name");
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
