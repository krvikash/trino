/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.JsonSchemaParameterType.INTEGER;
import static io.starburst.ai.client.JsonSchemaParameterType.NUMBER;
import static io.starburst.ai.client.JsonSchemaParameterType.STRING;
import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.TestingUtils.createLlmExecutor;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseTestToolUse
{
    private ScheduledExecutorService reloadingExecutor;
    private ExecutorService llmExecutor;
    private ModelClientProvider modelClientProvider;
    private static final Logger log = Logger.get(BaseTestToolUse.class);

    @BeforeAll
    public void setup()
            throws IOException
    {
        reloadingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
        llmExecutor = createLlmExecutor();

        modelClientProvider = staticModelClientProvider(getLanguageModelProviders(), reloadingExecutor, llmExecutor);
    }

    protected abstract String getLanguageModelProviders();

    @AfterAll
    public void cleanup()
    {
        reloadingExecutor.shutdownNow();
        llmExecutor.shutdownNow();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testSimpleToolCall(String modelId)
    {
        CalculatorTool tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "What is 25 + 37? Use the calculator tool."));

        ToolUseResponse response = modelClientProvider
                .languageModelClient(utf8Slice(modelId))
                .generateWithTools(
                        "You are a helpful assistant with access to a calculator.",
                        messages,
                        ImmutableList.of(tool));

        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }
        assertThat(response.toolCalls()).hasSize(1);

        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        assertThat(toolCall.name()).isEqualTo("calculator");
        assertThat(toolCall.input().has("operation")).isTrue();
        assertThat(toolCall.input().has("left_operand")).isTrue();
        assertThat(toolCall.input().has("right_operand")).isTrue();
        ToolResult<Double> toolResult = tool.execute(toolCall.input());
        assertThat(toolResult.content().orElseThrow()).isEqualTo(25d + 37d);
        assertThat(tool.formatResult(toolResult)).isEqualTo("62.0");
        assertThat(toolCall.id()).isNotEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMultipleTools(String modelId)
    {
        List<ToolDefinition<?>> tools = ImmutableList.of(
                new CalculatorTool(),
                new WeatherTool(),
                new SearchTool());

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "What's the weather like in Paris?"));

        ToolUseResponse response = modelClientProvider
                .languageModelClient(utf8Slice(modelId))
                .generateWithTools(
                        "You are a helpful assistant. Use the appropriate tool to answer questions.",
                        messages,
                        tools);
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.toolCalls()).isNotEmpty();

        // Should call the weather tool
        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        assertThat(toolCall.name()).isEqualTo("get_weather");
        assertThat(toolCall.input().has("location")).isTrue();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testToolCallWithConversationHistory(String modelId)
    {
        ToolDefinition<Double> tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "I need to do some math"),
                new LlmMessage(ASSISTANT, "I can use my calculator for that! What calculation do you need?"),
                new LlmMessage(USER, "What's 42 times 13?"));

        ToolUseResponse response = modelClientProvider
                .languageModelClient(utf8Slice(modelId))
                .generateWithTools(
                        "You are a helpful math assistant.",
                        messages,
                        ImmutableList.of(tool));
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.toolCalls()).isNotEmpty();
        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        ToolResult<Double> toolResult = tool.execute(toolCall.input());
        assertThat(toolResult.content()).contains(42d * 13d);
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testNoToolCallWhenNotNeeded(String modelId)
    {
        ToolDefinition<?> tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "Hello, how are you?"));

        ToolUseResponse response = modelClientProvider
                .languageModelClient(utf8Slice(modelId))
                .generateWithTools(
                        "You are a helpful assistant",
                        messages,
                        ImmutableList.of(tool));
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.textResponse()).isNotEmpty();
        assertThat(response.toolCalls()).isEmpty();
    }

    private static class CalculatorTool
            extends InternalToolDefinition<Double>
    {
        public CalculatorTool()
        {
            super(
                    "calculator",
                    "Performs basic arithmetic operations on two numbers",
                    ImmutableList.of(
                            new ToolParameter(
                                    "operation",
                                    STRING,
                                    "The operation to perform: add, subtract, multiply, or divide",
                                    ImmutableList.of("add", "subtract", "multiply", "divide"),
                                    true,
                                    ImmutableList.of(),
                                    Optional.empty()),
                            new ToolParameter(
                                    "left_operand",
                                    NUMBER,
                                    "The number to the left of the operator",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(),
                                    Optional.empty()),
                            new ToolParameter(
                                    "right_operand",
                                    NUMBER,
                                    "The number to the right of the operator",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(),
                                    Optional.empty())));
        }

        @Override
        public ToolResult<Double> executeInternal(JsonNode input)
        {
            String operation = input.get("operation").asText();
            return switch (operation) {
                case "add" -> ToolResult.success(
                        input.get("left_operand").asDouble() + input.get("right_operand").asDouble());
                case "subtract" -> ToolResult.success(
                        input.get("left_operand").asDouble() - input.get("right_operand").asDouble());
                case "multiply" -> ToolResult.success(
                        input.get("left_operand").asDouble() * input.get("right_operand").asDouble());
                case "divide" -> {
                    double b = input.get("right_operand").asDouble();
                    if (b == 0) {
                        yield ToolResult.error("Division by zero");
                    }
                    yield ToolResult.success(input.get("left_operand").asDouble() / b);
                }
                default -> ToolResult.error("Unknown operation: " + operation);
            };
        }
    }

    private static class WeatherTool
            extends InternalToolDefinition<String>
    {
        public WeatherTool()
        {
            super(
                    "get_weather",
                    "Gets the current weather for a specified location",
                    ImmutableList.of(
                            new ToolParameter(
                                    "location",
                                    STRING,
                                    "The city and country, e.g., 'Paris, France'",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(),
                                    Optional.empty()),
                            new ToolParameter(
                                    "unit",
                                    STRING,
                                    "Temperature unit (celsius or fahrenheit)",
                                    ImmutableList.of("celsius", "fahrenheit"),
                                    false,
                                    ImmutableList.of(),
                                    Optional.empty())));
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            // Mock implementation
            return ToolResult.success("Sunny, 72°F");
        }
    }

    private static class SearchTool
            extends InternalToolDefinition<String>
    {
        public SearchTool()
        {
            super(
                    "search",
                    "Searches for information on the internet",
                    ImmutableList.of(
                            new ToolParameter(
                                    "query",
                                    STRING,
                                    "The search query",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(),
                                    Optional.empty()),
                            new ToolParameter(
                                    "max_results",
                                    INTEGER,
                                    "Maximum number of results to return",
                                    ImmutableList.of(),
                                    false,
                                    ImmutableList.of(),
                                    Optional.empty())));
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            // Mock implementation
            String query = input.has("query") ? input.get("query").asText() : "unknown";
            return ToolResult.success("Search results for: " + query);
        }
    }

    public Object[][] modelIds()
    {
        return new Object[][] {};
    }
}
