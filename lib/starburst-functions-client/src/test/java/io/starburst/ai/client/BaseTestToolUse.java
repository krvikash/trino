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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.JsonSchemaParameterType.INTEGER;
import static io.starburst.ai.client.JsonSchemaParameterType.NUMBER;
import static io.starburst.ai.client.JsonSchemaParameterType.STRING;
import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.TOOL_RESPONSE;
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
    private final AtomicReference<TokenUsage> capturedUsage = new AtomicReference<>();
    private final AtomicReference<TokenUsageContext> capturedContext = new AtomicReference<>();

    @BeforeAll
    public void setup()
            throws IOException
    {
        reloadingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
        llmExecutor = createLlmExecutor();

        modelClientProvider = staticModelClientProvider(getLanguageModelProviders(), reloadingExecutor, llmExecutor,
                (ctx, usage) -> {
                    capturedContext.set(ctx);
                    capturedUsage.set(usage);
                });
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
                new LlmMessage(USER, Optional.of("What is 25 + 37? Use the calculator tool."), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                modelId,
                "You are a helpful assistant with access to a calculator.",
                messages,
                ImmutableList.of(tool));

        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }
        assertThat(response.toolCalls()).hasSize(1);

        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        assertThat(toolCall.name()).withFailMessage("Model response: " + response.textResponse()).isEqualTo("calculator");
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
                new LlmMessage(USER, Optional.of("What's the weather like in Paris, France?"), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                modelId,
                "You are a helpful assistant. Use the appropriate tool to answer questions.",
                messages,
                tools);
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.toolCalls()).isNotEmpty();

        // Should call the weather tool
        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        assertThat(toolCall.name()).withFailMessage("Model response: " + response.textResponse()).isEqualTo("get_weather");
        assertThat(toolCall.input().has("location")).isTrue();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testToolCallWithConversationHistory(String modelId)
    {
        ToolDefinition<Double> tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, Optional.of("I need to do some math"), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(ASSISTANT, Optional.of("I can use my calculator for that! What calculation do you need?"), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(USER, Optional.of("What's 42 times 13?"), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                modelId,
                "You are a helpful math assistant. You must always use the provided tools to answer. Never compute answers yourself.",
                messages,
                ImmutableList.of(tool));
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.toolCalls()).withFailMessage("Model response: " + response.textResponse()).isNotEmpty();
        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        ToolResult<Double> toolResult = tool.execute(toolCall.input());
        assertThat(toolResult.content()).contains(42d * 13d);
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMultiToolCallHistory(String modelId)
            throws JsonProcessingException
    {
        List<ToolDefinition<?>> tools = ImmutableList.of(
                new CalculatorTool(),
                new WeatherTool(),
                new SearchTool());
        JsonMapper mapper = new JsonMapperProvider().get();
        ObjectNode callCalculatorNode0 = mapper.createObjectNode();
        callCalculatorNode0.put("operation", "multiply");
        callCalculatorNode0.put("left_operand", 42);
        callCalculatorNode0.put("right_operand", 13);
        ObjectNode callCalculatorNode1 = mapper.createObjectNode();
        callCalculatorNode1.put("operation", "divide");
        callCalculatorNode1.put("left_operand", 546);
        callCalculatorNode1.put("right_operand", 3);
        ObjectNode callCalculatorNode2 = mapper.createObjectNode();
        callCalculatorNode2.put("operation", "multiply");
        callCalculatorNode2.put("left_operand", 182.0);
        callCalculatorNode2.put("right_operand", 99);

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, Optional.of("What's 42 times 13?"), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(ASSISTANT, Optional.empty(), ImmutableList.of(), ImmutableList.of(
                        new ToolUseResponse.ToolCall("1", "calculator", callCalculatorNode0))),
                new LlmMessage(TOOL_RESPONSE, Optional.empty(), ImmutableList.of(
                        new LlmMessage.ToolResponse(mapper.createObjectNode().put("result", 42 * 13), "1")), ImmutableList.of()),
                new LlmMessage(ASSISTANT, Optional.of("The result is 546."), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(USER, Optional.of("Divide this by 3, show the result, then multiply by 99"), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(ASSISTANT, Optional.empty(), ImmutableList.of(), ImmutableList.of(
                        new ToolUseResponse.ToolCall("2", "calculator", callCalculatorNode1),
                        new ToolUseResponse.ToolCall("3", "calculator", callCalculatorNode2))),
                // A tool response block MUST contain a matching response id for every call in the preceding tool use block!
                new LlmMessage(TOOL_RESPONSE, Optional.empty(), ImmutableList.of(
                        new LlmMessage.ToolResponse(mapper.createObjectNode().put("result", 42 * 13 / 3.0), "2"),
                        new LlmMessage.ToolResponse(mapper.createObjectNode().put("result", 42 * 13 * 99 / 3.0), "3")), ImmutableList.of()),
                new LlmMessage(ASSISTANT, Optional.of("The result is %s.".formatted(42 * 13 * 99 / 3.0)), ImmutableList.of(), ImmutableList.of()),
                new LlmMessage(USER, Optional.of("Divide this by 71"), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                modelId,
                "You are a helpful math assistant.",
                messages,
                tools);
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.toolCalls()).withFailMessage("Model response: " + response.textResponse()).isNotEmpty();
        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        assertThat(toolCall.name()).withFailMessage("Model response: " + response.textResponse()).isEqualTo("calculator");
        ToolResult<Double> toolResult = new CalculatorTool().execute(toolCall.input());
        assertThat(toolResult.content()).isPresent();
        assertThat(toolResult.content().get()).isCloseTo(42 * 13 * 99 / 3.0 / 71.0, Offset.offset(0.01));
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testNoToolCallWhenNotNeeded(String modelId)
    {
        ToolDefinition<?> tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, Optional.of("Hello, how are you?"), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                modelId,
                "You are a helpful assistant",
                messages,
                ImmutableList.of(tool));
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        assertThat(response.textResponse()).isNotEmpty();
        assertThat(response.toolCalls()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testToolCallWithNoParameters(String modelId)
    {
        ToolDefinition<?> tool = new ClockTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, Optional.of("What's the current time in UTC?"), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                modelId,
                "You are a helpful assistant",
                messages,
                ImmutableList.of(tool));
        for (ToolUseResponse.ToolCall call : response.toolCalls()) {
            log.info("Model: %s, Tool call: %s with parameters %s", modelId, call.name(), call.input().toPrettyString());
        }

        // Should call the clock tool
        ToolUseResponse.ToolCall toolCall = response.toolCalls().getFirst();
        assertThat(toolCall.name()).withFailMessage("Model response: " + response.textResponse()).isEqualTo("clock");
        assertThat(toolCall.input().isNull() || toolCall.input().isEmpty()).isTrue();
    }

    protected ToolUseResponse executeToolUse(
            String modelId,
            String systemPrompt,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools)
    {
        StringBuilder streamedTokens = new StringBuilder();
        capturedUsage.set(null);
        capturedContext.set(null);
        TokenUsageContext expectedContext = TokenUsageContext.of(modelId, new TestingUtils.TestOperationId("test"));

        LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice(modelId));

        ToolUseResponse response = client.generateWithTools(
                systemPrompt,
                messages,
                tools,
                streamedTokens::append,
                () -> false,
                expectedContext);
        assertThat(streamedTokens.toString()).isEqualTo(response.textResponse());

        assertThat(capturedUsage.get()).isNotNull();
        assertThat(capturedUsage.get().inputTokens()).isGreaterThan(0);
        assertThat(capturedUsage.get().outputTokens()).isGreaterThan(0);
        assertThat(capturedUsage.get().modelName()).isNotEmpty();
        assertThat(capturedContext.get()).isEqualTo(expectedContext);

        return response;
    }

    protected static class CalculatorTool
            extends InternalToolDefinition<Double>
    {
        public CalculatorTool()
        {
            super("calculator",
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
            super("get_weather",
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
            super("search",
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

    private static class ClockTool
            extends InternalToolDefinition<String>
    {
        public ClockTool()
        {
            super("clock",
                    "Gets the current UTC timestamp",
                    ImmutableList.of());
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            // Mock implementation
            return ToolResult.success(Instant.now().toString());
        }
    }

    public Object[][] modelIds()
    {
        return new Object[][] {};
    }
}
