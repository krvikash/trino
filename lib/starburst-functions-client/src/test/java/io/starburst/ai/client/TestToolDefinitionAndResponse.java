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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.ai.client.JsonSchemaParameterType.ARRAY;
import static io.starburst.ai.client.JsonSchemaParameterType.INTEGER;
import static io.starburst.ai.client.JsonSchemaParameterType.NUMBER;
import static io.starburst.ai.client.JsonSchemaParameterType.OBJECT;
import static io.starburst.ai.client.JsonSchemaParameterType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestToolDefinitionAndResponse
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testInternalToolDefinitionWithBasicParameters()
    {
        TestTool tool = new TestTool();

        assertThat(tool.getName()).isEqualTo("test_tool");
        assertThat(tool.getDescription()).isEqualTo("A test tool");

        JsonNode schema = tool.getInputSchema();
        assertThat(schema.get("type").asText()).isEqualTo("object");
        assertThat(schema.get("properties").get("query").get("type").asText()).isEqualTo("string");
        assertThat(schema.get("required")).containsExactly(OBJECT_MAPPER.valueToTree("query"));
    }

    @Test
    public void testInternalToolDefinitionWithEnumParameter()
    {
        CalculatorTestTool tool = new CalculatorTestTool();

        JsonNode schema = tool.getInputSchema();
        JsonNode operationParam = schema.get("properties").get("operation");

        assertThat(operationParam.get("type").asText()).isEqualTo("string");
        assertThat(operationParam.get("enum")).containsExactlyElementsOf(
                ImmutableList.of("add", "subtract", "multiply", "divide").stream()
                        .map(op -> (JsonNode) OBJECT_MAPPER.valueToTree(op))
                        .collect(toImmutableList()));
        assertThat(schema.get("required")).containsExactlyElementsOf(
                ImmutableList.of("operation", "leftOperand", "rightOperand").stream()
                        .map(op -> (JsonNode) OBJECT_MAPPER.valueToTree(op))
                        .collect(toImmutableList()));
    }

    @Test
    public void testInternalToolDefinitionWithOptionalParameter()
    {
        SearchTestTool tool = new SearchTestTool();

        JsonNode schema = tool.getInputSchema();
        assertThat(schema.get("required")).containsExactly(OBJECT_MAPPER.valueToTree("query"));
        assertThat(schema.get("properties").has("max_results")).isTrue();
    }

    @Test
    public void testToolExecutionSuccess()
    {
        TestTool tool = new TestTool();
        ObjectNode input = OBJECT_MAPPER.createObjectNode();
        input.put("query", "test");

        ToolResult<String> result = tool.execute(input);

        assertThat(result.success()).isTrue();
        assertThat(result.content()).contains("success result");
        assertThat(result.error()).isEmpty();
    }

    @Test
    public void testToolExecutionMissingRequiredParameter()
    {
        TestTool tool = new TestTool();
        JsonNode input = OBJECT_MAPPER.createObjectNode();

        ToolResult<String> result = tool.execute(input);

        assertThat(result.success()).isFalse();
        assertThat(result.content()).isEmpty();
        assertThat(result.error()).contains("Missing required parameters: query");
    }

    @Test
    public void testToolExecutionError()
    {
        ErrorTestTool tool = new ErrorTestTool();
        JsonNode input = OBJECT_MAPPER.createObjectNode();

        ToolResult<String> result = tool.execute(input);

        assertThat(result.success()).isFalse();
        assertThat(result.content()).isEmpty();
        assertThat(result.error()).contains("error message");
    }

    @Test
    public void testToolResultFactoryMethods()
    {
        ToolResult<String> success = ToolResult.success("good");
        assertThat(success.success()).isTrue();
        assertThat(success.content()).contains("good");
        assertThat(success.error()).isEmpty();

        ToolResult<String> error = ToolResult.error("bad");
        assertThat(error.success()).isFalse();
        assertThat(error.content()).isEmpty();
        assertThat(error.error()).contains("bad");
    }

    @Test
    public void testInternalToolDefinitionWithNestedObjectInArray()
    {
        NestedStructureTestTool tool = new NestedStructureTestTool();

        JsonNode schema = tool.getInputSchema();
        JsonNode usersParam = schema.get("properties").get("users");

        // Verify ARRAY type
        assertThat(usersParam.get("type").asText()).isEqualTo("array");

        // Verify OBJECT items
        JsonNode items = usersParam.get("items");
        assertThat(items.get("type").asText()).isEqualTo("object");
        assertThat(items.has("properties")).isTrue();
        assertThat(items.has("additionalProperties")).isTrue();
        assertThat(items.get("additionalProperties").asBoolean()).isFalse();

        // Verify nested object properties
        JsonNode itemProperties = items.get("properties");
        assertThat(itemProperties.has("name")).isTrue();
        assertThat(itemProperties.has("age")).isTrue();
        assertThat(itemProperties.get("name").get("type").asText()).isEqualTo("string");
        assertThat(itemProperties.get("age").get("type").asText()).isEqualTo("integer");

        // Verify required fields in nested object
        JsonNode itemRequired = items.get("required");
        assertThat(itemRequired).hasSize(1);
        assertThat(itemRequired.get(0).asText()).isEqualTo("name");
    }

    @Test
    public void testInternalToolDefinitionWithObjectContainingArray()
    {
        ComplexStructureTestTool tool = new ComplexStructureTestTool();

        JsonNode schema = tool.getInputSchema();
        JsonNode metadataParam = schema.get("properties").get("metadata");

        // Verify outer OBJECT
        assertThat(metadataParam.get("type").asText()).isEqualTo("object");

        JsonNode properties = metadataParam.get("properties");
        assertThat(properties.has("tableName")).isTrue();
        assertThat(properties.has("columns")).isTrue();

        // Verify nested ARRAY
        JsonNode columnsParam = properties.get("columns");
        assertThat(columnsParam.get("type").asText()).isEqualTo("array");

        // Verify ARRAY items
        JsonNode items = columnsParam.get("items");
        assertThat(items.get("type").asText()).isEqualTo("string");
    }

    private static class TestTool
            extends InternalToolDefinition<String>
    {
        public TestTool()
        {
            super("test_tool", "A test tool", ImmutableList.of(new ToolParameter("query", STRING, "The search query", true)));
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.success("success result");
        }
    }

    private static class ErrorTestTool
            extends InternalToolDefinition<String>
    {
        public ErrorTestTool()
        {
            super("error_tool", "A tool that errors", ImmutableList.of());
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.error("error message");
        }
    }

    private static class CalculatorTestTool
            extends InternalToolDefinition<Double>
    {
        public CalculatorTestTool()
        {
            super(
                    "calculator",
                    "Performs math operations",
                    ImmutableList.of(
                            new ToolParameter(
                                    "operation",
                                    STRING,
                                    "The operation",
                                    ImmutableList.of("add", "subtract", "multiply", "divide"),
                                    true),
                            new ToolParameter(
                                    "leftOperand",
                                    NUMBER,
                                    "The number to the left of the operator",
                                    true),

                            new ToolParameter(
                                    "rightOperand",
                                    NUMBER,
                                    "The number to the right of the operator",
                                    true)));
        }

        @Override
        public ToolResult<Double> executeInternal(JsonNode input)
        {
            return ToolResult.success(42.0);
        }
    }

    private static class SearchTestTool
            extends InternalToolDefinition<String>
    {
        public SearchTestTool()
        {
            super(
                    "search",
                    "Searches for information",
                    ImmutableList.of(
                            new ToolParameter(
                                    "query",
                                    STRING,
                                    "The query",
                                    true),
                            new ToolParameter(
                                    "max_results",
                                    INTEGER,
                                    "Max results",
                                    false)));
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.success("results");
        }
    }

    private static class NestedStructureTestTool
            extends InternalToolDefinition<String>
    {
        public NestedStructureTestTool()
        {
            super(
                    "create_users",
                    "Creates multiple users",
                    ImmutableList.of(
                            new ToolParameter(
                                    "users",
                                    ARRAY,
                                    "List of users",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(),
                                    Optional.of(
                                            new ToolParameter(
                                                    "user",
                                                    OBJECT,
                                                    "User object",
                                                    ImmutableList.of(),
                                                    false,
                                                    ImmutableList.of(
                                                            new ToolParameter("name", STRING, "User name", true),
                                                            new ToolParameter("age", INTEGER, "User age", false)),
                                                    Optional.empty())))));
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.success("users created");
        }
    }

    private static class ComplexStructureTestTool
            extends InternalToolDefinition<String>
    {
        public ComplexStructureTestTool()
        {
            super(
                    "define_table",
                    "Defines a table structure",
                    ImmutableList.of(
                            new ToolParameter(
                                    "metadata",
                                    OBJECT,
                                    "Table metadata",
                                    ImmutableList.of(),
                                    true,
                                    ImmutableList.of(
                                            new ToolParameter("tableName", STRING, "Name of table", true),
                                            new ToolParameter(
                                                    "columns",
                                                    ARRAY,
                                                    "Column names",
                                                    ImmutableList.of(),
                                                    true,
                                                    ImmutableList.of(),
                                                    Optional.of(
                                                            new ToolParameter(
                                                                    "column",
                                                                    STRING,
                                                                    "Column name",
                                                                    false)))),
                                    Optional.empty())));
        }

        @Override
        public ToolResult<String> executeInternal(JsonNode input)
        {
            return ToolResult.success("table defined");
        }
    }
}
