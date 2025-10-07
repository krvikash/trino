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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

import static io.starburst.ai.client.JsonSchemaParameterType.ARRAY;
import static io.starburst.ai.client.JsonSchemaParameterType.OBJECT;
import static java.util.Objects.requireNonNull;

public abstract class InternalToolDefinition<T>
        extends ToolDefinition<T>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final List<ToolParameter> parameters;

    protected InternalToolDefinition(
            String name,
            String description,
            List<ToolParameter> parameters)
    {
        super(name, description);
        this.parameters = requireNonNull(parameters, "parameters is null");
    }

    @Override
    public JsonNode getInputSchema()
    {
        ObjectNode schema = OBJECT_MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.put("additionalProperties", false);

        ObjectNode properties = OBJECT_MAPPER.createObjectNode();
        ArrayNode required = OBJECT_MAPPER.createArrayNode();

        for (ToolParameter param : parameters) {
            ObjectNode paramNode = buildParameterSchema(param);
            properties.set(param.name(), paramNode);

            if (param.required()) {
                required.add(param.name());
            }
        }

        schema.set("properties", properties);
        if (!required.isEmpty()) {
            schema.set("required", required);
        }

        return schema;
    }

    /**
     * Builds a JSON schema node for a parameter, handling OBJECT and ARRAY types recursively.
     */
    private ObjectNode buildParameterSchema(ToolParameter param)
    {
        ObjectNode paramNode = OBJECT_MAPPER.createObjectNode();
        paramNode.put("type", param.type().toString());
        paramNode.put("description", param.description());

        // Handle enum values
        if (param.enumValues() != null && !param.enumValues().isEmpty()) {
            ArrayNode enumArray = OBJECT_MAPPER.createArrayNode();
            param.enumValues().forEach(enumArray::add);
            paramNode.set("enum", enumArray);
        }

        // Handle OBJECT type with nested properties
        if (param.type() == OBJECT && !param.properties().isEmpty()) {
            ObjectNode nestedProperties = OBJECT_MAPPER.createObjectNode();
            ArrayNode nestedRequired = OBJECT_MAPPER.createArrayNode();

            for (ToolParameter nestedParam : param.properties()) {
                ObjectNode nestedParamNode = buildParameterSchema(nestedParam);
                nestedProperties.set(nestedParam.name(), nestedParamNode);

                if (nestedParam.required()) {
                    nestedRequired.add(nestedParam.name());
                }
            }

            paramNode.set("properties", nestedProperties);
            if (!nestedRequired.isEmpty()) {
                paramNode.set("required", nestedRequired);
            }
            paramNode.put("additionalProperties", false);
        }

        // Handle ARRAY type with item schema
        if (param.type() == ARRAY && param.items().isPresent()) {
            ObjectNode itemsNode = buildParameterSchema(param.items().get());
            paramNode.set("items", itemsNode);
        }

        return paramNode;
    }

    @Override
    public final ToolResult<T> execute(JsonNode input)
    {
        List<String> missingParams = new ArrayList<>();
        for (ToolParameter param : parameters) {
            if (param.required() && (!input.has(param.name()) || input.get(param.name()).isNull())) {
                missingParams.add(param.name());
            }
        }
        if (!missingParams.isEmpty()) {
            return ToolResult.error("Missing required parameters: " + String.join(", ", missingParams));
        }

        return executeInternal(input);
    }

    @Override
    public String formatResult(ToolResult<T> result)
    {
        if (result.success() && result.content().isPresent()) {
            T content = result.content().get();
            return content.toString();
        }
        return "";
    }

    protected abstract ToolResult<T> executeInternal(JsonNode input);
}
