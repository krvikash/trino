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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static io.starburst.ai.client.JsonSchemaParameterType.ARRAY;
import static io.starburst.ai.client.JsonSchemaParameterType.OBJECT;
import static java.util.Objects.requireNonNull;

// A parameter is defined using JSON Schema: https://json-schema.org/understanding-json-schema/keywords
// Only a subset of JSON Schema is supported: https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/structured-outputs?tabs=python-secure,dotnet-entra-id&pivots=programming-language-csharp#supported-schemas-and-limitations
// Additionally, we do not support anyOf for internally defined tools
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ToolParameter(
        @JsonProperty("name") String name,
        @JsonProperty("type") JsonSchemaParameterType type,
        @JsonProperty("description") String description,
        @JsonProperty("enum") List<String> enumValues,
        @JsonProperty("required") boolean required,
        @JsonProperty("properties") List<ToolParameter> properties,
        @JsonProperty("items") Optional<ToolParameter> items)
{
    public ToolParameter(String name, JsonSchemaParameterType type, String description, boolean required)
    {
        this(name, type, description, ImmutableList.of(), required);
    }

    public ToolParameter(String name, JsonSchemaParameterType type, String description, List<String> enumValues, boolean required)
    {
        this(name, type, description, enumValues, required, List.of(), Optional.empty());
    }

    public ToolParameter
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(description, "description is null");
        requireNonNull(enumValues, "enumValues is null");
        requireNonNull(properties, "properties is null");
        requireNonNull(items, "items is null");

        if (type == OBJECT && properties.isEmpty()) {
            throw new IllegalArgumentException("Object type parameters must not have empty properties");
        }
        if (type == ARRAY && items.isEmpty()) {
            throw new IllegalArgumentException("Array type parameters must not have empty items");
        }
    }
}
