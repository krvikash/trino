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

import static java.util.Objects.requireNonNull;

public abstract class ToolDefinition<T>
{
    private final String name;
    private final String description;

    protected ToolDefinition(String name, String description)
    {
        this.name = requireNonNull(name, "name is null");
        this.description = requireNonNull(description, "description is null");
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return description;
    }

    /**
     * Serializes this tool definition to a JSON Schema format suitable for the LLM.
     * The returned JsonNode should have the structure:
     * {
     * "type": "object",
     * "properties": { ... },
     * "required": [ ... ]
     * }
     *
     * @return JSON Schema describing the tool's input parameters
     */
    public abstract JsonNode getInputSchema();

    // This method is used by the SEP agent to execute the tool with the input parameters provided by the LLM.
    public abstract ToolResult<T> execute(JsonNode input);

    /**
     * Formats the tool's output content into a string for returning to the LLM.
     * Tool implementations can override this to provide custom formatting for their specific result types.
     *
     * @param result The result from executing this tool
     * @return Formatted string representation of the result
     */
    public abstract String formatResult(ToolResult<T> result);
}
