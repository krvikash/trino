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

import java.util.List;

import static java.util.Objects.requireNonNull;

public record ToolUseResponse(String textResponse, List<ToolCall> toolCalls)
{
    public ToolUseResponse
    {
        requireNonNull(textResponse, "textResponse is null");
        requireNonNull(toolCalls, "toolCalls is null");
    }

    public record ToolCall(String id, String name, JsonNode input)
    {
        public ToolCall
        {
            requireNonNull(id, "id is null");
            requireNonNull(name, "name is null");
            requireNonNull(input, "input is null");
        }
    }
}
