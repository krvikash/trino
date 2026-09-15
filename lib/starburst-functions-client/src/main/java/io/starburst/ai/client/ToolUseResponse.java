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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record ToolUseResponse(String textResponse, List<ToolCall> toolCalls)
{
    public ToolUseResponse
    {
        requireNonNull(textResponse, "textResponse is null");
        requireNonNull(toolCalls, "toolCalls is null");
    }

    /**
     * A single tool call requested by the model.
     *
     * <p>{@code thoughtSignature} is opaque, provider-specific reasoning metadata that Gemini 3.x
     * thinking models attach to a function-call part and require to be echoed back verbatim on the
     * assistant turn in the follow-up request; omitting it on the first call of a step yields a 400
     * "Function call is missing a thought_signature". It is base64-encoded here so it round-trips
     * cleanly through JSON persistence and record equality, and is decoded back to bytes only at the
     * Vertex AI SDK boundary. Empty for providers that emit no such token, for the 2nd+ call of a
     * parallel group (Gemini signs only the first), and for app-synthesized calls the model never
     * generated.
     */
    public record ToolCall(String id, String name, JsonNode input, Optional<String> thoughtSignature)
    {
        public ToolCall
        {
            requireNonNull(id, "id is null");
            requireNonNull(name, "name is null");
            requireNonNull(input, "input is null");
            requireNonNull(thoughtSignature, "thoughtSignature is null");
        }

        public ToolCall(String id, String name, JsonNode input)
        {
            this(id, name, input, Optional.empty());
        }
    }
}
