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

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record LlmMessage(MessageRole role, Optional<String> content, List<ToolResponse> toolResponse, List<ToolUseResponse.ToolCall> toolCalls)
{
    /*
    Models and APIs may have different constraints. For example, Mistral requires that TOOL_RESPONSE must be followed
    by ASSISTANT before the next USER text. Anthropic models are more lenient, however, all Bedrock models require that an assistant message
    containing tool calls must be followed by a user message with a tool response block, and that block must contain a response with all ids
    present in the previous tool call block. Mistral also rejects assistant messages containing both text and tool calls.
     */
    public LlmMessage
    {
        requireNonNull(role, "role is null");
        requireNonNull(content, "content is null");
        requireNonNull(toolResponse, "toolResponse is null");
        requireNonNull(toolCalls, "toolCalls is null");
        if (content.isPresent() && content.get().isBlank()) {
            throw new IllegalArgumentException("content may not be blank");
        }
        switch (role) {
            case USER -> {
                if (content.isEmpty()) {
                    throw new IllegalArgumentException("USER messages must have content");
                }
                if (!toolCalls.isEmpty()) {
                    throw new IllegalArgumentException("toolCalls are not supported for role " + role);
                }
                if (!toolResponse.isEmpty()) {
                    throw new IllegalArgumentException("toolResponses are not supported for role " + role);
                }
            }
            case ASSISTANT -> {
                if (content.isEmpty() && toolCalls.isEmpty()) {
                    throw new IllegalArgumentException("ASSISTANT messages must have content or tool calls");
                }
                if (!toolResponse.isEmpty()) {
                    throw new IllegalArgumentException("toolResponses are not supported for role " + role);
                }
            }
            case TOOL_RESPONSE -> {
                if (content.isPresent()) {
                    throw new IllegalArgumentException("content is not supported for role " + role);
                }
                if (!toolCalls.isEmpty()) {
                    throw new IllegalArgumentException("toolCalls are not supported for role " + role);
                }
                if (toolResponse.isEmpty()) {
                    throw new IllegalArgumentException("TOOL_RESPONSE messages must have at least one tool response");
                }
            }
        }
    }

    public LlmMessage(MessageRole role, String content)
    {
        this(role, Optional.of(requireNonNull(content)), List.of(), List.of());
    }

    public record ToolResponse(ObjectNode responseJson, String toolUseId)
    {
        public ToolResponse
        {
            requireNonNull(responseJson);
            requireNonNull(toolUseId);
        }
    }
}
