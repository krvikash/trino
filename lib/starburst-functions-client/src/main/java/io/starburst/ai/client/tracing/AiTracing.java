/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.tracing;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.ToolUseResponse;

import java.util.List;
import java.util.function.Supplier;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.starburst.ai.client.tracing.GenAiAttributes.EVENT_NAME_COMPLETION;
import static io.starburst.ai.client.tracing.GenAiAttributes.EVENT_NAME_PROMPT;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_COMPLETION;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_PROMPT;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_REASONING_OUTPUT_TOKENS;

public final class AiTracing
{
    private static final Logger log = Logger.get(AiTracing.class);
    private static final JsonCodec<PromptContent> PROMPT_CODEC = jsonCodec(PromptContent.class);
    private static final JsonCodec<ToolUseResponse> COMPLETION_CODEC = jsonCodec(ToolUseResponse.class);

    private AiTracing() {}

    public static <T> T withTracing(Span span, Supplier<T> supplier)
    {
        try (var _ = span.makeCurrent()) {
            return supplier.get();
        }
        catch (Throwable t) {
            span.setStatus(ERROR, t.getMessage());
            span.recordException(t);
            throw t;
        }
        finally {
            span.end();
        }
    }

    // Tracing is diagnostic, not functional: a bug here (e.g. a value that fails to serialize) must
    // never fail an otherwise-successful AI request, so every failure is caught, logged, and dropped.
    public static void recordUsage(Span span, TokenUsage usage)
    {
        try {
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, usage.inputTokens());
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, usage.outputTokens());
            if (usage.cacheReadInputTokens() > 0) {
                span.setAttribute(GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS, usage.cacheReadInputTokens());
            }
            if (usage.cacheCreationInputTokens() > 0) {
                span.setAttribute(GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS, usage.cacheCreationInputTokens());
            }
            if (usage.reasoningOutputTokens() > 0) {
                span.setAttribute(GEN_AI_USAGE_REASONING_OUTPUT_TOKENS, usage.reasoningOutputTokens());
            }
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to record AI token usage in trace");
        }
    }

    public static void recordMessages(Span span, List<String> systemPrompts, List<LlmMessage> messages, ToolUseResponse response)
    {
        try {
            span.addEvent(EVENT_NAME_PROMPT, Attributes.of(GEN_AI_PROMPT, PROMPT_CODEC.toJson(new PromptContent(systemPrompts, messages))));
            span.addEvent(EVENT_NAME_COMPLETION, Attributes.of(GEN_AI_COMPLETION, COMPLETION_CODEC.toJson(response)));
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to record AI messages in trace");
        }
    }

    public record PromptContent(List<String> systemPrompts, List<LlmMessage> messages) {}
}
