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

import io.opentelemetry.api.trace.Span;
import io.starburst.ai.client.TokenUsage;

import java.util.function.Supplier;

import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_REASONING_OUTPUT_TOKENS;

public final class AiTracing
{
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

    public static void recordUsage(Span span, TokenUsage usage)
    {
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
}
