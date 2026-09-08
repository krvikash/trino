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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.MessageRole;
import io.starburst.ai.client.ModelBackend;
import io.starburst.ai.client.ModelType;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.ToolUseResponse;
import io.trino.testing.TestingTelemetry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.starburst.ai.client.tracing.GenAiAttributes.EVENT_NAME_COMPLETION;
import static io.starburst.ai.client.tracing.GenAiAttributes.EVENT_NAME_PROMPT;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_COMPLETION;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_PROMPT;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.starburst.ai.client.tracing.GenAiAttributes.GEN_AI_USAGE_REASONING_OUTPUT_TOKENS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAiTracing
{
    // Exercises the exact serialization path AiTracing.recordMessages uses. This is the regression
    // guard for PromptContent needing to stay a *public* nested record: Jackson's ObjectMapper here has
    // MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS disabled, so a private record's accessors are
    // unreachable and toJson() throws instead of producing an event attribute.
    private static final JsonCodec<AiTracing.PromptContent> PROMPT_CODEC = jsonCodec(AiTracing.PromptContent.class);
    private static final JsonCodec<ToolUseResponse> COMPLETION_CODEC = jsonCodec(ToolUseResponse.class);

    @Test
    public void testRecordMessagesEmitsPromptAndCompletionEvents()
    {
        TestingTelemetry telemetry = TestingTelemetry.create("test-ai-tracing");
        List<String> systemPrompts = List.of("You are a helpful assistant.");
        List<LlmMessage> messages = List.of(new LlmMessage(MessageRole.USER, "What is the capital of France?"));
        ToolUseResponse response = new ToolUseResponse("Paris", List.of());

        List<SpanData> spans = telemetry.captureSpans(() -> {
            Span span = telemetry.getTracer().spanBuilder("ai-request").startSpan();
            AiTracing.recordMessages(span, systemPrompts, messages, response);
            span.end();
        });

        assertThat(spans).hasSize(1);
        List<EventData> events = spans.getFirst().getEvents();
        assertThat(events).hasSize(2);

        EventData promptEvent = events.getFirst();
        assertThat(promptEvent.getName()).isEqualTo(EVENT_NAME_PROMPT);
        AiTracing.PromptContent promptContent = PROMPT_CODEC.fromJson(promptEvent.getAttributes().get(GEN_AI_PROMPT));
        assertThat(promptContent.systemPrompts()).isEqualTo(systemPrompts);
        assertThat(promptContent.messages()).isEqualTo(messages);

        EventData completionEvent = events.get(1);
        assertThat(completionEvent.getName()).isEqualTo(EVENT_NAME_COMPLETION);
        ToolUseResponse completion = COMPLETION_CODEC.fromJson(completionEvent.getAttributes().get(GEN_AI_COMPLETION));
        assertThat(completion).isEqualTo(response);
    }

    @Test
    public void testRecordUsageSetsTokenAttributes()
    {
        TestingTelemetry telemetry = TestingTelemetry.create("test-ai-tracing");
        TokenUsage usage = new TokenUsage(10, 20, 5, 3, 2, "test-model", Optional.empty(), ModelType.LANGUAGE, ModelBackend.AWS_BEDROCK);

        List<SpanData> spans = telemetry.captureSpans(() -> {
            Span span = telemetry.getTracer().spanBuilder("ai-request").startSpan();
            AiTracing.recordUsage(span, usage);
            span.end();
        });

        assertThat(spans).hasSize(1);
        Attributes attributes = spans.getFirst().getAttributes();
        assertThat(attributes.get(GEN_AI_USAGE_INPUT_TOKENS)).isEqualTo(10L);
        assertThat(attributes.get(GEN_AI_USAGE_OUTPUT_TOKENS)).isEqualTo(20L);
        assertThat(attributes.get(GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS)).isEqualTo(5L);
        assertThat(attributes.get(GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS)).isEqualTo(3L);
        assertThat(attributes.get(GEN_AI_USAGE_REASONING_OUTPUT_TOKENS)).isEqualTo(2L);
    }
}
