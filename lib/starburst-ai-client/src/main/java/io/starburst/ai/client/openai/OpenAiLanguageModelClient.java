/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai;

import com.google.common.collect.ImmutableList;
import com.openai.client.OpenAIClient;
import com.openai.core.JsonValue;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessage;
import com.openai.models.completions.CompletionUsage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.AbstractLanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.PromptDao;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;

import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPENAI_RESPONSE_SERVICE_TIER;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_SEED;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_ID;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_SYSTEM;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiOperationNameIncubatingValues.CHAT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiSystemIncubatingValues.OPENAI;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.MessageRole.USER;
import static java.util.Objects.requireNonNull;

public class OpenAiLanguageModelClient
        extends AbstractLanguageModelClient
{
    private static final int SEED = 37;

    private final Optional<Float> temperature;
    private final Optional<Integer> maxTokens;
    private final Optional<Float> topP;
    private final boolean useDeveloperForSystemRole;
    private final Tracer tracer;
    private final OpenAIClient client;

    public OpenAiLanguageModelClient(
            String modelName,
            Optional<Float> temperature,
            Optional<Integer> maxTokens,
            Optional<Float> topP,
            boolean useDeveloperForSystemRole,
            PromptDao promptDao,
            Tracer tracer,
            OpenAIClient client)
    {
        super(modelName, promptDao);
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.useDeveloperForSystemRole = useDeveloperForSystemRole;
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    protected String generateCompletion(String model, List<String> systemPrompts, String prompt)
    {
        return generateCompletion(model, systemPrompts, ImmutableList.of(new LlmMessage(USER, prompt)));
    }

    @Override
    protected String generateCompletion(String model, List<String> systemPrompts, List<LlmMessage> llmMessages)
    {
        ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder()
                .model(model)
                .seed(SEED);
        temperature.ifPresent(builder::temperature);
        topP.ifPresent(builder::topP);
        maxTokens.ifPresent(builder::maxTokens);

        if (useDeveloperForSystemRole) {
            systemPrompts.forEach(builder::addSystemMessage);
        }
        else {
            systemPrompts.forEach(builder::addDeveloperMessage);
        }

        llmMessages.forEach(llmMessage -> {
            switch (llmMessage.role()) {
                case USER -> builder.addUserMessage(llmMessage.content());
                case ASSISTANT -> builder.addMessage(createAssistantMessage(llmMessage.content()));
            }
        });

        Span span = tracer.spanBuilder(CHAT + " " + model)
                .setAttribute(GEN_AI_OPERATION_NAME, CHAT)
                .setAttribute(GEN_AI_SYSTEM, OPENAI)
                .setAttribute(GEN_AI_REQUEST_MODEL, model)
                .setAttribute(GEN_AI_REQUEST_SEED, SEED)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        ChatCompletion response;
        try (var _ = span.makeCurrent()) {
            response = client.chat().completions().create(builder.build());
            span.setAttribute(GEN_AI_RESPONSE_ID, response.id());
            span.setAttribute(GEN_AI_RESPONSE_MODEL, response.model());
            span.setAttribute(GEN_AI_OPENAI_RESPONSE_SERVICE_TIER, response.serviceTier()
                    .map(ChatCompletion.ServiceTier::value)
                    .map(ChatCompletion.ServiceTier.Value::name)
                    .orElse(""));
            span.setAttribute(GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT, response.systemFingerprint().orElse(""));
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, response.usage().map(CompletionUsage::promptTokens).orElse(0L));
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, response.usage().map(CompletionUsage::completionTokens).orElse(0L));
        }
        catch (RuntimeException e) {
            span.setStatus(ERROR, e.getMessage());
            span.recordException(e);
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute AI request", e);
        }
        finally {
            span.end();
        }
        ChatCompletionMessage message = response.choices().stream()
                .map(ChatCompletion.Choice::message)
                .findFirst()
                .orElseThrow(() -> new TrinoException(AI_CLIENT_ERROR, "No response from AI model"));

        if (message.refusal().isPresent()) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + message.refusal());
        }

        return message.content().orElse("");
    }

    private static ChatCompletionAssistantMessageParam createAssistantMessage(String content)
    {
        return ChatCompletionAssistantMessageParam.builder()
                .role(JsonValue.from("assistant"))
                .content(content)
                .build();
    }
}
