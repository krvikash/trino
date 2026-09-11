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

import io.opentelemetry.api.common.AttributeKey;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

/**
 * GenAI semantic convention attribute keys and values.
 * <p>
 * These mirror the conventions that used to live in {@code io.opentelemetry.semconv.incubating}
 * before they were moved out of that artifact to the
 * <a href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI
 * semantic conventions repository</a>, which does not yet publish a Maven artifact. They are
 * defined locally so we no longer depend on the deprecated constants while keeping the emitted
 * attribute names unchanged.
 */
public final class GenAiAttributes
{
    private GenAiAttributes() {}

    public static final AttributeKey<String> GEN_AI_OPERATION_NAME = stringKey("gen_ai.operation.name");
    public static final AttributeKey<String> GEN_AI_PROVIDER_NAME = stringKey("gen_ai.provider.name");
    public static final AttributeKey<String> GEN_AI_REQUEST_MODEL = stringKey("gen_ai.request.model");
    public static final AttributeKey<Long> GEN_AI_USAGE_INPUT_TOKENS = longKey("gen_ai.usage.input_tokens");
    public static final AttributeKey<Long> GEN_AI_USAGE_OUTPUT_TOKENS = longKey("gen_ai.usage.output_tokens");
    public static final AttributeKey<Long> GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS = longKey("gen_ai.usage.cache_read_input_tokens");
    public static final AttributeKey<Long> GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS = longKey("gen_ai.usage.cache_creation_input_tokens");
    public static final AttributeKey<Long> GEN_AI_USAGE_REASONING_OUTPUT_TOKENS = longKey("gen_ai.usage.reasoning_output_tokens");
    public static final AttributeKey<String> GEN_AI_PROMPT = stringKey("gen_ai.prompt");
    public static final AttributeKey<String> GEN_AI_COMPLETION = stringKey("gen_ai.completion");

    public static final String OPERATION_NAME_CHAT = "chat";
    public static final String PROVIDER_NAME_OPENAI = "openai";
    public static final String PROVIDER_NAME_AZURE_OPENAI = "azure.ai.openai";
    public static final String PROVIDER_NAME_AWS_BEDROCK = "aws.bedrock";
    public static final String PROVIDER_NAME_VERTEX_AI = "gcp.vertex_ai";

    public static final String EVENT_NAME_PROMPT = "gen_ai.content.prompt";
    public static final String EVENT_NAME_COMPLETION = "gen_ai.content.completion";
}
