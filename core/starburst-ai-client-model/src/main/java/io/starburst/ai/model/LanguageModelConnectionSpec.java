/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.model;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public record LanguageModelConnectionSpec(
        String id,
        String modelName,
        Optional<String> description,
        Optional<Integer> maxTokens,
        Optional<Float> temperature,
        Optional<Float> topP,
        boolean useDeveloperForSystemRole,
        Optional<PromptOverrides> prompts,
        Map<LlmTrait, String> traits,
        ConnectionInfo connectionInfo,
        Boolean useResponsesApi,
        Optional<ReasoningEffort> reasoningEffort)
        implements ModelConnectionSpec
{
    public LanguageModelConnectionSpec
    {
        requireNonNull(id, "id is null");
        requireNonNull(modelName, "modelName is null");
        requireNonNull(description, "description is null");
        requireNonNull(maxTokens, "maxTokens is null");
        requireNonNull(temperature, "temperature is null");
        requireNonNull(topP, "topP is null");
        requireNonNull(prompts, "prompts is null");
        traits = requireNonNullElse(traits, Map.of());
        requireNonNull(connectionInfo, "connectionInfo is null");
        if (!(maxTokens.isEmpty() || maxTokens.get() > 0)) {
            throw new IllegalArgumentException("if present maxTokens must be greater than 0");
        }
        if (!(temperature.isEmpty() || temperature.get() >= 0)) {
            throw new IllegalArgumentException("if present temperature must be a positive number");
        }
        if (!(topP.isEmpty() || (topP.get() >= 0 && topP.get() <= 1))) {
            throw new IllegalArgumentException("if present top_p must be between 0 and 1");
        }
        useResponsesApi = useResponsesApi != null && useResponsesApi;
        if (useResponsesApi && connectionInfo instanceof ConnectionInfo.AwsBedrockConnectionInfo) {
            throw new IllegalArgumentException("Responses API is not supported for AWS Bedrock connections");
        }
        requireNonNull(reasoningEffort, "reasoningEffort is null");
    }

    public enum ReasoningEffort
    {
        // Thinking off: GPT-5.x, Gemma 4 31B (Cerebras), DeepSeek (Responses API). Sent to Claude as thinking disabled
        none,
        // Some Gemini 3.x Flash and Flash-Lite models, Gemma 4 31B (Gemini API). Rejected by Claude, GPT-5.6 Luna and Gemini 3.8 Flash. DeepSeek's API treats it as low
        minimal,
        // Supported by every model family: Claude, GPT, Gemini (incl. Gemini 4 Argon), DeepSeek, Gemma 4 31B (Cerebras)
        low,
        medium,
        high,
        // Claude (Opus 4.7+, Sonnet 5+, Fable, Haiku 5.5), GPT. DeepSeek's API treats it as high
        xhigh,
        // Claude, GPT (Responses API only), DeepSeek. Gemini 3.8 Flash accepts it although its documentation lists only low to high
        max,
    }
}
