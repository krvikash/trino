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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record LanguageModelConnectionSpec(
        String id,
        String modelName,
        Optional<String> description,
        Optional<Integer> maxTokens,
        Optional<Float> temperature,
        Optional<Float> topP,
        boolean useDeveloperForSystemRole,
        Optional<PromptOverrides> prompts,
        ConnectionInfo connectionInfo)
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
    }
}
