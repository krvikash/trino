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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record TokenUsage(
        long inputTokens,
        long outputTokens,
        long cacheReadInputTokens,
        long cacheCreationInputTokens,
        long reasoningOutputTokens,
        String modelName,
        Optional<String> endpoint,
        ModelType modelType,
        ModelBackend modelBackend)
{
    public TokenUsage
    {
        requireNonNull(modelName, "modelName is null");
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(modelType, "modelType is null");
        requireNonNull(modelBackend, "modelBackend is null");
    }
}
