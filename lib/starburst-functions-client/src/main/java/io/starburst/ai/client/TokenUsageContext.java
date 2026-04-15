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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public record TokenUsageContext(String modelId, OperationId operationId, Map<String, String> additionalProperties)
{
    public static final TokenUsageContext EMPTY = new TokenUsageContext("", null, ImmutableMap.of());

    public TokenUsageContext
    {
        requireNonNull(modelId, "modelId is null");
        additionalProperties = ImmutableMap.copyOf(requireNonNull(additionalProperties, "additionalProperties is null"));
    }

    public static TokenUsageContext of(String modelId, OperationId operationId)
    {
        return new TokenUsageContext(modelId, operationId, ImmutableMap.of());
    }
}
