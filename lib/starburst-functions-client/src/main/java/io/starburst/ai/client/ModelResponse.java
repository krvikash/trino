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

public record ModelResponse<T>(T response, Optional<TokenUsage> tokenUsage)
{
    public ModelResponse
    {
        requireNonNull(response, "response is null");
        requireNonNull(tokenUsage, "tokenUsage is null");
    }

    public ModelResponse(T response)
    {
        this(response, Optional.empty());
    }
}
