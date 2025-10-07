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

public record ToolResult<T>(Optional<T> content, Optional<String> error)
{
    public static <T> ToolResult<T> success(T content)
    {
        return new ToolResult<>(Optional.of(requireNonNull(content)), Optional.empty());
    }

    public static <T> ToolResult<T> error(String error)
    {
        return new ToolResult<>(Optional.empty(), Optional.of(requireNonNull(error)));
    }

    public boolean success()
    {
        return error.isEmpty();
    }
}
