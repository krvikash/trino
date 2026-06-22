/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Represents an error encountered while validating an OpenAPI description.
 * Carries a list of path segments that locate the error within the document
 * (e.g. {@code paths./pets.get.responses.200.content.application/json.schema.properties}).
 */
public class SpecException
        extends Exception
{
    private final List<String> path;
    private final String text;

    public SpecException(String text)
    {
        this(text, null, emptyList());
    }

    private SpecException(String text, Throwable cause, List<String> path)
    {
        super(formatMessage(path, text), cause);
        this.text = requireNonNull(text, "text is null");
        this.path = requireNonNull(path, "path is null");
    }

    public List<String> path()
    {
        return path;
    }

    public String text()
    {
        return text;
    }

    private static String formatMessage(List<String> path, String text)
    {
        if (path.isEmpty()) {
            return text;
        }
        return "%s: %s".formatted(String.join(".", path), text);
    }

    public SpecException fromMember(String member)
    {
        return new SpecException(
                text,
                getCause(),
                ImmutableList.<String>builderWithExpectedSize(path.size() + 1)
                        .add(member)
                        .addAll(path)
                        .build());
    }

    public SpecException fromPath(List<String> members)
    {
        return new SpecException(
                text,
                getCause(),
                ImmutableList.<String>builderWithExpectedSize(path.size() + members.size())
                        .addAll(members)
                        .addAll(path)
                        .build());
    }

    public SpecException withCause(Throwable cause)
    {
        return new SpecException(text, cause, path);
    }
}
