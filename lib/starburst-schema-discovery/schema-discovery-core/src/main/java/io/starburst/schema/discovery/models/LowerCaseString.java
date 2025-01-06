/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public record LowerCaseString(String string)
{
    public LowerCaseString
    {
        requireNonNull(string, "string is null");
    }

    @JsonCreator
    public static LowerCaseString toLowerCase(String name)
    {
        return new LowerCaseString(name);
    }

    @JsonValue
    @Override
    public String string()
    {
        return string.toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString()
    {
        return string();
    }

    @JsonIgnore
    public String getOriginalString()
    {
        return string;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LowerCaseString that = (LowerCaseString) o;
        return Objects.equals(string(), that.string());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(string());
    }
}
