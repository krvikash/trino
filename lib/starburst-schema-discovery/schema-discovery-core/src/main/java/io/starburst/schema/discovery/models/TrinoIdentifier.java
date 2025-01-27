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

import java.util.Objects;

import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static java.util.Objects.requireNonNull;

record TrinoIdentifier(LowerCaseString name)
        implements DiscoveredIdentifier
{
    TrinoIdentifier
    {
        requireNonNull(name, "name is null");
    }

    TrinoIdentifier(String name)
    {
        this(toLowerCase(name));
    }

    static TrinoIdentifier toTrinoIdentifier(String name)
    {
        return new TrinoIdentifier(name);
    }

    @Override
    public String string()
    {
        return name.string();
    }

    @Override
    public String toString()
    {
        return string();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null) {
            return false;
        }
        if (o instanceof DiscoveredIdentifier that) {
            return Objects.equals(this.string(), that.string());
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
