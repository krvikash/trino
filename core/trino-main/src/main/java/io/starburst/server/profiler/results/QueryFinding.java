/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.results;

import static java.util.Objects.requireNonNull;

public record QueryFinding(String name, String description)
        implements RuleFinding
{
    public QueryFinding
    {
        requireNonNull(name, "name is null");
        requireNonNull(description, "description is null");
    }
}
