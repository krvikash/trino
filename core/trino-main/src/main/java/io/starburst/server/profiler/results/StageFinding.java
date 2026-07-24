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

public record StageFinding(String name, String stageId, String description)
        implements RuleFinding
{
    public StageFinding
    {
        requireNonNull(name, "name is null");
        requireNonNull(stageId, "stageId is null");
        requireNonNull(description, "description is null");
    }
}
