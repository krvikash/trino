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

import io.trino.spi.profiler.Severity;

import java.util.Optional;

import static io.trino.spi.profiler.Severity.WARNING;
import static java.util.Objects.requireNonNull;

public record Finding(String description, Severity severity, Optional<String> recommendation)
{
    public Finding
    {
        requireNonNull(description, "description is null");
        requireNonNull(severity, "severity is null");
        requireNonNull(recommendation, "recommendation is null");
    }

    public Finding(String description)
    {
        this(description, WARNING, Optional.empty());
    }
}
