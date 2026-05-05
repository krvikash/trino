/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public enum OracleParallelismType
{
    NO_PARALLELISM,
    PARTITIONS,
    /**/;

    public static OracleParallelismType fromString(String value)
    {
        return switch (requireNonNull(value, "value is null").toLowerCase(ENGLISH)) {
            case "no_concurrency", "no_parallelism" -> NO_PARALLELISM;
            case "partitions" -> PARTITIONS;
            default -> throw new IllegalArgumentException(format("Unrecognized value: '%s'", value));
        };
    }
}
