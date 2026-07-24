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

public record ColumnIndexUsage(String columnName, long indexUsedCount, long totalReads, double usageRatio)
{
    public ColumnIndexUsage
    {
        requireNonNull(columnName, "columnName is null");
    }
}
