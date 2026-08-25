/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.model;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Two tables joined on one or more equality clauses. The columns are positional:
 * {@code leftColumns.get(i)} is joined with {@code rightColumns.get(i)}.
 * <p>
 * The right side is the build side and the left side the probe side, following the plan the
 * engine settled on.
 * <p>
 * The row counts are what the join actually saw and emitted, not estimates: {@code leftRows} and
 * {@code rightRows} on the way in, {@code outputRows} on the way out. Together they give the
 * join's selectivity, and so the cardinality of the relationship between the two tables.
 */
public record JoinInsight(
        TableRef left,
        List<String> leftColumns,
        TableRef right,
        List<String> rightColumns,
        JoinType joinType,
        long leftRows,
        long rightRows,
        long outputRows)
        implements CastleInsight
{
    public JoinInsight
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        leftColumns = ImmutableList.copyOf(leftColumns);
        rightColumns = ImmutableList.copyOf(rightColumns);
        checkArgument(!leftColumns.isEmpty(), "leftColumns is empty");
        checkArgument(leftColumns.size() == rightColumns.size(), "leftColumns and rightColumns have different sizes");
        requireNonNull(joinType, "joinType is null");
        checkArgument(leftRows >= 0, "leftRows is negative");
        checkArgument(rightRows >= 0, "rightRows is negative");
        checkArgument(outputRows >= 0, "outputRows is negative");
    }
}
