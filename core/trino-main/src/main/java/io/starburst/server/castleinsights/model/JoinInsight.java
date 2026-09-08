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
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Two tables joined on one or more equality clauses. The columns are positional:
 * {@code leftColumns.get(i)} is joined with {@code rightColumns.get(i)}.
 * <p>
 * The right side is the build side and the left side the probe side, following the plan the
 * engine settled on. {@code cardinality} is the relationship between them where it could be
 * determined, empty otherwise.
 * <p>
 * {@code outputExpansionRatio} is the join's output row count over its probe (left) input row count:
 * below one where the join filters the probe stream down, above one where it fans it out. Empty where
 * it could not be measured with confidence.
 */
public record JoinInsight(
        TableRef left,
        List<String> leftColumns,
        TableRef right,
        List<String> rightColumns,
        JoinType joinType,
        Optional<Cardinality> cardinality,
        Optional<Double> outputExpansionRatio)
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
        requireNonNull(cardinality, "cardinality is null");
        requireNonNull(outputExpansionRatio, "outputExpansionRatio is null");
        outputExpansionRatio.ifPresent(ratio ->
                checkArgument(Double.isFinite(ratio) && ratio >= 0, "outputExpansionRatio must be finite and non-negative, got %s", ratio));
    }
}
