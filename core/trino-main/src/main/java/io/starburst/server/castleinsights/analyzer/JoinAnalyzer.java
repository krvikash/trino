/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.analyzer;

import com.google.common.collect.ImmutableList;
import io.starburst.server.castleinsights.CastleQueryAnalysis;
import io.starburst.server.castleinsights.CastleQueryAnalysis.ColumnRef;
import io.starburst.server.castleinsights.model.Cardinality;
import io.starburst.server.castleinsights.model.CastleInsight;
import io.starburst.server.castleinsights.model.JoinInsight;
import io.starburst.server.castleinsights.model.JoinType;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.operator.OperatorStats;
import io.trino.operator.join.JoinOperatorInfo;
import io.trino.operator.join.LookupOuterOperator;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.castleinsights.model.Cardinality.MANY_TO_MANY;
import static io.starburst.server.castleinsights.model.Cardinality.MANY_TO_ONE;
import static io.starburst.server.castleinsights.model.Cardinality.ONE_TO_MANY;
import static io.starburst.server.castleinsights.model.Cardinality.ONE_TO_ONE;
import static java.util.Arrays.stream;

/**
 * Emits a {@link JoinInsight} for every equi-join between two tables, tracing each join key back to
 * the column it came from and classifying the relationship between the two sides.
 */
public class JoinAnalyzer
        implements CastleInsightAnalyzer
{
    private static final String LOOKUP_OUTER_OPERATOR = LookupOuterOperator.class.getSimpleName();

    @Override
    public List<CastleInsight> analyze(CastleQueryAnalysis analysis)
    {
        ImmutableList.Builder<CastleInsight> insights = ImmutableList.builder();
        for (JoinNode join : analysis.planNodes(JoinNode.class)) {
            analyze(analysis, join).ifPresent(insights::add);
        }
        return insights.build();
    }

    private Optional<JoinInsight> analyze(CastleQueryAnalysis analysis, JoinNode join)
    {
        if (join.getCriteria().isEmpty()) {
            // Cross joins and pure non-equi joins carry no columns to attribute.
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnRef> leftColumns = ImmutableList.builder();
        ImmutableList.Builder<ColumnRef> rightColumns = ImmutableList.builder();
        for (EquiJoinClause clause : join.getCriteria()) {
            Optional<ColumnRef> left = analysis.traceColumn(join.getLeft(), clause.getLeft());
            Optional<ColumnRef> right = analysis.traceColumn(join.getRight(), clause.getRight());
            if (left.isEmpty() || right.isEmpty()) {
                // A clause over an expression rather than a plain column; skip the whole join rather
                // than report a partial key.
                return Optional.empty();
            }
            leftColumns.add(left.get());
            rightColumns.add(right.get());
        }

        List<ColumnRef> left = leftColumns.build();
        List<ColumnRef> right = rightColumns.build();
        Optional<TableRef> leftTable = singleTable(left);
        Optional<TableRef> rightTable = singleTable(right);
        if (leftTable.isEmpty() || rightTable.isEmpty()) {
            // A side spanning more than one table (e.g. a join under a join); not a two-table join.
            return Optional.empty();
        }

        return Optional.of(new JoinInsight(
                leftTable.get(),
                columnNames(left),
                rightTable.get(),
                columnNames(right),
                joinType(join.getType()),
                cardinality(analysis, join)));
    }

    /**
     * Classifies the relationship from the join's match histogram and measured row counts.
     * <p>
     * The histogram records, per probe (left) row, how many build (right) rows it matched, so a probe
     * row matching two or more build rows proves the right side is not unique on the join key. The left
     * side is decided by {@link #leftUniqueness}, and the two together name the relationship.
     */
    private static Optional<Cardinality> cardinality(CastleQueryAnalysis analysis, JoinNode join)
    {
        Optional<JoinOperatorInfo> joinInfo = joinOperatorInfo(analysis, join);
        if (joinInfo.isEmpty()) {
            return Optional.empty();
        }
        long[] probesByMatchCount = joinInfo.get().getLogHistogramProbes();
        if (sum(probesByMatchCount) == 0) {
            return Optional.empty();
        }
        // A probe row that took the run-length fast path is only recorded once for its whole run, and only
        // ever when it matched no or one build row, so buckets two and up stay faithful.
        boolean rightUnique = sumFrom(probesByMatchCount, 2) == 0;

        return switch (leftUniqueness(analysis, join, joinInfo.get())) {
            case UNIQUE -> Optional.of(rightUnique ? ONE_TO_ONE : ONE_TO_MANY);
            case NON_UNIQUE -> Optional.of(rightUnique ? MANY_TO_ONE : MANY_TO_MANY);
            case UNKNOWN -> Optional.empty();
        };
    }

    /**
     * Whether the left side is unique on the join key, which nothing measures directly: the histogram
     * counts matches per probe row, not per build row.
     * <p>
     * A right or full join answers it exactly all the same. Such a join has to emit the build rows nothing
     * matched, so the engine tracks which build rows were matched and a {@code LookupOuterOperator} turns
     * the rest into output rows. Subtracting those from both the join's output and the build side leaves
     * the number of matches and the number of build rows that took part in one; the left side is unique
     * exactly when they are equal.
     * <p>
     * An inner or left join has no such operator, so a unique left side cannot be told from a non-unique
     * one. Only more matches than build rows still proves non-unique, because some build row must then
     * have been matched by several probe rows.
     */
    private static Uniqueness leftUniqueness(CastleQueryAnalysis analysis, JoinNode join, JoinOperatorInfo joinInfo)
    {
        long buildRows = analysis.outputRowCount(join.getRight().getId());
        long outputRows = analysis.outputRowCount(join.getId());
        JoinType type = joinType(join.getType());

        long preservedProbeRows = 0;
        if (type == JoinType.LEFT || type == JoinType.FULL) {
            if (joinInfo.getRleProbes() > 0) {
                // The unmatched probe rows are read off the histogram's first bucket, which the run-length
                // fast path undercounts: it records one probe for a whole run of equal keys.
                return Uniqueness.UNKNOWN;
            }
            preservedProbeRows = joinInfo.getLogHistogramProbes()[0];
        }

        if (type == JoinType.RIGHT || type == JoinType.FULL) {
            Optional<Long> unmatchedBuildRows = unmatchedBuildRows(analysis, join);
            if (unmatchedBuildRows.isEmpty()) {
                return Uniqueness.UNKNOWN;
            }
            long matches = outputRows - preservedProbeRows - unmatchedBuildRows.get();
            long matchedBuildRows = buildRows - unmatchedBuildRows.get();
            return matches == matchedBuildRows ? Uniqueness.UNIQUE : Uniqueness.NON_UNIQUE;
        }
        return outputRows - preservedProbeRows > buildRows ? Uniqueness.NON_UNIQUE : Uniqueness.UNKNOWN;
    }

    /**
     * The rows a right or full join emitted for build rows nothing matched, reported by the separate
     * {@code LookupOuterOperator} the engine runs under the join's own plan node id. Empty when that
     * operator reported nothing, leaving the matched build rows unknown.
     */
    private static Optional<Long> unmatchedBuildRows(CastleQueryAnalysis analysis, JoinNode join)
    {
        Long unmatched = null;
        for (OperatorStats operator : analysis.operatorStats(join.getId())) {
            if (operator.getOperatorType().equals(LOOKUP_OUTER_OPERATOR)) {
                unmatched = (unmatched == null ? 0L : unmatched) + operator.getOutputPositions();
            }
        }
        return Optional.ofNullable(unmatched);
    }

    private static Optional<JoinOperatorInfo> joinOperatorInfo(CastleQueryAnalysis analysis, JoinNode join)
    {
        JoinOperatorInfo merged = null;
        for (OperatorStats operator : analysis.operatorStats(join.getId())) {
            if (operator.getInfo() instanceof JoinOperatorInfo info) {
                merged = merged == null ? info : merged.mergeWith(info);
            }
        }
        return Optional.ofNullable(merged);
    }

    private static Optional<TableRef> singleTable(List<ColumnRef> columns)
    {
        checkArgument(!columns.isEmpty(), "columns is empty");
        TableRef table = columns.getFirst().table();
        for (ColumnRef column : columns) {
            if (!column.table().equals(table)) {
                return Optional.empty();
            }
        }
        return Optional.of(table);
    }

    private static List<String> columnNames(List<ColumnRef> columns)
    {
        return columns.stream()
                .map(ColumnRef::column)
                .collect(toImmutableList());
    }

    private static long sum(long[] values)
    {
        return sumFrom(values, 0);
    }

    private static long sumFrom(long[] values, int fromIndex)
    {
        return stream(values).skip(fromIndex).sum();
    }

    private static JoinType joinType(io.trino.sql.planner.plan.JoinType type)
    {
        return switch (type) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case FULL -> JoinType.FULL;
        };
    }

    /**
     * Whether a join side holds at most one row per join key, as far as the recorded stats can prove.
     */
    private enum Uniqueness
    {
        UNIQUE,
        NON_UNIQUE,
        UNKNOWN,
    }
}
