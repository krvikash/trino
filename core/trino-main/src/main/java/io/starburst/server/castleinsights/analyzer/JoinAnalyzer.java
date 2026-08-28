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
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.castleinsights.model.Cardinality.MANY_TO_MANY;
import static io.starburst.server.castleinsights.model.Cardinality.MANY_TO_ONE;

/**
 * Emits a {@link JoinInsight} for every equi-join between two tables, tracing each join key back to
 * the column it came from and classifying the relationship between the two sides.
 */
public class JoinAnalyzer
        implements CastleInsightAnalyzer
{
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
     * The histogram records, per probe (left) row, how many build (right) rows it matched, so it
     * tells us whether the right side is unique on the join key. A probe row matching two or more
     * build rows proves it is not. The output-vs-build row comparison then proves whether the left
     * side is non-unique too: more output rows than build rows means a build row was matched by
     * several probe rows.
     * <p>
     * That row comparison only holds for an inner join. An outer join's output also carries the
     * unmatched preserved rows, so more output than build rows says nothing about matching; for a
     * non-unique right side we cannot then prove the left side, and the cardinality is absent.
     * <p>
     * {@code ONE_TO_MANY} needs the left side's uniqueness, which nothing measures cheaply, so the
     * right-non-unique / left-not-proven case is absent rather than a guess.
     */
    private static Optional<Cardinality> cardinality(CastleQueryAnalysis analysis, JoinNode join)
    {
        Optional<JoinOperatorInfo> joinInfo = joinOperatorInfo(analysis, join);
        if (joinInfo.isEmpty()) {
            return Optional.empty();
        }
        long[] probesByMatchCount = joinInfo.get().getLogHistogramProbes();
        long recordedProbes = sum(probesByMatchCount);
        if (recordedProbes == 0) {
            return Optional.empty();
        }
        if (joinInfo.get().getRleProbes() > recordedProbes) {
            // Too much of the probe side went through the run-length fast path, which does not record
            // per-row match counts, to trust the histogram.
            return Optional.empty();
        }

        boolean rightNonUnique = sumFrom(probesByMatchCount, 2) > 0;
        if (!rightNonUnique) {
            return Optional.of(MANY_TO_ONE);
        }
        // More output rows than build rows means a build row was matched by several probe rows, which
        // is only possible when the probe (left) side is not unique on the join key either. Only sound
        // for an inner join, whose every output row is a match; an outer join's preserved unmatched
        // rows would break the comparison.
        boolean leftNonUnique = join.getType() == io.trino.sql.planner.plan.JoinType.INNER
                && analysis.outputRowCount(join.getId()) > analysis.outputRowCount(join.getRight().getId());
        if (leftNonUnique) {
            return Optional.of(MANY_TO_MANY);
        }
        return Optional.empty();
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
        long total = 0;
        for (int i = fromIndex; i < values.length; i++) {
            total += values[i];
        }
        return total;
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
}
