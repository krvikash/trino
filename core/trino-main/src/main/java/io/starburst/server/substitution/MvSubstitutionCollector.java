/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.starburst.server.substitution.MaterializedViewSubstitution.ReplacedPlan;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Per-query record of every materialized-view candidate evaluated during {@link MvSubstitutionOptimizer}
 * planning: both the substitution that was made (with the query-side subtree it replaced) and every
 * near-miss candidate that was rejected. Substituted and rejected rows are bounded independently, each
 * capped at {@link #MAX_RECORDED_ROWS_PER_KIND}, so a rejection-heavy query cannot crowd out the
 * substituted row.
 * Rows are written while planning and read once planning has finished; both are synchronized so the
 * recorded list is safely published to the reader.
 */
@ThreadSafe
public final class MvSubstitutionCollector
{
    private static final int MAX_RECORDED_ROWS_PER_KIND = 100;

    public static final MvSubstitutionCollector NOOP = new MvSubstitutionCollector(0);

    private final int maxRecordsPerKind;
    @GuardedBy("this")
    private final List<MaterializedViewSubstitution> substitutions = new ArrayList<>();
    @GuardedBy("this")
    private int substitutedCount;
    @GuardedBy("this")
    private int rejectedCount;

    public MvSubstitutionCollector()
    {
        this(MAX_RECORDED_ROWS_PER_KIND);
    }

    private MvSubstitutionCollector(int maxRecordsPerKind)
    {
        this.maxRecordsPerKind = maxRecordsPerKind;
    }

    public void recordSubstituted(
            CatalogSchemaTableName materializedView,
            CatalogSchemaTableName storageTable,
            Instant lastRefreshTime,
            ReplacedPlan replacedPlan)
    {
        record(new MaterializedViewSubstitution(
                Optional.of(materializedView),
                MaterializedViewSubstitutionOutcome.SUBSTITUTED,
                Optional.of(storageTable),
                Optional.of(lastRefreshTime),
                Optional.of(replacedPlan)));
    }

    public void recordRejected(
            Optional<CatalogSchemaTableName> materializedView,
            MaterializedViewSubstitutionOutcome outcome)
    {
        record(new MaterializedViewSubstitution(
                materializedView,
                outcome,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    private synchronized void record(MaterializedViewSubstitution row)
    {
        if (row.outcome() == MaterializedViewSubstitutionOutcome.SUBSTITUTED) {
            if (substitutedCount >= maxRecordsPerKind) {
                return;
            }
            substitutedCount++;
        }
        else {
            if (rejectedCount >= maxRecordsPerKind) {
                return;
            }
            rejectedCount++;
        }
        substitutions.add(row);
    }

    public synchronized List<MaterializedViewSubstitution> getSubstitutions()
    {
        return ImmutableList.copyOf(substitutions);
    }
}
