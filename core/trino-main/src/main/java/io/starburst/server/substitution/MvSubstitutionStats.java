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

import io.airlift.stats.CounterStat;
import io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.EnumMap;
import java.util.Map;

import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.ACCESS_DENIED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COLUMN_ACCESS_DENIED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COLUMN_NOT_IN_MATERIALIZED_VIEW;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COLUMN_TYPE_MISMATCH;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COMPUTATION_MISMATCH;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.EXCLUDED_BY_FILTER;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.MAX_STALENESS_EXCEEDED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.SCAN_NOT_SUBSTITUTABLE;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STALE;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_COLUMN_MISSING;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_TABLE_CHANGED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_TABLE_NOT_FOUND;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_TYPE_NOT_COERCIBLE;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.SUBSTITUTED;
import static java.util.Objects.requireNonNull;

/**
 * Per-outcome counters for materialized-view substitution (one {@link CounterStat} each), registered
 * via airlift's {@code MBeanExporter#exportWithGeneratedName} under
 * {@code io.starburst.server.substitution:name=MvSubstitutionStats} whenever a
 * {@code MaterializationIndex} is present. This is not a JMX/RMI scrape: Trino's own openmetrics
 * {@code /metrics} endpoint serves it in Prometheus format. The exported series are
 * named {@code io_starburst_server_substitution_name_MvSubstitutionStats_<GetterName>}, with no
 * {@code JMX_} prefix; since the ObjectName is generated from this class's package and simple name,
 * renaming or moving the class changes those names, and — same as {@code METRIC_NAME}/{@code OUTCOME}
 * in {@link MvSubstitutionUsageMetrics} — the getter names below are the exported contract a metrics
 * pipeline matches on.
 *
 * <p>These count every substitution decision the optimizer reaches while planning, including planning
 * runs that only produce plan text and never execute it (EXPLAIN), the same way the planner's own
 * {@code RuleStats} are recorded for any planning run. A consumer reading these as adoption is reading
 * plan-time decisions, so EXPLAIN traffic is included in the totals.
 *
 * <p>See {@link MvSubstitutionUsageMetrics} for the complementary OpenTelemetry counter: it carries
 * {@code outcome} as a real attribute where this one must encode it in the metric name, but exports
 * only where tracing is enabled, which is why these always-on counters exist alongside it.
 */
public class MvSubstitutionStats
{
    private final Map<MaterializedViewSubstitutionOutcome, CounterStat> counters;

    public MvSubstitutionStats()
    {
        Map<MaterializedViewSubstitutionOutcome, CounterStat> counters = new EnumMap<>(MaterializedViewSubstitutionOutcome.class);
        for (MaterializedViewSubstitutionOutcome outcome : MaterializedViewSubstitutionOutcome.values()) {
            counters.put(outcome, new CounterStat());
        }
        this.counters = counters;
    }

    public void increment(MaterializedViewSubstitutionOutcome outcome)
    {
        counters.get(requireNonNull(outcome, "outcome is null")).update(1);
    }

    @Managed
    @Nested
    public CounterStat getSubstituted()
    {
        return counters.get(SUBSTITUTED);
    }

    @Managed
    @Nested
    public CounterStat getScanNotSubstitutable()
    {
        return counters.get(SCAN_NOT_SUBSTITUTABLE);
    }

    @Managed
    @Nested
    public CounterStat getStale()
    {
        return counters.get(STALE);
    }

    @Managed
    @Nested
    public CounterStat getMaxStalenessExceeded()
    {
        return counters.get(MAX_STALENESS_EXCEEDED);
    }

    @Managed
    @Nested
    public CounterStat getExcludedByFilter()
    {
        return counters.get(EXCLUDED_BY_FILTER);
    }

    @Managed
    @Nested
    public CounterStat getComputationMismatch()
    {
        return counters.get(COMPUTATION_MISMATCH);
    }

    @Managed
    @Nested
    public CounterStat getColumnNotInMaterializedView()
    {
        return counters.get(COLUMN_NOT_IN_MATERIALIZED_VIEW);
    }

    @Managed
    @Nested
    public CounterStat getColumnTypeMismatch()
    {
        return counters.get(COLUMN_TYPE_MISMATCH);
    }

    @Managed
    @Nested
    public CounterStat getStorageTableNotFound()
    {
        return counters.get(STORAGE_TABLE_NOT_FOUND);
    }

    @Managed
    @Nested
    public CounterStat getStorageTableChanged()
    {
        return counters.get(STORAGE_TABLE_CHANGED);
    }

    @Managed
    @Nested
    public CounterStat getStorageColumnMissing()
    {
        return counters.get(STORAGE_COLUMN_MISSING);
    }

    @Managed
    @Nested
    public CounterStat getStorageTypeNotCoercible()
    {
        return counters.get(STORAGE_TYPE_NOT_COERCIBLE);
    }

    @Managed
    @Nested
    public CounterStat getAccessDenied()
    {
        return counters.get(ACCESS_DENIED);
    }

    @Managed
    @Nested
    public CounterStat getColumnAccessDenied()
    {
        return counters.get(COLUMN_ACCESS_DENIED);
    }
}
