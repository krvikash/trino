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

import com.google.inject.Inject;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome;

import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Cluster-wide, per-outcome adoption counter for materialized-view substitution. The {@link Meter}
 * singleton this depends on is bound by airlift's {@code OpenTelemetryModule}; it exports over OTLP
 * through the {@code PeriodicMetricReader} that airlift's {@code OpenTelemetryExporterModule}
 * contributes whenever {@code tracing.enabled=true}, and no-ops otherwise.
 *
 * <p>See {@link MvSubstitutionStats} for why the two counters coexist and for the shared counting
 * semantics (both count every substitution decision reached while planning, EXPLAIN included).
 */
public class MvSubstitutionUsageMetrics
{
    // The metric name and the outcome attribute key are the exported contract a metrics pipeline
    // matches on; renaming either breaks that match.
    private static final String METRIC_NAME = "mv_substitution_attempts";

    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");

    private final LongCounter attempts;
    private final Map<MaterializedViewSubstitutionOutcome, Attributes> attributesByOutcome;

    @Inject
    public MvSubstitutionUsageMetrics(Meter meter)
    {
        requireNonNull(meter, "meter is null");
        attempts = meter.counterBuilder(METRIC_NAME).setUnit("1").build();

        Map<MaterializedViewSubstitutionOutcome, Attributes> attributesByOutcome = new EnumMap<>(MaterializedViewSubstitutionOutcome.class);
        for (MaterializedViewSubstitutionOutcome outcome : MaterializedViewSubstitutionOutcome.values()) {
            attributesByOutcome.put(outcome, Attributes.of(OUTCOME, outcome.name()));
        }
        this.attributesByOutcome = attributesByOutcome;
    }

    public void recordAttempt(MaterializedViewSubstitutionOutcome outcome)
    {
        attempts.add(1, attributesByOutcome.get(outcome));
    }
}
