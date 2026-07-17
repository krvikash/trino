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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.MINUTES;

public class MaterializedViewSubstitutionConfig
{
    private boolean materializedViewSubstitutionSupportEnabled;
    private boolean materializedViewSubstitutionEnabled;
    private Duration materializedViewSubstitutionMetastoreRefreshInterval = new Duration(1, MINUTES);
    private Optional<Duration> materializedViewSubstitutionMaxStaleness = Optional.empty();
    private Optional<String> materializedViewSubstitutionCandidatesRegexFilter = Optional.empty();

    public boolean isMaterializedViewSubstitutionSupportEnabled()
    {
        return materializedViewSubstitutionSupportEnabled;
    }

    @Config("materialized-view-substitution.support.enabled")
    @ConfigDescription("Enable materialized view substitution feature. When false, no substitution wiring is installed and queries are never rewritten to read from MV storage tables.")
    public MaterializedViewSubstitutionConfig setMaterializedViewSubstitutionSupportEnabled(boolean value)
    {
        this.materializedViewSubstitutionSupportEnabled = value;
        return this;
    }

    public boolean isMaterializedViewSubstitutionEnabled()
    {
        return materializedViewSubstitutionEnabled;
    }

    @Config("materialized-view-substitution.enabled")
    @ConfigDescription("Enables materialized view substitution for the query by default")
    public MaterializedViewSubstitutionConfig setMaterializedViewSubstitutionEnabled(boolean value)
    {
        this.materializedViewSubstitutionEnabled = value;
        return this;
    }

    @MinDuration("1s")
    public Duration getMaterializedViewSubstitutionMetastoreRefreshInterval()
    {
        return materializedViewSubstitutionMetastoreRefreshInterval;
    }

    @Config("materialized-view-substitution.metastore-refresh-interval")
    @ConfigDescription("How often the in-memory materialization index is rebuilt from the underlying materialization metastore, picking up changes made by other clusters")
    public MaterializedViewSubstitutionConfig setMaterializedViewSubstitutionMetastoreRefreshInterval(Duration value)
    {
        this.materializedViewSubstitutionMetastoreRefreshInterval = value;
        return this;
    }

    @NotNull
    public Optional<Duration> getMaterializedViewSubstitutionMaxStaleness()
    {
        return materializedViewSubstitutionMaxStaleness;
    }

    @Config("materialized-view-substitution.max-staleness")
    @ConfigDescription("Maximum staleness of a materialized view eligible for substitution; unset means only each materialized view's own grace period applies")
    public MaterializedViewSubstitutionConfig setMaterializedViewSubstitutionMaxStaleness(Duration value)
    {
        this.materializedViewSubstitutionMaxStaleness = Optional.ofNullable(value);
        return this;
    }

    @NotNull
    public Optional<String> getMaterializedViewSubstitutionCandidatesRegexFilter()
    {
        return materializedViewSubstitutionCandidatesRegexFilter;
    }

    @Config("materialized-view-substitution.candidates-regex-filter")
    @ConfigDescription("Regular expression matched against the fully qualified materialized view name (catalog.schema.table); unset means all materialized views are eligible for substitution")
    public MaterializedViewSubstitutionConfig setMaterializedViewSubstitutionCandidatesRegexFilter(String value)
    {
        // Compile eagerly so an invalid pattern fails at startup rather than at query time
        this.materializedViewSubstitutionCandidatesRegexFilter = Optional.ofNullable(value).map(pattern -> {
            Pattern.compile(pattern);
            return pattern;
        });
        return this;
    }
}
