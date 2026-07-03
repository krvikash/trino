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

import static java.util.concurrent.TimeUnit.MINUTES;

public class MaterializedViewSubstitutionConfig
{
    private boolean materializedViewSubstitutionSupportEnabled;
    private boolean materializedViewSubstitutionEnabled;
    private Duration materializedViewSubstitutionMetastoreRefreshInterval = new Duration(1, MINUTES);
    private MaterializationMetastoreType materializationMetastoreType = MaterializationMetastoreType.IN_MEMORY;

    public enum MaterializationMetastoreType
    {
        IN_MEMORY,
        REST,
    }

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

    public MaterializationMetastoreType getMaterializationMetastoreType()
    {
        return materializationMetastoreType;
    }

    @Config("materialization.metastore.type")
    @ConfigDescription("Backing store for the materialization metastore: IN_MEMORY or REST")
    public MaterializedViewSubstitutionConfig setMaterializationMetastoreType(MaterializationMetastoreType materializationMetastoreType)
    {
        this.materializationMetastoreType = materializationMetastoreType;
        return this;
    }
}
