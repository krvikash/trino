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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.execution.StageInfo;
import io.trino.execution.TableInfo;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.eventlistener.MaterializedViewSubstitutionInfo;
import io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.SUBSTITUTED;
import static java.util.Objects.requireNonNull;

/**
 * Trino-main's internal representation of a materialized-view substitution candidate, from which
 * {@link #toEventInfo()} derives the {@link MaterializedViewSubstitutionInfo} surfaced to the event
 * listener. Carries the same flat fields as the SPI record, plus (for a {@code SUBSTITUTED} row) the
 * query-side {@link PlanNode} subtree the materialized view replaced.
 * Unlike the SPI record, which renders that subtree to opaque text, {@code replacedPlan} is included
 * as-is in this class's JSON (e.g. via {@code GET /v1/query/{queryId}}), same as {@link StageInfo}.
 */
public record MaterializedViewSubstitution(
        Optional<CatalogSchemaTableName> materializedView,
        MaterializedViewSubstitutionOutcome outcome,
        Optional<CatalogSchemaTableName> storageTable,
        Optional<Instant> lastRefreshTime,
        Optional<ReplacedPlan> replacedPlan)
{
    public MaterializedViewSubstitution
    {
        requireNonNull(materializedView, "materializedView is null");
        requireNonNull(outcome, "outcome is null");
        requireNonNull(storageTable, "storageTable is null");
        requireNonNull(lastRefreshTime, "lastRefreshTime is null");
        requireNonNull(replacedPlan, "replacedPlan is null");
        // A SUBSTITUTED row may legitimately have no replacedPlan: withoutReplacedPlan() drops it once the
        // query ages out of QueryTracker's retained-history window (see QueryStateMachine.pruneQueryInfo()),
        // so the plan isn't held onto in memory indefinitely.
        checkArgument(outcome == SUBSTITUTED || replacedPlan.isEmpty(), "only a SUBSTITUTED row may carry a replacedPlan");
    }

    /**
     * The query-side subtree a materialized view replaced, with the table metadata needed to render
     * it outside a transaction.
     */
    public record ReplacedPlan(PlanNode root, Map<PlanNodeId, TableInfo> tableInfos)
    {
        public ReplacedPlan
        {
            requireNonNull(root, "root is null");
            tableInfos = ImmutableMap.copyOf(requireNonNull(tableInfos, "tableInfos is null"));
        }
    }

    @JsonCreator
    public static MaterializedViewSubstitution fromJson(
            @JsonProperty("materializedView") Optional<CatalogSchemaTableName> materializedView,
            @JsonProperty("outcome") MaterializedViewSubstitutionOutcome outcome,
            @JsonProperty("storageTable") Optional<CatalogSchemaTableName> storageTable,
            @JsonProperty("lastRefreshTime") Optional<Instant> lastRefreshTime,
            @JsonProperty("replacedPlan") Optional<ReplacedPlan> replacedPlan)
    {
        return new MaterializedViewSubstitution(materializedView, outcome, storageTable, lastRefreshTime, replacedPlan);
    }

    public MaterializedViewSubstitutionInfo toEventInfo()
    {
        return new MaterializedViewSubstitutionInfo(materializedView, outcome, storageTable, lastRefreshTime, Optional.empty());
    }

    public MaterializedViewSubstitution withoutReplacedPlan()
    {
        return new MaterializedViewSubstitution(materializedView, outcome, storageTable, lastRefreshTime, Optional.empty());
    }
}
