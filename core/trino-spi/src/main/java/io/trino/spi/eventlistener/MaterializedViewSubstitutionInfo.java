/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.eventlistener;

import io.trino.spi.connector.CatalogSchemaTableName;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * One row per materialized view candidate considered during materialized-view substitution. Flat so
 * telemetry can {@code count by outcome} without unnesting.
 * <p>
 * {@code replacedPlanText} is a rendered-text snapshot of the query-side subtree the candidate replaced,
 * present only for a {@code SUBSTITUTED} row. Carrying it as opaque text keeps this record's shape
 * independent of how complex the substituted subtree is.
 * <p>
 * This class is JSON serializable for convenience; serialization compatibility is not guaranteed across versions.
 */
public record MaterializedViewSubstitutionInfo(
        Optional<CatalogSchemaTableName> materializedView,
        MaterializedViewSubstitutionOutcome outcome,
        Optional<CatalogSchemaTableName> storageTable,
        Optional<Instant> lastRefreshTime,
        Optional<String> replacedPlanText)
{
    public MaterializedViewSubstitutionInfo
    {
        requireNonNull(materializedView, "materializedView is null");
        requireNonNull(outcome, "outcome is null");
        requireNonNull(storageTable, "storageTable is null");
        requireNonNull(lastRefreshTime, "lastRefreshTime is null");
        requireNonNull(replacedPlanText, "replacedPlanText is null");
    }
}
