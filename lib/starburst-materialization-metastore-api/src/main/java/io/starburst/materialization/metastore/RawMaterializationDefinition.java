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
package io.starburst.materialization.metastore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Opaque storage record for a materialization.
 * The metastore implementation does not interpret {@code computationPlanRootJson} or {@code irVersions}; both are
 * written and read as-is by the layer above.
 *
 * @param irVersions map from IR class TYPE_NAME to the version stamped at write time
 * @param catalogIrVersions {@code ConnectorTableId} and {@code ConnectorColumnId} versions used by each catalog used by the MV query
 * @param computationPlanRootJson serialized IR tree (an {@code Output} JSON document); opaque to this layer
 * @param storageTableId see {@link StorageTableId}
 * @param source see {@link MaterializationSource}
 * @param lastKnownFreshTime see {@link MaterializationDefinition}
 * @param gracePeriod see {@link MaterializationDefinition}
 */
public record RawMaterializationDefinition(
        Map<String, Integer> irVersions,
        Map<CatalogName, ConnectorIdVersions> catalogIrVersions,
        String computationPlanRootJson,
        StorageTableId storageTableId,
        MaterializationSource source,
        Instant lastKnownFreshTime,
        Optional<Duration> gracePeriod)
{
    public RawMaterializationDefinition
    {
        irVersions = ImmutableMap.copyOf(irVersions);
        catalogIrVersions = ImmutableMap.copyOf(catalogIrVersions);
        requireNonNull(computationPlanRootJson, "computationPlanRootJson is null");
        requireNonNull(storageTableId, "storageTableId is null");
        requireNonNull(source, "source is null");
        requireNonNull(lastKnownFreshTime, "lastKnownFreshTime is null");
        requireNonNull(gracePeriod, "gracePeriod is null");
    }

    public RawMaterializationDefinition renamedTo(CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        return new RawMaterializationDefinition(
                irVersions,
                catalogIrVersions,
                computationPlanRootJson,
                targetStorageTableId,
                new MaterializedViewSource(target),
                lastKnownFreshTime,
                gracePeriod);
    }

    public record ConnectorIdVersions(Set<ConnectorIdVersion> tableIdVersions, Set<ConnectorIdVersion> columnIdVersions)
    {
        public ConnectorIdVersions
        {
            tableIdVersions = ImmutableSet.copyOf(requireNonNull(tableIdVersions, "tableIdVersions is null"));
            columnIdVersions = ImmutableSet.copyOf(requireNonNull(columnIdVersions, "columnIdVersions is null"));
        }

        public ConnectorIdVersions add(ConnectorIdVersion tableIdVersion, Optional<ConnectorIdVersion> columnIdVersion)
        {
            return new ConnectorIdVersions(
                    ImmutableSet.<ConnectorIdVersion>builder().addAll(tableIdVersions).add(tableIdVersion).build(),
                    columnIdVersion
                            .map(idVersion -> (Set<ConnectorIdVersion>) ImmutableSet.<ConnectorIdVersion>builder().addAll(columnIdVersions).add(idVersion).build())
                            .orElse(columnIdVersions));
        }
    }
}
