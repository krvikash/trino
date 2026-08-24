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
package io.trino.plugin.prometheus.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.prometheus.PrometheusColumnHandle;
import io.trino.plugin.prometheus.PrometheusTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class PrometheusSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        PrometheusTableHandle handle = (PrometheusTableHandle) queryTable;
        if (!(candidateTable instanceof PrometheusTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schemaName().equals(candidateId.getSchemaName()) && candidate.tableName().equals(candidateId.getTableName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((PrometheusTableHandle) handle)
                .map(candidate -> new PrometheusTableId(candidate.schemaName(), candidate.tableName()));
    }

    private static Optional<PrometheusTableHandle> substitutionCandidate(PrometheusTableHandle handle)
    {
        // A restricting predicate pushed into the handle makes it a non-plain scan, so it cannot drive
        // substitution. An empty or all() predicate is still a full-table scan: applyFilter stamps an
        // all() predicate even for a query without a WHERE clause, so treat that as a plain scan.
        if (handle.predicate().map(predicate -> !predicate.isAll()).orElse(false)) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        PrometheusColumnHandle handle = (PrometheusColumnHandle) column;
        return Optional.of(new PrometheusColumnId(handle.columnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(PrometheusTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(PrometheusColumnId.VERSION);
    }
}
