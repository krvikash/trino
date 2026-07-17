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
package io.trino.plugin.cassandra.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.cassandra.CassandraColumnHandle;
import io.trino.plugin.cassandra.CassandraNamedRelationHandle;
import io.trino.plugin.cassandra.CassandraTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class CassandraSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        CassandraTableHandle queryTableHandle = (CassandraTableHandle) queryTable;
        if (!(candidateTable instanceof CassandraTableId candidateTableId)) {
            return false;
        }
        return substitutionCandidate(queryTableHandle)
                .map(namedRelation -> namedRelation.getSchemaTableName().equals(candidateTableId.getSchemaTableName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((CassandraTableHandle) handle)
                .map(namedRelation -> new CassandraTableId(namedRelation.getSchemaTableName()));
    }

    /**
     * A handle can drive substitution only when it is a plain named-relation scan with no
     * pushdown. Cassandra pushes partition-key and clustering-key predicates into the relation
     * handle; such a handle no longer represents a full-table scan, so it is not a candidate
     * (mirrors the JDBC connector returning empty for handles that contain pushdowns).
     */
    private static Optional<CassandraNamedRelationHandle> substitutionCandidate(CassandraTableHandle tableHandle)
    {
        if (tableHandle.isSynthetic()) {
            return Optional.empty();
        }
        CassandraNamedRelationHandle namedRelation = tableHandle.getRequiredNamedRelation();
        if (namedRelation.getPartitions().isPresent() || !namedRelation.getClusteringKeyPredicates().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(namedRelation);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        CassandraColumnHandle columnHandle = (CassandraColumnHandle) column;
        return Optional.of(new CassandraColumnId(columnHandle.name(), columnHandle.getType()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(CassandraTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(CassandraColumnId.VERSION);
    }
}
