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
package io.trino.plugin.thrift.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.thrift.ThriftColumnHandle;
import io.trino.plugin.thrift.ThriftTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class ThriftSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        ThriftTableHandle handle = (ThriftTableHandle) queryTable;
        if (!(candidateTable instanceof ThriftTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schemaName().equals(candidateId.getSchemaName()) && candidate.tableName().equals(candidateId.getTableName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((ThriftTableHandle) handle)
                .map(candidate -> new ThriftTableId(candidate.schemaName(), candidate.tableName()));
    }

    /**
     * A handle can drive substitution only when it is a plain full-table scan. The Thrift connector
     * pushes a predicate into {@code constraint}; a non-{@code all()} constraint means the handle no
     * longer represents the full table, so it is not a candidate. Projected columns
     * ({@code desiredColumns}) are ignored: they do not affect table identity.
     */
    private static Optional<ThriftTableHandle> substitutionCandidate(ThriftTableHandle handle)
    {
        if (!handle.constraint().isAll()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        ThriftColumnHandle handle = (ThriftColumnHandle) column;
        return Optional.of(new ThriftColumnId(handle.columnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(ThriftTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(ThriftColumnId.VERSION);
    }
}
