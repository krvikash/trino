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
package io.trino.plugin.pinot.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class PinotSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        PinotTableHandle handle = (PinotTableHandle) queryTable;
        if (!(candidateTable instanceof PinotTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schemaName().equals(candidateId.getSchema()) && candidate.tableName().equals(candidateId.getTable()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((PinotTableHandle) handle)
                .map(candidate -> new PinotTableId(candidate.schemaName(), candidate.tableName()));
    }

    /**
     * A handle can drive substitution only when it is a plain full-table scan. Pinot pushes a filter
     * ({@code constraint}), a {@code limit} and a raw/aggregation {@code query} (a {@code DynamicTable})
     * into the handle; any of those means the handle no longer represents the full table, so it is not a
     * candidate. {@code enableNullHandling} is intrinsic to the table (derived from its schema) and does
     * not affect identity.
     */
    private static Optional<PinotTableHandle> substitutionCandidate(PinotTableHandle handle)
    {
        if (!handle.constraint().isAll() || handle.limit().isPresent() || handle.query().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        PinotColumnHandle handle = (PinotColumnHandle) column;
        return Optional.of(new PinotColumnId(handle.getColumnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(PinotTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(PinotColumnId.VERSION);
    }
}
