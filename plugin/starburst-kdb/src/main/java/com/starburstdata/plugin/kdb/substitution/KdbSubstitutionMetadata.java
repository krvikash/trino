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
package com.starburstdata.plugin.kdb.substitution;

import com.google.common.collect.ImmutableSet;
import com.starburstdata.plugin.kdb.KdbColumnHandle;
import com.starburstdata.plugin.kdb.KdbTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class KdbSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        KdbTableHandle handle = (KdbTableHandle) queryTable;
        if (!(candidateTable instanceof KdbTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schemaTableName().equals(candidateId.getTableName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((KdbTableHandle) handle)
                .map(candidate -> new KdbTableId(candidate.schemaTableName()));
    }

    private static Optional<KdbTableHandle> substitutionCandidate(KdbTableHandle handle)
    {
        // A pushed-down limit makes the handle a partial scan, not a plain full-table scan.
        if (handle.limit().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        KdbColumnHandle handle = (KdbColumnHandle) column;
        return Optional.of(new KdbColumnId(handle.columnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(KdbTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(KdbColumnId.VERSION);
    }
}
