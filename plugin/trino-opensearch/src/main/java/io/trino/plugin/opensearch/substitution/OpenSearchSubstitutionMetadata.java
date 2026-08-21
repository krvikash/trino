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
package io.trino.plugin.opensearch.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class OpenSearchSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        OpenSearchTableHandle handle = (OpenSearchTableHandle) queryTable;
        if (!(candidateTable instanceof OpenSearchTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schema().equals(candidateId.getSchema()) && candidate.index().equals(candidateId.getIndex()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((OpenSearchTableHandle) handle)
                .map(candidate -> new OpenSearchTableId(candidate.schema(), candidate.index()));
    }

    /**
     * A handle can drive substitution only when it is a plain full-index scan. OpenSearch pushes
     * predicates, term regexes, a raw query and a limit into the handle; any of those means the
     * handle no longer represents the full index, so it is not a candidate. An aggregation-pushed
     * handle carries {@code Type.AGGREGATION} and a raw-query table {@code Type.QUERY} is a
     * synthetic passthrough, so both are rejected by requiring {@code Type.SCAN}. Projected columns
     * are ignored: they do not affect table identity.
     */
    private static Optional<OpenSearchTableHandle> substitutionCandidate(OpenSearchTableHandle handle)
    {
        if (handle.type() != OpenSearchTableHandle.Type.SCAN) {
            return Optional.empty();
        }
        if (!handle.constraint().isAll() || !handle.regexes().isEmpty() || handle.query().isPresent() || handle.limit().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        OpenSearchColumnHandle handle = (OpenSearchColumnHandle) column;
        return Optional.of(new OpenSearchColumnId(handle.path()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(OpenSearchTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(OpenSearchColumnId.VERSION);
    }
}
