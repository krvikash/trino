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
package io.trino.plugin.elasticsearch.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;
import io.trino.plugin.elasticsearch.ElasticsearchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class ElasticsearchSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) queryTable;
        if (!(candidateTable instanceof ElasticsearchTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schema().equals(candidateId.getSchema()) && candidate.index().equals(candidateId.getIndex()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((ElasticsearchTableHandle) handle)
                .map(candidate -> new ElasticsearchTableId(candidate.schema(), candidate.index()));
    }

    /**
     * A handle can drive substitution only when it is a plain full-index scan. Elasticsearch pushes
     * predicates, term regexes, a raw query and a limit into the handle; any of those means the
     * handle no longer represents the full index, so it is not a candidate. A raw-query table
     * ({@code Type.QUERY}) is a synthetic passthrough and is never a candidate. Projected columns
     * are ignored: they do not affect table identity.
     */
    private static Optional<ElasticsearchTableHandle> substitutionCandidate(ElasticsearchTableHandle handle)
    {
        if (handle.type() != ElasticsearchTableHandle.Type.SCAN) {
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
        ElasticsearchColumnHandle handle = (ElasticsearchColumnHandle) column;
        return Optional.of(new ElasticsearchColumnId(handle.path()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(ElasticsearchTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(ElasticsearchColumnId.VERSION);
    }
}
