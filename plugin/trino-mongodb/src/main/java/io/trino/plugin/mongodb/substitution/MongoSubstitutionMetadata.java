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
package io.trino.plugin.mongodb.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.plugin.mongodb.MongoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class MongoSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        MongoTableHandle handle = (MongoTableHandle) queryTable;
        if (!(candidateTable instanceof MongoTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(h -> h.schemaTableName().equals(candidateId.getSchemaTableName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((MongoTableHandle) handle)
                .map(h -> new MongoTableId(h.schemaTableName()));
    }

    /**
     * A handle can drive substitution only when it is a plain named-relation scan with no
     * pushdown. A pushed-down filter, a narrowed constraint, or a limit means the handle no
     * longer represents a full-table scan. projectedColumns is ignored: MongoMetadata always
     * populates it and it does not affect table identity (same rationale as BigQuery).
     */
    private static Optional<MongoTableHandle> substitutionCandidate(MongoTableHandle handle)
    {
        if (handle.filter().isPresent() || !handle.constraint().isAll() || handle.limit().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        MongoColumnHandle handle = (MongoColumnHandle) column;
        return Optional.of(new MongoColumnId(handle.baseName(), handle.dereferenceNames()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(MongoTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(MongoColumnId.VERSION);
    }
}
