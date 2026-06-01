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
package io.trino.plugin.iceberg.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class IcebergSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        IcebergTableHandle queryTableHandle = (IcebergTableHandle) queryTable;
        if (!(candidateTable instanceof IcebergTableId candidateTableId)) {
            return false;
        }
        return isSubstitutionCandidate(queryTableHandle)
                && queryTableHandle.getSchemaName().equals(candidateTableId.schemaName())
                && queryTableHandle.getTableName().equals(candidateTableId.tableName())
                // Reject if the candidate captures a different physical source table (drop+recreate
                // of the source under iceberg.unique-table-location=false reuses the on-disk
                // location, so schema+name+location can collide).
                && queryTableHandle.getTableUuid().equals(candidateTableId.tableUuid());
    }

    private static boolean isSubstitutionCandidate(IcebergTableHandle table)
    {
        return table.getLimit().isEmpty()
                && table.getEnforcedPredicate().isAll()
                && table.getBranch().isEmpty()
                && table.getSortOrderId().isEmpty()
                // FOR VERSION/TIMESTAMP AS OF pins a user-chosen snapshot; substituting the MV
                // storage would potentially silently return other data instead of the requested snapshot.
                && !table.isVersionPinnedByQuery();
    }

    @Override
    public Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle storageTableHandle)
    {
        // The storage table's current snapshot id is the substitution identity: substitution is
        // allowed only while the storage table sits on the exact snapshot that was captured when
        // the materialization was indexed. A refresh advances the snapshot, so a divergent (e.g.
        // cross-cluster) refresh stops substitution until the index is rebuilt; the local refresh
        // path re-captures the new snapshot. A storage table with no snapshot (never refreshed)
        // returns empty, so the engine skips indexing rather than failing the user's ALTER/REFRESH.
        IcebergTableHandle handle = (IcebergTableHandle) storageTableHandle;
        OptionalLong snapshotId = handle.getSnapshotId();
        if (snapshotId.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorStorageTableId(
                handle.getSchemaName(),
                handle.getTableName(),
                Long.toString(snapshotId.getAsLong())));
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle;
        if (!isSubstitutionCandidate(tableHandle)) {
            return Optional.empty();
        }

        return Optional.of(new IcebergTableId(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getTableUuid()));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) column;
        return Optional.of(new IcebergColumnId(columnHandle.getId(), columnHandle.getPath()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(IcebergTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(IcebergColumnId.VERSION);
    }
}
