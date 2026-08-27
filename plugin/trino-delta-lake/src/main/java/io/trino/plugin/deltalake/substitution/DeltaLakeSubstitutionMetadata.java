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
package io.trino.plugin.deltalake.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnProjectionInfo;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DeltaLakeSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        DeltaLakeTableHandle queryTableHandle = (DeltaLakeTableHandle) queryTable;
        if (!(candidateTable instanceof DeltaLakeTableId candidateTableId)) {
            return false;
        }
        return substitutionCandidate(queryTableHandle)
                .map(candidateTableId::equals)
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((DeltaLakeTableHandle) handle);
    }

    /**
     * Substitution runs before predicate/projection pushdown (see PlanOptimizers), so a table scan
     * eligible for substitution carries a plain, whole-table handle. Any pushdown or mutation state
     * means the handle no longer represents the full source table and must not match. Time travel
     * pins a user-chosen table version, which the materialization does not necessarily hold.
     * projectedColumns is ignored: it does not affect table identity, and it is populated only in
     * the fully optimized plan.
     */
    private static Optional<ConnectorTableId> substitutionCandidate(DeltaLakeTableHandle tableHandle)
    {
        if (!tableHandle.getEnforcedPartitionConstraint().isAll()
                || !tableHandle.getNonPartitionConstraint().isAll()
                || !tableHandle.getConstraintColumns().isEmpty()
                || tableHandle.isMerge()
                || tableHandle.getAnalyzeHandle().isPresent()
                || tableHandle.isOptimize()
                || tableHandle.isRecordScannedFiles()
                || tableHandle.getMaxScannedFileSize().isPresent()
                || tableHandle.isTimeTravel()) {
            return Optional.empty();
        }
        // The transaction log is not required to carry a table id; without it there is no stable identity.
        return Optional.ofNullable(tableHandle.getMetadataEntry().getId())
                .map(deltaTableId -> new DeltaLakeTableId(tableHandle.getSchemaName(), tableHandle.getTableName(), deltaTableId));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        DeltaLakeColumnHandle columnHandle = (DeltaLakeColumnHandle) column;
        // Capture the struct-field path so a projected sub-field is distinct from its base column
        // and from sibling sub-fields of the same type (empty path for a whole base column).
        List<String> dereferencePhysicalNames = columnHandle.projectionInfo()
                .map(DeltaLakeColumnProjectionInfo::dereferencePhysicalNames)
                .orElseGet(ImmutableList::of);
        return Optional.of(new DeltaLakeColumnId(
                columnHandle.basePhysicalColumnName(),
                dereferencePhysicalNames));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(DeltaLakeTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(DeltaLakeColumnId.VERSION);
    }
}
