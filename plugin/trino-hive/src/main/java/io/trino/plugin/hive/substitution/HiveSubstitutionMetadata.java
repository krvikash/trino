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
package io.trino.plugin.hive.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveTableHandle;
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

public class HiveSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        HiveTableHandle queryTableHandle = (HiveTableHandle) queryTable;
        if (!(candidateTable instanceof HiveTableId candidateTableId)) {
            return false;
        }
        return isSubstitutionCandidate(queryTableHandle)
                && queryTableHandle.getSchemaName().equals(candidateTableId.schemaName())
                && queryTableHandle.getTableName().equals(candidateTableId.tableName());
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        HiveTableHandle tableHandle = (HiveTableHandle) handle;
        if (!isSubstitutionCandidate(tableHandle)) {
            return Optional.empty();
        }
        return Optional.of(new HiveTableId(tableHandle.getSchemaName(), tableHandle.getTableName()));
    }

    // Substitution runs before predicate/partition/projection pushdown (see PlanOptimizers), so a
    // table scan eligible for substitution carries a plain, whole-table handle. Any pushdown or
    // mutation state means the handle no longer represents the full source table and must not match.
    private static boolean isSubstitutionCandidate(HiveTableHandle tableHandle)
    {
        return tableHandle.getEnforcedConstraint().isAll()
                && tableHandle.getCompactEffectivePredicate().isAll()
                && tableHandle.getPartitions().isEmpty()
                && tableHandle.getPartitionNames().isEmpty()
                && tableHandle.getBucketFilter().isEmpty()
                && tableHandle.getTablePartitioning().isEmpty()
                && tableHandle.getAnalyzePartitionValues().isEmpty()
                && tableHandle.getMaxScannedFileSize().isEmpty()
                && !tableHandle.isRecordScannedFiles()
                && !tableHandle.getTransaction().isTransactional();
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;
        // Capture the struct-field path so a projected sub-field is distinct from its base column
        // and from sibling sub-fields of the same type (empty path for a whole base column).
        List<String> dereferenceNames = columnHandle.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceNames)
                .orElseGet(ImmutableList::of);
        return Optional.of(new HiveColumnId(
                columnHandle.getBaseColumnName(),
                dereferenceNames));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(HiveTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(HiveColumnId.VERSION);
    }
}
