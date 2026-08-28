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
package io.trino.plugin.hudi.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.substitution.HiveColumnId;
import io.trino.plugin.hudi.HudiTableHandle;
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

public class HudiSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        HudiTableHandle queryTableHandle = (HudiTableHandle) queryTable;
        if (!(candidateTable instanceof HudiTableId candidateTableId)) {
            return false;
        }
        return substitutionCandidate(queryTableHandle)
                .map(candidateTableId::equals)
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((HudiTableHandle) handle);
    }

    /**
     * Substitution runs before predicate pushdown (see PlanOptimizers), so a table scan eligible for
     * substitution carries a plain, whole-table handle. A pushed-down predicate means the handle no
     * longer represents the full source table and must not match.
     */
    private static Optional<ConnectorTableId> substitutionCandidate(HudiTableHandle tableHandle)
    {
        if (!tableHandle.getPartitionPredicates().isAll()
                || !tableHandle.getRegularPredicates().isAll()
                || !tableHandle.getConstraintColumns().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new HudiTableId(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getBasePath()));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        // Hudi represents columns with HiveColumnHandle, so it reports the Hive column identity: the
        // delegating connectors (Object Store, Lakehouse) route column handles by type and hand a Hudi
        // table's columns to the Hive delegate, which must produce the same id as this connector does.
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;
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
        return ImmutableSet.of(HudiTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(HiveColumnId.VERSION);
    }
}
