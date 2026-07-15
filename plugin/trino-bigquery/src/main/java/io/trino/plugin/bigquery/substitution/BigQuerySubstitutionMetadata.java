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
package io.trino.plugin.bigquery.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.bigquery.BigQueryColumnHandle;
import io.trino.plugin.bigquery.BigQueryTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class BigQuerySubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        BigQueryTableHandle queryTableHandle = (BigQueryTableHandle) queryTable;
        if (!(candidateTable instanceof BigQueryTableId candidateTableId)) {
            return false;
        }
        return isSubstitutionCandidate(queryTableHandle)
                && queryTableHandle.getRequiredNamedRelation().getRemoteTableName().equals(candidateTableId.remoteTableName());
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        BigQueryTableHandle tableHandle = (BigQueryTableHandle) handle;
        if (!isSubstitutionCandidate(tableHandle)) {
            return Optional.empty();
        }
        return Optional.of(new BigQueryTableId(tableHandle.getRequiredNamedRelation().getRemoteTableName()));
    }

    private static boolean isSubstitutionCandidate(BigQueryTableHandle tableHandle)
    {
        // projectedColumns is intentionally not checked: BigQueryMetadata.getTableHandle always
        // populates it with all columns, and projection does not change the table's identity —
        // the engine remaps columns via getColumnId. Constraint and limit, however, are genuine
        // pushdowns that change the rows a scan returns, so substituting the full MV storage for
        // such a handle would be incorrect.
        return !tableHandle.isSynthetic()
                && tableHandle.constraint().isAll()
                && tableHandle.limit().isEmpty();
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        BigQueryColumnHandle bigQueryColumnHandle = (BigQueryColumnHandle) column;
        return Optional.of(new BigQueryColumnId(
                bigQueryColumnHandle.name(),
                bigQueryColumnHandle.dereferenceNames(),
                bigQueryColumnHandle.trinoType(),
                bigQueryColumnHandle.bigqueryType()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(BigQueryTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(BigQueryColumnId.VERSION);
    }
}
