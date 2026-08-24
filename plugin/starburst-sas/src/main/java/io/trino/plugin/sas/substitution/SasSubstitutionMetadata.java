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
package io.trino.plugin.sas.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.sas.SasColumnHandle;
import io.trino.plugin.sas.SasTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class SasSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        SasTableHandle handle = (SasTableHandle) queryTable;
        if (!(candidateTable instanceof SasTableId candidateId)) {
            return false;
        }
        return handle.schemaName().equals(candidateId.getSchemaName()) && handle.tableName().equals(candidateId.getTableName());
    }

    /**
     * The SAS connector pushes nothing into the table handle - there is no filter, limit, aggregation
     * or projection pushdown - so every handle represents a plain full-table scan and always yields an
     * identity. The split-count hint and page count on the handle are read-path metadata and do not
     * affect table identity.
     */
    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        SasTableHandle sasHandle = (SasTableHandle) handle;
        return Optional.of(new SasTableId(sasHandle.schemaName(), sasHandle.tableName()));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        return Optional.of(new SasColumnId(((SasColumnHandle) column).columnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(SasTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(SasColumnId.VERSION);
    }
}
