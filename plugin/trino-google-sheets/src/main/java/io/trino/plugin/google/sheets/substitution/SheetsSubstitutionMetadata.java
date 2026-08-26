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
package io.trino.plugin.google.sheets.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.google.sheets.SheetsColumnHandle;
import io.trino.plugin.google.sheets.SheetsNamedTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class SheetsSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        if (!(queryTable instanceof SheetsNamedTableHandle handle) || !(candidateTable instanceof SheetsTableId candidateId)) {
            return false;
        }
        return handle.schemaName().equals(candidateId.getSchemaName()) && handle.tableName().equals(candidateId.getTableName());
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        // Only named tables (from the metadata sheet) have a stable identity. A sheet() table-function
        // scan (SheetsSheetTableHandle) is not a named table and cannot drive substitution.
        if (!(handle instanceof SheetsNamedTableHandle namedHandle)) {
            return Optional.empty();
        }
        return Optional.of(new SheetsTableId(namedHandle.schemaName(), namedHandle.tableName()));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        SheetsColumnHandle handle = (SheetsColumnHandle) column;
        return Optional.of(new SheetsColumnId(handle.columnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(SheetsTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(SheetsColumnId.VERSION);
    }
}
