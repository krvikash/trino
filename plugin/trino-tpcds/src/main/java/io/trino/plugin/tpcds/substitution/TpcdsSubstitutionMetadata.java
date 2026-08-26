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
package io.trino.plugin.tpcds.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.tpcds.TpcdsColumnHandle;
import io.trino.plugin.tpcds.TpcdsTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class TpcdsSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        TpcdsTableHandle handle = (TpcdsTableHandle) queryTable;
        if (!(candidateTable instanceof TpcdsTableId candidateId)) {
            return false;
        }
        return handle.tableName().equals(candidateId.getTableName())
                && Double.compare(handle.scaleFactor(), candidateId.getScaleFactor()) == 0;
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        // The TPC-DS table handle carries no filter/limit/aggregation pushdown, so every handle is a
        // full scan and a valid substitution candidate. The table name and scale factor fully identify it.
        TpcdsTableHandle tpcdsHandle = (TpcdsTableHandle) handle;
        return Optional.of(new TpcdsTableId(tpcdsHandle.tableName(), tpcdsHandle.scaleFactor()));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        TpcdsColumnHandle handle = (TpcdsColumnHandle) column;
        return Optional.of(new TpcdsColumnId(handle.columnName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(TpcdsTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(TpcdsColumnId.VERSION);
    }
}
