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
package io.trino.plugin.jdbc.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class JdbcSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        JdbcTableHandle queryTableHandle = (JdbcTableHandle) queryTable;
        if (!(candidateTable instanceof JdbcTableId candidateTableId)) {
            return false;
        }
        return isSubstitutionCandidate(queryTableHandle)
                && queryTableHandle.getRelationHandle().equals(candidateTableId.relationHandle());
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) handle;
        if (!isSubstitutionCandidate(tableHandle)) {
            return Optional.empty();
        }
        return Optional.of(new JdbcTableId((JdbcNamedRelationHandle) tableHandle.getRelationHandle()));
    }

    private static boolean isSubstitutionCandidate(JdbcTableHandle tableHandle)
    {
        return !tableHandle.isSynthetic()
                && tableHandle.getUpdateAssignments().isEmpty()
                && tableHandle.getAuthorization().isEmpty();
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        JdbcColumnHandle jdbcColumnHandle = (JdbcColumnHandle) column;
        return Optional.of(new JdbcColumnId(
                jdbcColumnHandle.getColumnName(),
                JdbcTypeHandleDto.from(jdbcColumnHandle.getJdbcTypeHandle()),
                jdbcColumnHandle.getColumnType()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(JdbcTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(JdbcColumnId.VERSION);
    }
}
