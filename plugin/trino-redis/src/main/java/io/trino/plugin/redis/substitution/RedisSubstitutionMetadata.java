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
package io.trino.plugin.redis.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.redis.RedisColumnHandle;
import io.trino.plugin.redis.RedisTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class RedisSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        RedisTableHandle handle = (RedisTableHandle) queryTable;
        if (!(candidateTable instanceof RedisTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schemaName().equals(candidateId.getSchemaName()) && candidate.tableName().equals(candidateId.getTableName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((RedisTableHandle) handle)
                .map(candidate -> new RedisTableId(candidate.schemaName(), candidate.tableName()));
    }

    private static Optional<RedisTableHandle> substitutionCandidate(RedisTableHandle handle)
    {
        if (!handle.constraint().isAll()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        RedisColumnHandle handle = (RedisColumnHandle) column;
        return Optional.of(new RedisColumnId(handle.getName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(RedisTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(RedisColumnId.VERSION);
    }
}
