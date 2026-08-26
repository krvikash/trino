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
package io.trino.plugin.kafka.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.KafkaTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

public class KafkaSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        KafkaTableHandle handle = (KafkaTableHandle) queryTable;
        if (!(candidateTable instanceof KafkaTableId candidateId)) {
            return false;
        }
        return substitutionCandidate(handle)
                .map(candidate -> candidate.schemaName().equals(candidateId.getSchemaName())
                        && candidate.tableName().equals(candidateId.getTableName())
                        && candidate.topicName().equals(candidateId.getTopicName()))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return substitutionCandidate((KafkaTableHandle) handle)
                .map(candidate -> new KafkaTableId(candidate.schemaName(), candidate.tableName(), candidate.topicName()));
    }

    private static Optional<KafkaTableHandle> substitutionCandidate(KafkaTableHandle handle)
    {
        // A pushed-down predicate (e.g. on the internal offset/partition/timestamp columns) makes the
        // handle a non-plain scan, so it cannot drive substitution.
        if (!handle.constraint().isAll()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        KafkaColumnHandle handle = (KafkaColumnHandle) column;
        return Optional.of(new KafkaColumnId(handle.getName()));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.of(KafkaTableId.VERSION);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.of(KafkaColumnId.VERSION);
    }
}
