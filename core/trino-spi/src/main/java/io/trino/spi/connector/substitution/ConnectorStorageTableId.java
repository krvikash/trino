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
package io.trino.spi.connector.substitution;

import static java.util.Objects.requireNonNull;

/**
 * Identifier for a materialized view's storage table. Captured when the materialization is indexed,
 * persisted in the materialization metastore, and compared by {@link Object#equals} at substitution
 * time. Substitution proceeds only while the storage table that resolves by name today reports the
 * same id that was indexed.
 * <p>
 * {@code uniqueId} is a connector-supplied identifier whose stability defines when substitution is
 * allowed; a connector may choose an identifier that changes on every refresh to pin substitution
 * to the indexed snapshot.
 */
public record ConnectorStorageTableId(String schemaName, String tableName, String uniqueId)
{
    public ConnectorStorageTableId
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(uniqueId, "uniqueId is null");
    }
}
