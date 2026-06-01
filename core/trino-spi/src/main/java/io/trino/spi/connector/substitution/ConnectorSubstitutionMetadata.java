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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;
import java.util.Set;

public interface ConnectorSubstitutionMetadata
{
    /**
     * Returns {@code true} when {@code queryTable} refers to the same logical table as
     * {@code candidateTable}, i.e. the connector considers the live handle a valid match for the
     * persisted source-table identity captured when the materialization was indexed.
     * There is no need to support any pushdowns or special attributes of the {@code ConnectorTableHandle},
     * and it that case this method can simply return false.
     */
    boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable);

    /**
     * Extracts a session-independent identity for a materialized view's storage table.
     * Captured once when the materialization is indexed and compared at substitution time
     * via {@link ConnectorStorageTableId#equals}. The comparison detects storage-table swaps
     * (e.g. the MV was dropped and recreated, reusing the logical name) so substitution can
     * avoid serving wrong results from a stale cached mapping, while tolerating rename and
     * refresh of the same physical table.
     * <p>
     * Returns empty when the storage table cannot be tagged with a stable identity — for
     * example a materialized view created before the substitution feature was introduced
     * whose storage table was never stamped with the connector's identity marker. A scan
     * over such a storage table cannot drive substitution; the engine simply skips indexing
     * the materialization. This should never happen in a normal operation.
     */
    default Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.empty();
    }

    /**
     * Extracts a session-independent identity from the given table handle for persistence in the
     * materialization metastore. Returns empty if the handle cannot be stably identified or in general, cannot
     * be used for substitution. It is ok and expected that the method returns empty for handles that contain pushdowns.
     */
    Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle);

    /**
     * Extracts a session-independent identity from a live column handle. Used by the
     * materialization metastore to persist column references.
     * <p>
     * Returns empty when the column handle does not have a stable identity — for example
     * when it represents a computed expression or a synthetic derivation rather than a
     * column of the underlying table. A scan whose assignments include such a column
     * cannot be indexed and cannot participate in substitution.
     */
    Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column);

    /**
     * Set of versions of the {@link ConnectorTableId} serialization format produced by this
     * connector. Bumped whenever fields are added/removed/renamed in the connector's table
     * identity implementation so the materialization metastore can detect and skip entries
     * persisted under an incompatible format.
     * Usually this will be a singleton. For connectors that wrap other connectors, this should return the union of all connectors' versions.
     */
    Set<ConnectorIdVersion> tableIdVersions();

    /**
     * Set of versions of the {@link ConnectorColumnId} serialization format produced by this
     * connector. Bumped whenever fields are added/removed/renamed in the connector's column
     * identity implementation so the materialization metastore can detect and skip entries
     * persisted under an incompatible format.
     * Usually this will be a singleton. For connectors that wrap other connectors, this should return the union of all connectors' versions.
     */
    Set<ConnectorIdVersion> columnIdVersions();
}
