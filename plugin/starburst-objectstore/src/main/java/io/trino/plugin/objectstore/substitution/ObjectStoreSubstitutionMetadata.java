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
package io.trino.plugin.objectstore.substitution;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * ObjectStore substitution metadata. ObjectStore stores and substitutes materializations on Iceberg
 * tables only, so this delegates to the Iceberg connector's {@link ConnectorSubstitutionMetadata}.
 * Handles from the other delegate connectors (Hive, Delta Lake, Hudi) are reported as
 * non-substitutable rather than routed, since those connectors provide no substitution identity.
 */
public class ObjectStoreSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    private final ConnectorSubstitutionMetadata icebergDelegate;

    public ObjectStoreSubstitutionMetadata(ConnectorSubstitutionMetadata icebergDelegate)
    {
        this.icebergDelegate = requireNonNull(icebergDelegate, "icebergDelegate is null");
    }

    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        return queryTable instanceof IcebergTableHandle
                && icebergDelegate.tableHandleMatchesId(session, queryTable, candidateTable);
    }

    @Override
    public Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        if (handle instanceof IcebergTableHandle) {
            return icebergDelegate.getStorageTableId(session, handle);
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        if (handle instanceof IcebergTableHandle) {
            return icebergDelegate.getTableId(session, handle);
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        if (column instanceof IcebergColumnHandle) {
            return icebergDelegate.getColumnId(session, column);
        }
        return Optional.empty();
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return icebergDelegate.tableIdVersions();
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return icebergDelegate.columnIdVersions();
    }
}
