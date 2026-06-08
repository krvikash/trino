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
package io.trino.plugin.warp.dispatcher.substitution;

import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
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
 * Adapts the proxied connector's {@link ConnectorSubstitutionMetadata} to Warp Speed table handles.
 * Warp Speed wraps the proxied connector's table handle in a {@link DispatcherTableHandle}, so the
 * handle is unwrapped before delegating. Column handles are not wrapped by Warp Speed, hence they
 * (and the version methods) are forwarded unchanged.
 */
public class DispatcherSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    private final ConnectorSubstitutionMetadata delegate;

    public DispatcherSubstitutionMetadata(ConnectorSubstitutionMetadata delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        return delegate.tableHandleMatchesId(session, proxied(queryTable), candidateTable);
    }

    @Override
    public Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return delegate.getStorageTableId(session, proxied(handle));
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return delegate.getTableId(session, proxied(handle));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        return delegate.getColumnId(session, column);
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return delegate.tableIdVersions();
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return delegate.columnIdVersions();
    }

    private static ConnectorTableHandle proxied(ConnectorTableHandle handle)
    {
        return ((DispatcherTableHandle) handle).getProxyConnectorTableHandle();
    }
}
