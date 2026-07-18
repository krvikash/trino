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
package io.trino.plugin.lakehouse.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTableHandle;
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
 * Lakehouse substitution metadata. Materializations may be stored on Iceberg or, in SEP, Hive.
 * Source tables may be Iceberg or Hive, so table/column identity
 * is routed by handle type to the matching delegate. Delta Lake and Hudi handles are reported as
 * non-substitutable, since those connectors provide no substitution identity yet.
 */
public class LakehouseSubstitutionMetadata
        implements ConnectorSubstitutionMetadata
{
    private final ConnectorSubstitutionMetadata icebergDelegate;
    private final ConnectorSubstitutionMetadata hiveDelegate;

    public LakehouseSubstitutionMetadata(ConnectorSubstitutionMetadata icebergDelegate, ConnectorSubstitutionMetadata hiveDelegate)
    {
        this.icebergDelegate = requireNonNull(icebergDelegate, "icebergDelegate is null");
        this.hiveDelegate = requireNonNull(hiveDelegate, "hiveDelegate is null");
    }

    @Override
    public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
    {
        return forTable(queryTable)
                .map(delegate -> delegate.tableHandleMatchesId(session, queryTable, candidateTable))
                .orElse(false);
    }

    @Override
    public Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return forTable(handle).flatMap(delegate -> delegate.getStorageTableId(session, handle));
    }

    @Override
    public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
    {
        return forTable(handle).flatMap(delegate -> delegate.getTableId(session, handle));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
    {
        return forColumn(column).flatMap(delegate -> delegate.getColumnId(session, column));
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions()
    {
        return ImmutableSet.<ConnectorIdVersion>builder()
                .addAll(icebergDelegate.tableIdVersions())
                .addAll(hiveDelegate.tableIdVersions())
                .build();
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions()
    {
        return ImmutableSet.<ConnectorIdVersion>builder()
                .addAll(icebergDelegate.columnIdVersions())
                .addAll(hiveDelegate.columnIdVersions())
                .build();
    }

    private Optional<ConnectorSubstitutionMetadata> forTable(ConnectorTableHandle handle)
    {
        return switch (handle) {
            case IcebergTableHandle _ -> Optional.of(icebergDelegate);
            case HiveTableHandle _ -> Optional.of(hiveDelegate);
            // Delta Lake and Hudi provide no substitution identity.
            default -> Optional.empty();
        };
    }

    private Optional<ConnectorSubstitutionMetadata> forColumn(ColumnHandle column)
    {
        return switch (column) {
            case IcebergColumnHandle _ -> Optional.of(icebergDelegate);
            case HiveColumnHandle _ -> Optional.of(hiveDelegate);
            default -> Optional.empty();
        };
    }
}
