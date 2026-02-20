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
package com.starburstdata.plugin.kdb;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RelationColumnsMetadata.forTable;
import static java.util.Objects.requireNonNull;

public class KdbMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(KdbMetadata.class);

    private final KdbClient client;

    @Inject
    public KdbMetadata(KdbClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.listSchemas();
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!KdbClient.isValidIdentifier(tableName.getSchemaName()) || !KdbClient.isValidIdentifier(tableName.getTableName())) {
            return null;
        }

        // Use a lightweight tables[] membership check rather than fetching full column
        // metadata here — getColumnHandles() will call getColumns() shortly after.
        if (!client.tableExists(tableName)) {
            return null;
        }
        return new KdbTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KdbTableHandle kdbTableHandle = (KdbTableHandle) tableHandle;
        SchemaTableName tableName = kdbTableHandle.schemaTableName();

        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        List<ColumnMetadata> columnMetadata = columns.entrySet().stream()
                .map(column -> new ColumnMetadata(column.getKey(), ((KdbColumnHandle) column.getValue()).columnType()))
                .collect(toImmutableList());

        return new ConnectorTableMetadata(tableName, columnMetadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return client.listTables(schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KdbTableHandle kdbTableHandle = (KdbTableHandle) tableHandle;
        SchemaTableName tableName = kdbTableHandle.schemaTableName();

        List<KdbColumnHandle> columns = client.loadColumns(tableName);
        if (columns.isEmpty()) {
            throw new TableNotFoundException(tableName);
        }
        return columns.stream()
                .collect(toImmutableMap(KdbColumnHandle::columnName, Function.identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        KdbColumnHandle kdbColumnHandle = (KdbColumnHandle) columnHandle;
        return new ColumnMetadata(kdbColumnHandle.columnName(), kdbColumnHandle.columnType());
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        for (SchemaTableName tableName : listTables(session, schemaName)) {
            try {
                ConnectorTableHandle tableHandle = getTableHandle(session, tableName, Optional.empty(), Optional.empty());
                if (tableHandle != null) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
                    relationColumns.put(tableName, forTable(tableName, tableMetadata.getColumns()));
                }
            }
            catch (Exception e) {
                log.warn(e, "Failed to list columns for table %s", tableName);
            }
        }

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }
}
