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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public class SasMetadata
        implements ConnectorMetadata
{
    private final SasClient client;

    @Inject
    public SasMetadata(SasClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.getSchemaNames(session.getIdentity());
    }

    @Override
    public SasTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return parseTableName(tableName.getTableName())
                .flatMap(parsed -> client.getTable(tableName.getSchemaName(), parsed.simpleName(), session.getIdentity())
                        .map(table -> new SasTableHandle(
                                tableName.getSchemaName(),
                                parsed.simpleName(),
                                parsed.splits(),
                                table.source(),
                                table.pageCount())))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        SchemaTableName schemaTableName = ((SasTableHandle) table).toSchemaTableName();
        return findTableMetadata(session, schemaTableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(client.getSchemaNames(session.getIdentity())));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            try {
                for (String tableName : client.getTableNames(schemaName, session.getIdentity())) {
                    builder.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (SchemaNotFoundException e) {
                // schema was dropped concurrently; skip it
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SasTableHandle sasTableHandle = (SasTableHandle) tableHandle;
        SasTable table = client.getTable(sasTableHandle.schemaName(), sasTableHandle.tableName(), session.getIdentity())
                .orElseThrow(() -> new TableNotFoundException(sasTableHandle.toSchemaTableName()));

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.columnsMetadata()) {
            columnHandles.put(column.getName(), new SasColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return relationFilter.apply(ImmutableSet.copyOf(listTables(session, schemaName)))
                .stream()
                .flatMap(tableName -> findTableMetadata(session, tableName).stream())
                .map(tableMetadata -> RelationColumnsMetadata.forTable(tableMetadata.getTable(), tableMetadata.getColumns()))
                .iterator();
    }

    private Optional<ConnectorTableMetadata> findTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return parseTableName(tableName.getTableName())
                    .flatMap(parsed -> client.getTable(tableName.getSchemaName(), parsed.simpleName(), session.getIdentity()))
                    .map(table -> new ConnectorTableMetadata(tableName, table.columnsMetadata()));
        }
        catch (TableNotFoundException | SchemaNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((SasColumnHandle) columnHandle).columnMetadata();
    }

    private record ParsedTableName(String simpleName, int splits) {}

    /**
     * Returns empty for non-integer {@code $} suffixes (e.g. system table references like {@code table$data})
     * so that {@link #getTableHandle} returns {@code null} (table not found) rather than an error.
     * Trino's connector contract requires returning {@code null} for unsupported system table names.
     */
    private static Optional<ParsedTableName> parseTableName(String rawName)
    {
        int dollar = rawName.indexOf('$');
        if (dollar == -1) {
            return Optional.of(new ParsedTableName(rawName, -1));
        }
        try {
            int splits = Integer.parseInt(rawName.substring(dollar + 1));
            return Optional.of(new ParsedTableName(rawName.substring(0, dollar), splits));
        }
        catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
