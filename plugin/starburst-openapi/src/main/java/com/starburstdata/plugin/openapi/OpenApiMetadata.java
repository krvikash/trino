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
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OpenApiMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final OpenApiSpec spec;
    private final int domainExpansionLimit;

    @Inject
    public OpenApiMetadata(OpenApiSpec spec, OpenApiConfig config)
    {
        this.spec = requireNonNull(spec, "spec is null");
        this.domainExpansionLimit = config.getDomainExpansionLimit();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession connectorSession,
            SchemaTableName schemaTableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        OpenApiTableHandle handle = spec.getTableHandle(schemaTableName);
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        return handle;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        return switch (connectorTableHandle) {
            case OpenApiTableHandle tableHandle -> spec.getTableMetadata(tableHandle.schemaTableName());
            case OpenApiRequestTableHandle _ -> new ConnectorTableMetadata(
                    new SchemaTableName("_generated", "_table"),
                    ImmutableList.of(new ColumnMetadata("value", VARCHAR)));
            default -> throw new IllegalArgumentException(
                    "Unexpected handle class %s".formatted(connectorTableHandle.getClass().getCanonicalName()));
        };
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return spec.getTables()
                .keySet()
                .stream()
                .map(table -> new SchemaTableName(SCHEMA_NAME, table))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) connectorTableHandle;
        return spec.getTables().get(tableHandle.schemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, OpenApiColumn::getHandle));
    }

    public Map<String, OpenApiColumn> getColumns(ConnectorTableHandle connectorTableHandle)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) connectorTableHandle;
        return spec.getTables().get(tableHandle.schemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        OpenApiColumnHandle handle = (OpenApiColumnHandle) columnHandle;
        return new ColumnMetadata(handle.name(), handle.type());
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return spec.getTables().entrySet().stream()
                .map(entry -> TableColumnsMetadata.forTable(
                        new SchemaTableName(prefix.getSchema().orElse(""), entry.getKey()),
                        entry.getValue().stream().map(OpenApiColumn::getMetadata).toList()))
                .iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        OpenApiTableHandle openApiTable = (OpenApiTableHandle) table;
        return openApiTable.applyFilter(constraint, getColumns(table), domainExpansionLimit);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof OpenApiTableFunctionHandle(OpenApiRequestTableHandle requestHandle)) {
            return Optional.of(new TableFunctionApplicationResult<>(
                    requestHandle,
                    ImmutableList.of(new OpenApiColumnHandle("value", VARCHAR))));
        }
        return Optional.empty();
    }
}
