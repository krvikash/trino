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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OpenApiMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final OpenApiDescription description;

    @Inject
    public OpenApiMetadata(OpenApiDescription description)
    {
        this.description = requireNonNull(description, "description is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        OpenApiRequestTableHandle handle = (OpenApiRequestTableHandle) connectorTableHandle;
        return new ConnectorTableMetadata(
                new SchemaTableName("_generated", "_table"),
                description.getDecoder(handle.path())
                        .getColumnHandles()
                        .stream()
                        .map(OpenApiColumnHandle::toColumnMetadata)
                        .collect(toImmutableList()));
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
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof OpenApiTableFunctionHandle(OpenApiRequestTableHandle requestHandle)) {
            return Optional.of(new TableFunctionApplicationResult<>(
                    requestHandle,
                    ImmutableList.copyOf(description.getDecoder(requestHandle.path()).getColumnHandles())));
        }
        return Optional.empty();
    }
}
