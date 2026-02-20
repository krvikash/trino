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

import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.plugin.openapi.OpenApiSpec.SCHEMA_NAME;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static java.util.Collections.emptyList;

class OpenApiRequestTableFunction
        extends AbstractConnectorTableFunction
{
    private final OpenApiRequestTableHandle handle;

    public String getPath()
    {
        return handle.path();
    }

    public OpenApiRequestTableFunction(
            String path,
            String identifier,
            List<OpenApiColumnHandle> columns)
    {
        super(
                SCHEMA_NAME,
                identifier,
                emptyList(),
                new DescribedTable(descriptor(
                        columns.stream().map(OpenApiColumnHandle::name).collect(toImmutableList()),
                        columns.stream().map(OpenApiColumnHandle::type).collect(toImmutableList()))));
        this.handle = new OpenApiRequestTableHandle(path);
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        return TableFunctionAnalysis.builder()
                .handle(new OpenApiTableFunctionHandle(handle))
                .build();
    }
}
