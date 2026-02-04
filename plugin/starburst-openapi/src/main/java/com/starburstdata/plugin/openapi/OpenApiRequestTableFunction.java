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
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;

import static com.starburstdata.plugin.openapi.OpenApiSpec.SCHEMA_NAME;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.emptyList;

class OpenApiRequestTableFunction
        extends AbstractConnectorTableFunction
{
    private final String path;

    public String getPath()
    {
        return path;
    }

    public OpenApiRequestTableFunction(
            String path,
            String identifier)
    {
        super(
                SCHEMA_NAME,
                identifier,
                emptyList(),
                new DescribedTable(descriptor(
                        ImmutableList.of("value"),
                        ImmutableList.of(VARCHAR))));
        this.path = path;
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        OpenApiRequestTableHandle requestHandle = new OpenApiRequestTableHandle(path);
        return TableFunctionAnalysis.builder()
                .handle(new OpenApiTableFunctionHandle(requestHandle))
                .build();
    }
}
