/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
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
