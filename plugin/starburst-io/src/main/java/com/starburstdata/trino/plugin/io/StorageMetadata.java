/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.io;

import com.starburstdata.trino.plugin.io.functions.Load;
import com.starburstdata.trino.plugin.io.functions.Load.LoadTableHandle;
import io.trino.plugin.hive.HiveColumnHandle;
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

public class StorageMetadata
        implements ConnectorMetadata
{
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof LoadTableHandle loadTableHandle) {
            return new ConnectorTableMetadata(
                    // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                    new SchemaTableName("_generated", "_generated_load"),
                    loadTableHandle.columns().stream()
                            .map(column -> new ColumnMetadata(column.getName(), column.getType()))
                            .collect(toImmutableList()));
        }
        throw new IllegalArgumentException("Unsupported table handle: " + tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HiveColumnHandle column = (HiveColumnHandle) columnHandle;
        return column.getColumnMetadata();
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof Load.LoadFunctionHandle loadFunctionHandle) {
            LoadTableHandle tableHandle = loadFunctionHandle.tableHandle();
            List<ColumnHandle> columnHandles = tableHandle.columns().stream()
                    .map(ColumnHandle.class::cast)
                    .collect(toImmutableList());
            return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
        }
        return Optional.empty();
    }
}
