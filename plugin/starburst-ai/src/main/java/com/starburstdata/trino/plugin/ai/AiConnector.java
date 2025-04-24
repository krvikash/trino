/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.ai;

import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AiConnector
        implements Connector
{
    private final ConnectorMetadata metadata;
    private final FunctionProvider functionProvider;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final Set<SystemTable> systemTables;

    @Inject
    public AiConnector(
            ConnectorMetadata metadata,
            FunctionProvider functionProvider,
            Set<ConnectorTableFunction> tableFunctions,
            Set<SystemTable> systemTables)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.tableFunctions = requireNonNull(tableFunctions, "tableFunctions is null");
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return AiTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }
}
