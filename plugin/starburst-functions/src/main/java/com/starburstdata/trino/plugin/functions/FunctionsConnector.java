/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions;

import com.google.inject.Inject;
import com.starburstdata.trino.plugin.functions.ai.AiTransactionHandle;
import com.starburstdata.trino.plugin.functions.io.StoragePageSourceProvider;
import com.starburstdata.trino.plugin.functions.io.StorageSplitManager;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FunctionsConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final ConnectorMetadata metadata;
    private final StorageSplitManager splitManager;
    private final StoragePageSourceProvider pageSourceProvider;
    private final FunctionProvider functionProvider;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final Set<SystemTable> systemTables;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public FunctionsConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorMetadata metadata,
            StorageSplitManager splitManager,
            StoragePageSourceProvider pageSourceProvider,
            FunctionProvider functionProvider,
            Set<ConnectorTableFunction> tableFunctions,
            Set<SystemTable> systemTables,
            Set<SessionPropertiesProvider> sessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.tableFunctions = requireNonNull(tableFunctions, "tableFunctions is null");
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.sessionProperties = sessionProperties.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
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
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
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

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
