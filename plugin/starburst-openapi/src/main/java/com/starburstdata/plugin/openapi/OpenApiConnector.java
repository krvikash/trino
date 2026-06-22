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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class OpenApiConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final OpenApiMetadata metadata;
    private final OpenApiDescription description;
    private final OpenApiSplitManager splitManager;
    private final OpenApiPageSourceProvider pageSourceProvider;
    private final OpenApiTableFunctionsTable tableFunctionsTable;

    @Inject
    public OpenApiConnector(
            LifeCycleManager lifeCycleManager,
            OpenApiMetadata metadata,
            OpenApiDescription description,
            OpenApiSplitManager splitManager,
            OpenApiPageSourceProvider pageSourceProvider,
            OpenApiTableFunctionsTable tableFunctionsTable)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.description = requireNonNull(description, "description is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.tableFunctionsTable = requireNonNull(tableFunctionsTable, "tableFunctionsTable is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return OpenApiTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
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
    public Set<SystemTable> getSystemTables()
    {
        return ImmutableSet.of(tableFunctionsTable);
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return description.getTableFunctions();
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
