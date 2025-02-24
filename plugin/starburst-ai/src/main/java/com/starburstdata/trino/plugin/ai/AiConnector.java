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
