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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSubstitutionMetadata;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.mongodb.substitution.MongoSubstitutionMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MongoConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final MongoTransactionManager transactionManager;
    private final MongoSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final Set<ConnectorTableFunction> connectorTableFunctions;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final ConnectorSubstitutionMetadata substitutionMetadata;

    @Inject
    public MongoConnector(
            LifeCycleManager lifeCycleManager,
            MongoTransactionManager transactionManager,
            MongoSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            Set<TableProcedureMetadata> tableProcedures,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            MongoSubstitutionMetadata substitutionMetadata)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.tableProcedures = ImmutableSet.copyOf(tableProcedures);
        this.connectorTableFunctions = ImmutableSet.copyOf(requireNonNull(connectorTableFunctions, "connectorTableFunctions is null"));
        this.sessionProperties = sessionPropertiesProviders.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
        this.substitutionMetadata = new ClassLoaderSafeConnectorSubstitutionMetadata(
                requireNonNull(substitutionMetadata, "substitutionMetadata is null"),
                getClass().getClassLoader());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return transactionManager.beginTransaction(isolationLevel);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return transactionManager.getMetadata(transaction, session.getIdentity());
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        transactionManager.commit(transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        transactionManager.rollback(transaction);
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return connectorTableFunctions;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public ConnectorSubstitutionMetadata getSubstitutionMetadata()
    {
        return substitutionMetadata;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
