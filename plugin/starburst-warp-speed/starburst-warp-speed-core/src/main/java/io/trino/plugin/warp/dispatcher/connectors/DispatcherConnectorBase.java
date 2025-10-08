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
package io.trino.plugin.warp.dispatcher.connectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherIndexProvider;
import io.trino.plugin.warp.dispatcher.DispatcherPageSinkProvider;
import io.trino.plugin.warp.dispatcher.substitution.DispatcherSubstitutionMetadata;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class DispatcherConnectorBase
        implements Connector
{
    private static final Logger logger = Logger.get(DispatcherConnectorBase.class);

    protected final Connector proxiedConnector;
    private final GlobalConfig globalConfig;
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;
    private final LifeCycleManager lifeCycleManager;
    private final ConnectorTaskExecutor connectorTaskExecutor;
    private final NativeStorageStateHandler nativeStorageStateHandler;

    public DispatcherConnectorBase(
            Connector proxiedConnector,
            GlobalConfig globalConfig,
            WarpSessionProperties warpSessionProperties,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor,
            NativeStorageStateHandler nativeStorageStateHandler)
    {
        this.proxiedConnector = requireNonNull(proxiedConnector);
        this.globalConfig = requireNonNull(globalConfig);
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(warpSessionProperties.getSessionProperties()));
        this.lifeCycleManager = requireNonNull(lifeCycleManager);
        this.connectorTaskExecutor = requireNonNull(connectorTaskExecutor);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
    }

    @VisibleForTesting
    public Connector getProxiedConnector()
    {
        return proxiedConnector;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        // @TODO ignore isolation level, readOnly for now
        return proxiedConnector.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return proxiedConnector.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return proxiedConnector.getMaterializedViewProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return proxiedConnector.getSchemaProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        List<PropertyMetadata<?>> ret = new ArrayList<>(sessionProperties);

        ret.addAll(proxiedConnector.getSessionProperties());
        return ret;
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return proxiedConnector.getAnalyzeProperties();
    }

    @Override
    public void shutdown()
    {
        nativeStorageStateHandler.shutdown();
        proxiedConnector.shutdown();
        lifeCycleManager.stop();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return new DispatcherPageSinkProvider(proxiedConnector.getPageSinkProvider());
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return new DispatcherIndexProvider(proxiedConnector.getIndexProvider());
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return proxiedConnector.getRecordSetProvider();
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return proxiedConnector.getSystemTables();
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return proxiedConnector.getFunctionProvider();
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return proxiedConnector.getTableProcedures();
    }

    @Override
    public long getInitialMemoryRequirement()
    {
        return globalConfig.getPreAllocMemorySize() + proxiedConnector.getInitialMemoryRequirement();
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return proxiedConnector.getTableFunctions();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return proxiedConnector.getColumnProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return proxiedConnector.getAccessControl();
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return proxiedConnector.isSingleStatementWritesOnly();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return proxiedConnector.getCapabilities();
    }

    @Override
    public ConnectorSubstitutionMetadata getSubstitutionMetadata()
    {
        // Delegate to the proxied connector, unwrapping Warp Speed's DispatcherTableHandle. When the
        // proxied connector does not support substitution it throws UnsupportedOperationException,
        // which propagates so the engine treats Warp Speed as unsupported too.
        return new DispatcherSubstitutionMetadata(proxiedConnector.getSubstitutionMetadata());
    }

    /**
     * this method is used by SEP plugin, do not remove
     */
    public Object executeConnectorTask(String taskName, String taskJsonData, String httpMethod)
    {
        try {
            return connectorTaskExecutor.executeTask(taskName, taskJsonData, httpMethod);
        }
        catch (TrinoException pe) {
            logger.warn(pe, "failed to execute connector task %s", taskJsonData);
            throw pe;
        }
        catch (Exception e) {
            logger.warn(e, "failed to execute connector task %s", taskJsonData);
            throw new RuntimeException(String.format(Locale.US, "failed to execute connector task %s", taskJsonData));
        }
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return Set.copyOf(proxiedConnector.getProcedures());
    }
}
