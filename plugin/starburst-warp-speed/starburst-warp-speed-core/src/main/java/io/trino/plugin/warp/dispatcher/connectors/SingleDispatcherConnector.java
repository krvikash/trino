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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherNodePartitioningProvider;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.node.CoordinatorNodeManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

@Singleton
public class SingleDispatcherConnector
        extends DispatcherConnectorBase
{
    private final CoordinatorDispatcherConnector coordinatorDispatcherConnector;
    private final WorkerDispatcherConnector workerDispatcherConnector;
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final WorkerCapacityManager workerCapacityManager;

    @Inject
    public SingleDispatcherConnector(@ForWarp Connector proxiedConnector,
            GlobalConfig globalConfig,
            WarpSessionProperties warpSessionProperties,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor,
            NativeStorageStateHandler nativeStorageStateHandler,
            CoordinatorDispatcherConnector coordinatorDispatcherConnector,
            WorkerDispatcherConnector workerDispatcherConnector,
            CoordinatorNodeManager coordinatorNodeManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WorkerCapacityManager workerCapacityManager)
    {
        super(proxiedConnector, globalConfig, warpSessionProperties, lifeCycleManager, connectorTaskExecutor, nativeStorageStateHandler);
        this.coordinatorDispatcherConnector = coordinatorDispatcherConnector;
        this.workerDispatcherConnector = workerDispatcherConnector;
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.workerCapacityManager = workerCapacityManager;
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return coordinatorDispatcherConnector.getMetadata(session, transactionHandle);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return workerDispatcherConnector.getPageSourceProvider();
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        coordinatorDispatcherConnector.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        coordinatorDispatcherConnector.rollback(transactionHandle);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return coordinatorDispatcherConnector.getSplitManager();
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        return coordinatorDispatcherConnector.getCacheMetadata();
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return new DispatcherNodePartitioningProvider(proxiedConnector.getNodePartitioningProvider(), coordinatorNodeManager, dispatcherProxiedConnectorTransformer);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        workerCapacityManager.shutdown();
    }
}
