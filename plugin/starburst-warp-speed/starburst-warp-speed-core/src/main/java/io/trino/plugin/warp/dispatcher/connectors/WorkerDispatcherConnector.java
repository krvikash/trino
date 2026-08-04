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
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceProviderFactory;
import io.trino.plugin.warp.dispatcher.DispatcherSplitManager;
import io.trino.plugin.warp.dispatcher.WorkerNodePartitioningProvider;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerDispatcherConnector
        extends DispatcherConnectorBase
{
    private final DispatcherPageSourceProviderFactory dispatcherPageSourceProviderFactory;
    private final DispatcherSplitManager dispatcherSplitManager;
    private final WorkerCapacityManager workerCapacityManager;

    @Inject
    public WorkerDispatcherConnector(
            @ForWarp Connector proxiedConnector,
            GlobalConfig globalConfig,
            WarpSessionProperties warpSessionProperties,
            DispatcherPageSourceProviderFactory dispatcherPageSourceProviderFactory,
            DispatcherSplitManager dispatcherSplitManager,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor,
            NativeStorageStateHandler nativeStorageStateHandler,
            WorkerCapacityManager workerCapacityManager)
    {
        super(proxiedConnector, globalConfig, warpSessionProperties, lifeCycleManager, connectorTaskExecutor, nativeStorageStateHandler);
        this.dispatcherPageSourceProviderFactory = requireNonNull(dispatcherPageSourceProviderFactory);
        this.dispatcherSplitManager = requireNonNull(dispatcherSplitManager);
        this.workerCapacityManager = workerCapacityManager;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return dispatcherSplitManager;
    }

    @Override
    public ConnectorPageSourceProviderFactory getPageSourceProviderFactory()
    {
        return dispatcherPageSourceProviderFactory;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return new WorkerNodePartitioningProvider(proxiedConnector.getNodePartitioningProvider());
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        workerCapacityManager.shutdown();
    }
}
