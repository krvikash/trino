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
import io.trino.plugin.warp.dispatcher.DispatcherAlternativeChooser;
import io.trino.plugin.warp.dispatcher.WorkerNodePartitioningProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerDispatcherConnector
        extends DispatcherConnectorBase
{
    private final DispatcherAlternativeChooser dispatcherAlternativeChooser;

    @Inject
    public WorkerDispatcherConnector(
            @ForWarp Connector proxiedConnector,
            WarpSessionProperties warpSessionProperties,
            DispatcherAlternativeChooser dispatcherAlternativeChooser,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor)
    {
        super(proxiedConnector, warpSessionProperties, lifeCycleManager, connectorTaskExecutor);
        this.dispatcherAlternativeChooser = requireNonNull(dispatcherAlternativeChooser);
    }

    @Override
    public DispatcherAlternativeChooser getAlternativeChooser()
    {
        return dispatcherAlternativeChooser;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return new WorkerNodePartitioningProvider(proxiedConnector.getNodePartitioningProvider());
    }
}
