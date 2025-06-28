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
package io.trino.plugin.warp.dispatcher;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorContext;

import static io.trino.plugin.warp.dispatcher.DispatcherCacheManagerFactory.DISPATCHER_CACHE_MANAGER_NAME;

public class WarpCacheMgrConnectorContext
        implements ConnectorContext, WarpContext
{
    private final Node currentNode;
    private final NodeManager nodeManager;
    private final WarpPluginSharedInstances sharedInstances;

    public WarpCacheMgrConnectorContext(Node currentNode, NodeManager nodeManager, WarpPluginSharedInstances sharedInstances)
    {
        this.currentNode = currentNode;
        this.nodeManager = nodeManager;
        this.sharedInstances = sharedInstances;
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return CatalogHandle.createRootCatalogHandle(
                new CatalogName(DISPATCHER_CACHE_MANAGER_NAME), new CatalogHandle.CatalogVersion("1"));
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return OpenTelemetry.noop();
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public WarpPluginSharedInstances getWarpPluginSharedInstances()
    {
        return sharedInstances;
    }

    @Override
    public Node getCurrentNode()
    {
        return currentNode;
    }
}
