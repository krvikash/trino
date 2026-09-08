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
package io.trino.plugin.warp.di;

import com.google.inject.Module;
import io.trino.plugin.warp.dispatcher.WarpConnectorContext;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

public interface WarpBaseModule
        extends Module
{
    default WarpBaseModule withConfig(Map<String, String> config)
    {
        return this;
    }

    default WarpBaseModule withContext(WarpConnectorContext context)
    {
        return this;
    }

    /**
     * True when the current node runs splits. The engine already folds
     * {@code node-scheduler.include-coordinator} into the worker node set.
     */
    static boolean isWorker(ConnectorContext context)
    {
        NodeManager nodeManager = context.getNodeManager();
        Node currentNode = nodeManager.getCurrentNode();
        if (!currentNode.isCoordinator()) {
            return true;
        }
        // the worker node set is only listable on the coordinator
        return nodeManager.getWorkerNodes().contains(currentNode);
    }

    default boolean shouldInstall()
    {
        return true;
    }
}
