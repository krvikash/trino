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
import io.trino.spi.NodeManager;

import java.util.Map;

import static io.trino.plugin.warp.config.GlobalConfig.CONFIG_IS_SINGLE;

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

    static boolean isCoordinator(NodeManager nodeManager)
    {
        return nodeManager.getCurrentNode().isCoordinator();
    }

    static boolean isWorker(NodeManager nodeManager, Map<String, String> config)
    {
        return isSingle(config) || !isCoordinator(nodeManager);
    }

    static boolean isSingle(Map<String, String> config)
    {
        return Boolean.parseBoolean(config.getOrDefault(CONFIG_IS_SINGLE, "false"));
    }

    default boolean shouldInstall()
    {
        return true;
    }
}
