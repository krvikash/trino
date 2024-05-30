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
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

import static io.trino.plugin.warp.config.GlobalConfig.CONFIG_IS_CACHE;
import static io.trino.plugin.warp.config.GlobalConfig.CONFIG_IS_SINGLE;

public interface WarpBaseModule
        extends Module
{
    default WarpBaseModule withConfig(Map<String, String> config)
    {
        return this;
    }

    default WarpBaseModule withContext(ConnectorContext context)
    {
        return this;
    }

    static boolean isCoordinator(ConnectorContext context)
    {
        return context.getNodeManager().getCurrentNode().isCoordinator();
    }

    static boolean isWorker(ConnectorContext context, Map<String, String> config)
    {
        return isSingle(config) || !isCoordinator(context);
    }

    static boolean isSingle(Map<String, String> config)
    {
        return Boolean.parseBoolean(config.getOrDefault(CONFIG_IS_SINGLE, Boolean.FALSE.toString()));
    }

    static boolean isCache(Map<String, String> config)
    {
        return Boolean.parseBoolean(config.getOrDefault(CONFIG_IS_CACHE, Boolean.FALSE.toString()));
    }

    default boolean shouldInstall()
    {
        return true;
    }
}
