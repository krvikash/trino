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
package io.trino.plugin.warp;

import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.warp.dispatcher.DispatcherCacheManagerFactory;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheManagerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.config.GlobalConfig.CONFIG_IS_CACHE;

public class WarpCacheManagerFactory
        implements CacheManagerFactory
{
    private final DispatcherCacheManagerFactory dispatcherCacheManagerFactory;

    public WarpCacheManagerFactory(DispatcherCacheManagerFactory dispatcherCacheManagerFactory)
    {
        this.dispatcherCacheManagerFactory = dispatcherCacheManagerFactory;
    }

    @Override
    public String getName()
    {
        return DispatcherCacheManagerFactory.DISPATCHER_CACHE_MANAGER_NAME;
    }

    @Override
    public CacheManager create(Map<String, String> config, CacheManagerContext context)
    {
        Map<String, String> configMap = new HashMap<>(config);

        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        WarpExtensionConfig warpExtensionConfig = configFactory.build(WarpExtensionConfig.class);

        if (!warpExtensionConfig.isUseHttpServerPort()) {
            String httpRestPortStr = WarpClient.getRestHttpPortStr(warpExtensionConfig, -1);

            configMap.put("http-server.http.port", httpRestPortStr);
            if (!configMap.containsKey(WarpExtensionConfig.HTTP_REST_PORT)) {
                configMap.put(WarpExtensionConfig.HTTP_REST_PORT, httpRestPortStr);
            }
        }

        configMap.put(CONFIG_IS_CACHE, "true");

        return dispatcherCacheManagerFactory.create(configMap, context, Optional.of(List.of(WarpModule.class)));
    }
}
