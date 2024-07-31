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

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.log.Logger;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.warp.di.CacheManagerModule;
import io.trino.plugin.warp.di.WarpBaseModule;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.di.dispatcher.DispatcherCacheManagerModule;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import org.weakref.jmx.guice.MBeanModule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;

public class InternalDispatcherCacheManagerFactory
{
    private static final Logger logger = Logger.get(InternalDispatcherCacheManagerFactory.class);

    private InternalDispatcherCacheManagerFactory()
    {
    }

    public static CacheManager createCacheManager(String cacheManagerName,
            Map<String, String> config,
            Optional<List<Module>> optionalModules,
            Module storageEngineModule,
            CacheManagerContext context)
    {
        List<Module> modules;
        boolean isCoordinator = context.isCoordinator() && !WarpBaseModule.isSingle(config);
        modules = new ArrayList<>(asList(
                new EventModule(),
                new MBeanServerModule(),
                new MBeanModule(),
                new DispatcherCacheManagerModule(cacheManagerName, config, storageEngineModule, isCoordinator),
                new CacheManagerModule(context, isCoordinator)));
        if (!isCoordinator) {
            optionalModules.ifPresent(modules::addAll);
        }

        Bootstrap app = new Bootstrap(modules);
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(Collections.emptyMap())
                .setOptionalConfigurationProperties(config)
                .initialize();

        initializeSystemServices(injector);

        return injector.getInstance(CacheManager.class);
    }

    private static void initializeSystemServices(Injector injector)
    {
        logger.debug("begin initialize system services");
        injector.getInstance(WarpInitializedServiceRegistry.class).init();
        logger.debug("finish initialize system services");
    }
}
