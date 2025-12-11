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

import com.google.inject.Module;
import io.trino.plugin.warp.di.InitializationModule;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class DispatcherCacheManagerFactory
{
    public static final String DISPATCHER_CACHE_MANAGER_NAME = "warp_cache";

    public DispatcherCacheManagerFactory()
    {
    }

    public CacheManager create(Map<String, String> config,
            CacheManagerContext context,
            WarpCacheMgrConnectorContext warpCacheMgrConnectorContext,
            Class<? extends InitializationModule> optionalModules)
    {
        try {
            ClassLoader classLoader = this.getClass().getClassLoader();

            Supplier<Module> optionalModule;
            try {
                Class<?> initModuleClass = classLoader.loadClass(optionalModules.getName());
                optionalModule = InitializationModule.invokeCreateModule(initModuleClass,
                        config,
                        warpCacheMgrConnectorContext,
                        DISPATCHER_CACHE_MANAGER_NAME);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            return (CacheManager) classLoader.loadClass(InternalDispatcherCacheManagerFactory.class.getName())
                    .getMethod("createCacheManager",
                            String.class,
                            Map.class,
                            Supplier.class,
                            CacheManagerContext.class,
                            WarpCacheMgrConnectorContext.class)
                    .invoke(
                            null,
                            DISPATCHER_CACHE_MANAGER_NAME,
                            config,
                            optionalModule,
                            context,
                            warpCacheMgrConnectorContext);
        }
        catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            throwIfUnchecked(targetException);
            throw new RuntimeException(targetException);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
