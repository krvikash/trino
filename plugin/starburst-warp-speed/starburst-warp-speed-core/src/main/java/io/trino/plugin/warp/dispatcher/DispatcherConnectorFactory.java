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
import io.trino.spi.connector.Connector;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class DispatcherConnectorFactory
{
    public static final String DISPATCHER_CONNECTOR_NAME = "warp_speed";
    private final Module proxyModule;

    public DispatcherConnectorFactory(Module proxyModule)
    {
        this.proxyModule = proxyModule;
    }

    public Connector create(
            String catalogName,
            Map<String, String> config,
            WarpConnectorContext context,
            Class<? extends InitializationModule> optionalModules,
            Map<String, String> proxiedConnectorInitializerMap)
    {
        try {
            ClassLoader classLoader = this.getClass().getClassLoader();
            // use the class instance from InternalDispatcherConnectorFactory's classloader
            Class<?> supplierClass = classLoader.loadClass(Supplier.class.getName());
            Supplier<Optional<Module>> optionalProxyModule = () -> Optional.ofNullable(proxyModule);

            Supplier<Module> optionalModule;
            try {
                Class<?> initModuleClass = classLoader.loadClass(optionalModules.getName());
                optionalModule = InitializationModule.invokeCreateModule(
                        initModuleClass,
                        config,
                        context,
                        catalogName);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            return (Connector) classLoader.loadClass(InternalDispatcherConnectorFactory.class.getName())
                    .getMethod(
                            "createConnector",
                            String.class,
                            Map.class,
                            supplierClass,
                            Map.class,
                            supplierClass,
                            WarpConnectorContext.class)
                    .invoke(null,
                            catalogName,
                            config,
                            optionalModule,
                            createProxiedConnectorInitializers(proxiedConnectorInitializerMap, classLoader),
                            optionalProxyModule,
                            context);
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

    public Set<String> getSecuritySensitivePropertyNames(
            String catalogName,
            Map<String, String> config,
            WarpConnectorContext context,
            Class<? extends InitializationModule> optionalModules,
            Map<String, String> proxiedConnectorInitializerMap)
    {
        try {
            ClassLoader classLoader = this.getClass().getClassLoader();
            Class<?> supplierClass = classLoader.loadClass(Supplier.class.getName());
            Supplier<Optional<Module>> optionalProxyModule = () -> Optional.ofNullable(proxyModule);

            return (Set<String>) classLoader.loadClass(InternalDispatcherConnectorFactory.class.getName())
                    .getMethod(
                            "getSecuritySensitivePropertyNames",
                            String.class,
                            Map.class,
                            Map.class,
                            supplierClass,
                            WarpConnectorContext.class)
                    .invoke(null,
                            catalogName,
                            config,
                            createProxiedConnectorInitializers(proxiedConnectorInitializerMap, classLoader),
                            optionalProxyModule,
                            context);
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

    private static Map<String, ?> createProxiedConnectorInitializers(Map<String, String> proxiedConnectorInitializerMap, ClassLoader classLoader)
    {
        return proxiedConnectorInitializerMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    try {
                        return classLoader.loadClass(entry.getValue()).getDeclaredConstructor().newInstance();
                    }
                    catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }
}
