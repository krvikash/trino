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
package io.trino.plugin.warp.proxiedconnector.hive;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.hive.HiveConnectorFactory;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.ProxiedConnectorInitializer;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.warp.proxiedconnector.utils.ConfigurationUtils.getHiveFilteredConfig;

public class HiveProxiedConnectorInitializer
        implements ProxiedConnectorInitializer
{
    private static final Module DEFAULT_ADDITIONAL_MODULE = EMPTY_MODULE;
    private static final Optional<HiveMetastore> DEFAULT_METASTORE = Optional.empty();
    private static final boolean DEFAULT_METASTORE_IMPERSONATION_ENABLED = false;
    private static final Optional<TrinoFileSystemFactory> DEFAULT_FILESYSTEM_FACTORY = Optional.empty();
    private static final Optional<DirectoryLister> DEFAULT_DIRECTORY_LISTENER = Optional.empty();

    @Override
    public Supplier<List<Module>> getModules(ConnectorContext context)
    {
        return () -> List.of(
                new ConnectorObjectNameGeneratorModule(
                        "io.trino.plugin.hive",
                        "trino.plugin.hive"),
                binder -> binder.bind(DispatcherProxiedConnectorTransformer.class).to(HiveProxiedConnectorTransformer.class));
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context, Optional<Module> optionalProxyModule)
    {
        try {
            // copy from HiveConnectorFactory::create - hive create a new classloader when calling the create method. we want to use the same classloader as the dispatcher
            Map<String, String> hiveConfig = getHiveFilteredConfig(config);
            return HiveConnectorFactory.createConnector(
                    catalogName,
                    hiveConfig,
                    context,
                    () -> DEFAULT_ADDITIONAL_MODULE,
                    DEFAULT_METASTORE,
                    DEFAULT_METASTORE_IMPERSONATION_ENABLED,
                    DEFAULT_FILESYSTEM_FACTORY,
                    DEFAULT_DIRECTORY_LISTENER);
        }
        catch (Exception e) {
            throw new RuntimeException("cant create hive connector", e);
        }
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context, Optional<Module> optionalProxyModule)
    {
        Map<String, String> hiveConfig = getHiveFilteredConfig(config);

        ClassLoader classLoader = HiveConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = HiveConnectorFactory.createBootstrap(
                    catalogName,
                    hiveConfig,
                    ImmutableMap.of(),
                    context,
                    () -> DEFAULT_ADDITIONAL_MODULE,
                    DEFAULT_METASTORE,
                    DEFAULT_METASTORE_IMPERSONATION_ENABLED,
                    DEFAULT_FILESYSTEM_FACTORY,
                    DEFAULT_DIRECTORY_LISTENER,
                    true);

            Set<ConfigPropertyMetadata> usedProperties = app.configure();

            return ConfigUtils.getSecuritySensitivePropertyNames(hiveConfig, usedProperties);
        }
    }
}
