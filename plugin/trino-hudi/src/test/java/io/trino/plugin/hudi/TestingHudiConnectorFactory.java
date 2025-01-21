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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.hudi.HudiConnectorFactory.createBootstrap;
import static io.trino.plugin.hudi.HudiConnectorFactory.createConnector;

public class TestingHudiConnectorFactory
        implements ConnectorFactory
{
    private final Path localFileSystemRootPath;

    public TestingHudiConnectorFactory(Path localFileSystemRootPath)
    {
        localFileSystemRootPath.toFile().mkdirs();
        this.localFileSystemRootPath = localFileSystemRootPath;
    }

    @Override
    public String getName()
    {
        return "hudi";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, createConfig(config), Optional.empty(), context, createAdditionalModule(catalogName));
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = HudiConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(catalogName, createConfig(config), ImmutableMap.of(), Optional.empty(), context, createAdditionalModule(catalogName), true);
            Set<ConfigPropertyMetadata> usedProperties = app.configure();
            return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
        }
    }

    private Module createAdditionalModule(String catalogName)
    {
        return binder -> {
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("local").toInstance(new LocalFileSystemFactory(localFileSystemRootPath));
            configBinder(binder).bindConfigDefaults(
                    FileHiveMetastoreConfig.class,
                    metastoreConfig -> metastoreConfig.setCatalogDirectory("local:///" + catalogName));
        };
    }

    private Map<String, String> createConfig(Map<String, String> config)
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.<String, String>builder()
                .putAll(config)
                .put("bootstrap.quiet", "true");
        if (!config.containsKey("hive.metastore")) {
            configBuilder.put("hive.metastore", "file");
        }
        return configBuilder.buildOrThrow();
    }
}
