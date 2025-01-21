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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.airlift.configuration.ConfigurationAwareModule;
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
import java.util.function.Supplier;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.IcebergConnectorFactory.createBootstrap;
import static io.trino.plugin.iceberg.IcebergConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingIcebergConnectorFactory
        implements ConnectorFactory
{
    private final Path localFileSystemRootPath;
    private final Supplier<Optional<Module>> icebergCatalogModule;
    private final Optional<WorkScheduler> workScheduler;
    private final Supplier<Optional<Module>> additionalOverrideModule;

    public TestingIcebergConnectorFactory(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional::empty, Optional.empty(), Optional::empty);
    }

    @Deprecated
    public TestingIcebergConnectorFactory(
            Path localFileSystemRootPath,
            Supplier<Optional<Module>> icebergCatalogModule,
            Optional<WorkScheduler> workScheduler)
    {
        this(localFileSystemRootPath, icebergCatalogModule, workScheduler, Optional::empty);
    }

    public TestingIcebergConnectorFactory(
            Path localFileSystemRootPath,
            Supplier<Optional<Module>> icebergCatalogModule,
            Supplier<Optional<Module>> additionalOverrideModule)
    {
        this(localFileSystemRootPath, icebergCatalogModule, Optional.empty(), additionalOverrideModule);
    }

    public TestingIcebergConnectorFactory(
            Path localFileSystemRootPath,
            Supplier<Optional<Module>> icebergCatalogModule,
            Optional<WorkScheduler> workScheduler,
            Supplier<Optional<Module>> additionalOverrideModule)
    {
        this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        var rootPath = localFileSystemRootPath.toFile();
        var _ = rootPath.mkdirs();
        this.icebergCatalogModule = requireNonNull(icebergCatalogModule, "icebergCatalogModule is null");
        this.workScheduler = requireNonNull(workScheduler, "workScheduler is null");
        this.additionalOverrideModule = requireNonNull(additionalOverrideModule, "additionalOverrideModule is null");
    }

    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, createConfig(config), context, createAdditionalModule(catalogName), icebergCatalogModule.get());
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = IcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(catalogName, createConfig(config), ImmutableMap.of(), context, createAdditionalModule(catalogName), icebergCatalogModule.get(), true);
            Set<ConfigPropertyMetadata> usedProperties = app.configure();
            return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
        }
    }

    private Map<String, String> createConfig(Map<String, String> config)
    {
        if (!config.containsKey("iceberg.catalog.type")) {
            return ImmutableMap.<String, String>builder()
                    .putAll(config)
                    .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                    .buildOrThrow();
        }
        return config;
    }

    private Module createAdditionalModule(String catalogName)
    {
        Module localModule = binder -> {
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("local").toInstance(new LocalFileSystemFactory(localFileSystemRootPath));
            configBinder(binder).bindConfigDefaults(
                    FileHiveMetastoreConfig.class,
                    metastoreConfig -> metastoreConfig.setCatalogDirectory("local:///" + catalogName));
            if (workScheduler.isPresent()) {
                newOptionalBinder(binder, WorkScheduler.class).setBinding().toInstance(workScheduler.get());
            }
        };
        return additionalOverrideModule.get()
                .map(override -> (Module) ConfigurationAwareModule.combine(localModule, override))
                .orElse(localModule);
    }
}
