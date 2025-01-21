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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.airlift.json.JsonModule;
import io.starburst.ai.client.AiClientModule;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class IcebergConnectorFactory
        implements ConnectorFactory
{
    private static final Module DEFAULT_ADDITIONAL_MODULE = EMPTY_MODULE;
    private static final Optional<Module> DEFAULT_ICEBERG_CATALOG_MODULE = Optional.empty();

    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, DEFAULT_ADDITIONAL_MODULE, DEFAULT_ICEBERG_CATALOG_MODULE);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = IcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(catalogName, config, ImmutableMap.of(), context, DEFAULT_ADDITIONAL_MODULE, DEFAULT_ICEBERG_CATALOG_MODULE, true);
            Set<ConfigPropertyMetadata> usedProperties = app.configure();
            return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
        }
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<Module> icebergCatalogModule)
    {
        return createConnector(catalogName, config, ImmutableMap.of(), _ -> {}, context, module, icebergCatalogModule);
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> requiredConfig,
            Map<String, String> optionalConfig, // Used from Starburst ObjectStore connector. Unused properties are verified later.
            Consumer<Set<String>> usedConfigPropertiesConsumer,
            ConnectorContext context,
            Module module,
            Optional<Module> icebergCatalogModule)
    {
        ClassLoader classLoader = IcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(catalogName, requiredConfig, optionalConfig, context, module, icebergCatalogModule, false);

            usedConfigPropertiesConsumer.accept(app.configure().stream()
                    .map(ConfigPropertyMetadata::name)
                    .collect(Collectors.toSet()));

            Injector injector = app
                    .loadSecretsPlugins() // starburst-functions-client requires access to secrets.
                    .initialize();

            verify(!injector.getBindings().containsKey(Key.get(HiveConfig.class)), "HiveConfig should not be bound");

            return injector.getInstance(IcebergConnector.class);
        }
    }

    @VisibleForTesting
    public static Bootstrap createBootstrap(
            String catalogName,
            Map<String, String> requiredConfig,
            Map<String, String> optionalConfig,
            ConnectorContext context,
            Module module,
            Optional<Module> icebergCatalogModule,
            boolean quietBootstrap)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new MBeanModule(),
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.iceberg", "trino.plugin.iceberg"),
                new JsonModule(),
                new IcebergModule(context),
                new IcebergSecurityModule(),
                new IcebergEncryptionModule(),
                icebergCatalogModule.orElse(new IcebergCatalogModule()),
                new MBeanServerModule(),
                new AiClientModule(context.getModelConnectionSpecsLoader()),
                new IcebergFileSystemModule(catalogName, context, quietBootstrap),
                new ConnectorContextModule(catalogName, context),
                binder -> {
                    binder.bind(ClassLoader.class).toInstance(IcebergConnectorFactory.class.getClassLoader());
                },
                module);

        if (quietBootstrap) {
            app.quiet().skipErrorReporting();
        }

        return app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(requiredConfig)
                .setOptionalConfigurationProperties(optionalConfig);
    }

    private static class IcebergFileSystemModule
            extends AbstractConfigurationAwareModule
    {
        private final String catalogName;
        private final ConnectorContext context;
        private final boolean quietBootstrap;

        public IcebergFileSystemModule(String catalogName, ConnectorContext context, boolean quietBootstrap)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.context = requireNonNull(context, "context is null");
            this.quietBootstrap = quietBootstrap;
        }

        @Override
        protected void setup(Binder binder)
        {
            boolean metadataCacheEnabled = buildConfigObject(IcebergConfig.class).isMetadataCacheEnabled();
            install(new FileSystemModule(catalogName, context, metadataCacheEnabled, quietBootstrap));
        }
    }
}
