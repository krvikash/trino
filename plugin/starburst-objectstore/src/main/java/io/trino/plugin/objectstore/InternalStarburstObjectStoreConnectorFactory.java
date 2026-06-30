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
package io.trino.plugin.objectstore;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.deltalake.DeltaLakeConnectorFactory;
import io.trino.plugin.hive.HiveConnectorFactory;
import io.trino.plugin.hive.metastore.MetastoreTypeConfig.MetastoreType;
import io.trino.plugin.hudi.HudiConnectorFactory;
import io.trino.plugin.iceberg.CatalogType;
import io.trino.plugin.iceberg.IcebergConnectorFactory;
import io.trino.plugin.objectstore.hive.schemadiscovery.HiveSchemaDiscoveryModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.starburst.schema.discovery.models.IdentifierConstraint.VALID_IN_HIVE_AND_TRINO;
import static io.starburst.schema.discovery.models.IdentifierConstraint.VALID_IN_TRINO;
import static io.trino.plugin.objectstore.ObjectStoreConnectorFactoryUtil.completeConnectorFuture;
import static io.trino.plugin.objectstore.ObjectStoreConnectorFactoryUtil.connectorModule;
import static io.trino.plugin.objectstore.ObjectStoreConnectorFactoryUtil.toCallable;
import static io.trino.plugin.objectstore.ObjectStoreConnectorFactoryUtil.usingTracing;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newFixedThreadPool;

public final class InternalStarburstObjectStoreConnectorFactory
{
    private InternalStarburstObjectStoreConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            Optional<HiveMetastore> hiveMetastore,
            Module hiveModule,
            Optional<Module> icebergCatalogModule,
            Optional<Module> deltaMetastoreModule,
            Module deltaModule,
            ConnectorContext context)
    {
        Map<String, String> objectStoreConfig = buildConfig(config);
        ClassLoader classLoader = InternalStarburstObjectStoreConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader);
                // Having more than the number of connectors doesn't add value
                ExecutorService executor = newFixedThreadPool(Math.min(4, Runtime.getRuntime().availableProcessors()), threadsNamed("objectstore-connector-factory-%s"))) {
            Set<String> usedProperties = ConcurrentHashMap.newKeySet();
            Consumer<Set<String>> usedPropertiesConsumer = usedProperties::addAll;

            Future<Connector> hiveConnectorFuture = executor.submit(toCallable(usingTracing(
                    context.getTracer(),
                    "build-hive-connector",
                    () -> buildHiveConnector(catalogName, objectStoreConfig, hiveMetastore, hiveModule, context, usedPropertiesConsumer))));
            Future<Connector> icebergConnectorFuture = executor.submit(toCallable(usingTracing(
                    context.getTracer(),
                    "build-iceberg-connector",
                    () -> buildIcebergConnector(catalogName, objectStoreConfig, icebergCatalogModule, context, usedPropertiesConsumer))));
            Future<Connector> deltaConnectorFuture = executor.submit(toCallable(usingTracing(
                    context.getTracer(),
                    "build-delta-connector",
                    () -> buildDeltaConnector(catalogName, objectStoreConfig, deltaMetastoreModule, deltaModule, context, usedPropertiesConsumer))));
            Future<Connector> hudiConnectorFuture = executor.submit(toCallable(usingTracing(
                    context.getTracer(),
                    "build-hudi-connector",
                    () -> buildHudiConnector(catalogName, objectStoreConfig, hiveMetastore, context, usedPropertiesConsumer))));

            Connector hiveConnector = completeConnectorFuture(hiveConnectorFuture);
            Connector icebergConnector = completeConnectorFuture(icebergConnectorFuture);
            Connector deltaConnector = completeConnectorFuture(deltaConnectorFuture);
            Connector hudiConnector = completeConnectorFuture(hudiConnectorFuture);

            return usingTracing(context.getTracer(), "build-objectstore-connector", () -> {
                Bootstrap app = new Bootstrap(
                        connectorModule(ForHive.class, hiveConnector),
                        connectorModule(ForIceberg.class, icebergConnector),
                        connectorModule(ForDelta.class, deltaConnector),
                        connectorModule(ForHudi.class, hudiConnector),
                        new ObjectStoreModule(),
                        new GalaxyLocationSecurityModule(),
                        new FileSystemModule(catalogName, context, false),
                        new ConnectorContextModule(catalogName, context));

                app
                        .doNotInitializeLogging()
                        .disableSystemProperties()
                        .setRequiredConfigurationProperties(ImmutableMap.of())
                        .setOptionalConfigurationProperties(objectStoreConfig);

                usedPropertiesConsumer.accept(app.configure().stream()
                        .map(ConfigPropertyMetadata::name)
                        .collect(Collectors.toSet()));

                verifyUsedProperties(objectStoreConfig.keySet(), usedProperties);

                Injector injector = app.initialize();

                return injector.getInstance(ObjectStoreConnector.class);
            }).get();
        }
    }

    private static Map<String, String> buildConfig(Map<String, String> config)
    {
        Map<String, String> newConfig = new HashMap<>(config);

        checkArgument(!newConfig.containsKey("iceberg.catalog.type"), "Configuration property 'iceberg.catalog.type' is not supported. Use 'hive.metastore' instead");

        String metastoreTypeValue = newConfig.getOrDefault("hive.metastore", "thrift");
        MetastoreType metastoreType = Enums.getIfPresent(MetastoreType.class, metastoreTypeValue.toUpperCase(ENGLISH)).toJavaUtil()
                .orElseThrow(() -> new IllegalArgumentException("Invalid value '%s' for 'hive.metastore' configuration property. Supported values are: %s".formatted(metastoreTypeValue, Arrays.toString(MetastoreType.values()))));

        CatalogType catalogType = switch (metastoreType) {
            case THRIFT -> CatalogType.HIVE_METASTORE;
            case FILE -> CatalogType.TESTING_FILE_METASTORE;
            case GLUE -> CatalogType.GLUE;
            case UNITY -> CatalogType.UNITY;
        };
        newConfig.put("iceberg.catalog.type", catalogType.name());

        return ImmutableMap.copyOf(newConfig);
    }

    private static Connector buildHudiConnector(
            String catalogName,
            Map<String, String> config,
            Optional<HiveMetastore> hiveMetastore,
            ConnectorContext context,
            Consumer<Set<String>> usedPropertiesConsumer)
    {
        Map<String, String> hudiConfig = new HashMap<>(config);
        // Vended credentials are only applicable to the Delta sub-connector; remove to avoid UnityMetastoreModule rejecting the Hudi sub-connector
        hudiConfig.remove("hive.metastore.unity.vended-credentials-enabled");
        Connector hudiConnector = HudiConnectorFactory.createConnector(
                catalogName,
                ImmutableMap.of(),
                hudiConfig,
                usedPropertiesConsumer,
                hiveMetastore,
                context,
                Optional.of(combine(
                        new ConfigureCachingMetastoreModule(),
                        new GalaxyLocationSecurityModule())));
        return hudiConnector;
    }

    private static Connector buildDeltaConnector(
            String catalogName,
            Map<String, String> config,
            Optional<Module> deltaMetastoreModule,
            Module deltaModule,
            ConnectorContext context,
            Consumer<Set<String>> usedPropertiesConsumer)
    {
        // Don't enable delta.register-table-procedure.enabled since location access control is disabled in SEP by default
        Connector deltaConnector = DeltaLakeConnectorFactory.createConnector(
                catalogName,
                ImmutableMap.of(),
                config,
                usedPropertiesConsumer,
                context,
                deltaMetastoreModule,
                combine(
                        new ConfigureCachingMetastoreModule(),
                        new GalaxyLocationSecurityModule(),
                        deltaModule));
        return deltaConnector;
    }

    private static Connector buildIcebergConnector(
            String catalogName,
            Map<String, String> config,
            Optional<Module> icebergCatalogModule,
            ConnectorContext context,
            Consumer<Set<String>> usedPropertiesConsumer)
    {
        // Don't enable iceberg.register-table-procedure.enabled since location access control is disabled in SEP by default
        Connector icebergConnector = IcebergConnectorFactory.createConnector(
                catalogName,
                ImmutableMap.of(),
                config,
                usedPropertiesConsumer,
                context,
                combine(
                        new ConfigureCachingMetastoreModule(),
                        new GalaxyLocationSecurityModule()),
                icebergCatalogModule);
        return icebergConnector;
    }

    private static Connector buildHiveConnector(
            String catalogName,
            Map<String, String> config,
            Optional<HiveMetastore> hiveMetastore,
            Module hiveModule,
            ConnectorContext context,
            Consumer<Set<String>> usedPropertiesConsumer)
    {
        Map<String, String> hiveConfig = new HashMap<>(config);
        // Vended credentials are only applicable to the Delta sub-connector; remove to avoid UnityMetastoreModule rejecting the Hive sub-connector
        hiveConfig.remove("hive.metastore.unity.vended-credentials-enabled");
        boolean isHiveMetastoreUsed = hiveConfig.containsKey("hive.metastore") && hiveConfig.get("hive.metastore").equals("thrift");
        Connector hiveConnector = HiveConnectorFactory.createConnector(
                catalogName,
                ImmutableMap.of(),
                hiveConfig,
                usedPropertiesConsumer,
                context,
                () -> combine(
                        hiveModule,
                        // HMS is not compatible with trino in terms of identifiers, so schema discovery needs to translate incompatible names
                        new HiveSchemaDiscoveryModule(isHiveMetastoreUsed ? VALID_IN_HIVE_AND_TRINO : VALID_IN_TRINO),
                        new ConfigureCachingMetastoreModule(),
                        new GalaxyLocationSecurityModule()),
                hiveMetastore,
                false,
                Optional.empty());
        return hiveConnector;
    }

    private static void verifyUsedProperties(Set<String> config, Set<String> usedProperties)
    {
        for (String key : config) {
            if (!usedProperties.contains(key)) {
                throw new IllegalArgumentException("Configuration property '%s' was not used".formatted(key));
            }
        }
    }
}
