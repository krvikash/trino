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
package io.trino.plugin.warp.it;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.varada.config.DictionaryConfig;
import io.trino.plugin.varada.config.GlobalConfig;
import io.trino.plugin.varada.dispatcher.CachingPlugin;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.varada.cloudvendors.config.CloudVendorConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.varada.config.GlobalConfig.CONFIG_IS_SINGLE;
import static io.trino.plugin.varada.config.GlobalConfig.ENABLE_DEFAULT_WARMING;
import static io.trino.plugin.varada.config.GlobalConfig.FAILURE_GENERATOR_ENABLED;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.CLUSTER_UUID;

public class DispatcherQueryRunner
{
    private static final Logger logger = Logger.get(DispatcherQueryRunner.class);

    private DispatcherQueryRunner() {}

    public static QueryRunner createQueryRunner(Module storageEngineModule,
            Optional<Module> additionalModule,
            int numOfNodes,
            Map<String, String> coordinatorProperties,
            Map<String, String> varadaConfig,
            Path hiveDir,
            String connectorName,
            String catalogName,
            Plugin proxiedPlugin,
            Map<String, String> extraConfigProperties)
            throws Exception
    {
        Path localStorePath = Files.createTempDirectory("local_store_");
        ImmutableMap<String, String> additionalCatalogConfig = ImmutableMap.<String, String>builder()
                .putAll(varadaConfig)
                // replace this config with the other 3 when you want to use a real thrift meta-store (E.g. local docker)
//          .put("hive.metastore.uri", "thrift://localhost:9083").build();
//                .put("testMode", "true")
                .put(CONFIG_IS_SINGLE, String.valueOf(numOfNodes < 2))
                .put("warp-speed.config.bundle-size-mb", "128")
                .put(ENABLE_DEFAULT_WARMING, "false")
                .put(CloudVendorConfig.STORE_PATH, "file:/" + localStorePath.toAbsolutePath())
                .put(CLUSTER_UUID, "some-uuid")
//                .put("warp-speed.metrics.enabled", (numOfNodes > 0) ? "false" : "true") // in case more than one node the jmx register fail on duplicate tables
                .put("hive.metastore", "file")
//                .put("hive.metastore.user", "presto")
                .put("hive.metastore.disable-location-checks", "true")
                .put("hive.metastore.catalog.dir", "file://" + hiveDir.toAbsolutePath())
//                .put("hive.metastore", "glue")
                .put(DictionaryConfig.EXCEPTIONAL_LIST_DICTIONARY, "REC_TYPE_ARRAY_INT,REC_TYPE_ARRAY_BIGINT")
                .put("warp-speed.config.dictionary.max-size", "3")
//                .put(HTTP_REST_PORT, "" + restPort)
                .put(WarpExtensionConfig.ENABLED, Boolean.TRUE.toString())
                .put(WarpExtensionConfig.HTTP_REST_PORT_ENABLED, Boolean.FALSE.toString())
                .put(GlobalConfig.LOCAL_STORE_PATH, localStorePath.toAbsolutePath().toString())
                .put(FAILURE_GENERATOR_ENABLED, "true")
                .put("warp-speed.objectstore.warmup.cloud.retries", "0")
                .put("warp-speed.objectstore.warmup.fetch.delay.duration", "1s")
                .buildOrThrow();
        QueryRunner queryRunner;
        try {
            queryRunner = createQueryRunner(storageEngineModule, additionalModule, additionalCatalogConfig, connectorName, catalogName, numOfNodes, coordinatorProperties, proxiedPlugin, extraConfigProperties);
        }
        catch (Exception io) {
            logger.error(io, "probably port already in use");
            queryRunner = createQueryRunner(storageEngineModule, additionalModule, additionalCatalogConfig, connectorName, catalogName, numOfNodes, coordinatorProperties, proxiedPlugin, extraConfigProperties);
            logger.info("GOOD FOR US - MANAGED TO RETRY AFTER 'Failed to bind' exception");
        }
        return queryRunner;
    }

    private static QueryRunner createQueryRunner(Module storageEngineModule,
            Optional<Module> additionalModule,
            Map<String, String> additionalCatalogConfig,
            String connectorName,
            String catalogName,
            int numOfNodes,
            Map<String, String> coordinatorProperties,
            Plugin proxiedPlugin,
            Map<String, String> extraConfigProperties)
            throws Exception
    {
        DistributedQueryRunner.Builder<?> queryRunnerBuilder = DistributedQueryRunner.builder(createSession(catalogName))
                .setWorkerCount(numOfNodes - 1)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(ImmutableMap.<String, String>builder().put("query.schedule-split-batch-size", "3")
                        .put("optimizer.use-sub-plan-alternatives", "true")
                        .put("node-scheduler.include-coordinator", String.valueOf(numOfNodes == 1))
                        .put("node-scheduler.policy", "topology")
                        .put("query.min-schedule-split-batch-size", "2")
                        .putAll(extraConfigProperties)
                        .buildKeepingLast());
        additionalModule.ifPresent(queryRunnerBuilder::setAdditionalModule);
        DistributedQueryRunner queryRunner = queryRunnerBuilder.build();

        try {
            CachingPlugin cachingPlugin = (CachingPlugin) proxiedPlugin;
            cachingPlugin.withStorageEngineModule(storageEngineModule);

            queryRunner.installPlugin(proxiedPlugin);

            queryRunner.createCatalog(catalogName, connectorName, getConfig(additionalCatalogConfig));
            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            logger.error(e, "failed creating query runner");
            throw e;
        }
    }

    private static Map<String, String> getConfig(Map<String, String> extConfig)
    {
        ImmutableMap.Builder<String, String> configMapBuilder = ImmutableMap.<String, String>builder()
                .putAll(extConfig)
//                .put("connector.name", CachingPlugin.CONNECTOR_NAME)
//                .put("metadata.resolveindex", "false")
//                .put("debug.limitnumobjs", "0")
                .put("warp-speed.config.task.max-worker-threads", "4")
                .put("warp-speed.config.warm-retry-backoff-factor-in-millis", "250");
        return configMapBuilder.buildOrThrow();
    }

    private static Session createSession(String catalogName)
    {
        return TestingSession.testSessionBuilder()
                .setCatalog(catalogName)
                .setSystemProperty(SystemSessionProperties.REDISTRIBUTE_WRITES, "true")
                .build();
    }
}
