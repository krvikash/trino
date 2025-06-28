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
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationUtils;
import io.airlift.log.Logger;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.di.WarpSharedInstancesModule;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.DemoterSync;
import io.trino.plugin.warp.metrics.ScheduledMetricsHandler;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.read.RangeFillerService;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.InternalDispatcherConnectorFactory.WARP_PREFIX;

public class WarpPluginSharedInstancesFactory
{
    private static final Logger logger = Logger.get(WarpPluginSharedInstancesFactory.class);

    private final Module storageEngineModule;
    private WarpPluginSharedInstances sharedInstances;

    public WarpPluginSharedInstancesFactory(Module storageEngineModule)
    {
        this.storageEngineModule = storageEngineModule;
    }

    public synchronized WarpPluginSharedInstances create(boolean isCoordinator, Map<String, String> config)
    {
        Map<String, String> warpConfig = getWarpConfig(config);

        if (sharedInstances == null) {
            Bootstrap app = new Bootstrap(
                    new MBeanServerModule(),
                    new MBeanModule(),
                    new WarpSharedInstancesModule(storageEngineModule, isCoordinator, config));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .setOptionalConfigurationProperties(warpConfig)
                    .initialize();

            sharedInstances = new WarpPluginSharedInstances(
                    injector.getInstance(SharedConfig.class),
                    injector.getInstance(NativeConfig.class),
                    injector.getInstance(ScheduledMetricsHandler.class),
                    injector.getInstance(ExceptionThrower.class),
                    injector.getInstance(RangeFillerService.class),
                    injector.getInstance(StorageEngine.class),
                    injector.getInstance(StorageEngineConstants.class),
                    injector.getInstance(MatchCollectIdService.class),
                    injector.getInstance(DemoterSync.class));
        }
        else {
            ConfigurationFactory configFactory = new ConfigurationFactory(warpConfig);
            SharedConfig sharedConfig = configFactory.build(SharedConfig.class);
            NativeConfig nativeConfig = configFactory.build(NativeConfig.class);

            if (!sharedInstances.sharedConfig().equals(sharedConfig)) {
                logger.warn("at least one shared configuration property is not equal. ignoring");
            }
            if (!sharedInstances.nativeConfig().equals(nativeConfig)) {
                logger.warn("at least one native configuration property is not equal. ignoring");
            }
        }
        return sharedInstances;
    }

    private Map<String, String> getWarpConfig(Map<String, String> config)
    {
        config = ConfigurationUtils.replaceEnvironmentVariables(config);

        return config.entrySet().stream()
                .filter(e ->
                        e.getKey().startsWith("warp-speed") ||
                                e.getKey().startsWith(WARP_PREFIX) ||
                                e.getKey().equals("node.environment"))
                .collect(Collectors.toMap(entry -> entry.getKey().startsWith(WARP_PREFIX) ?
                        entry.getKey().substring(WARP_PREFIX.length()) : entry.getKey(), Map.Entry::getValue));
    }
}
