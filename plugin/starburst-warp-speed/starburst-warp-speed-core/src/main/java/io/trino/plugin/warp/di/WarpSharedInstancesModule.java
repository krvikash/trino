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
package io.trino.plugin.warp.di;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.DemoterSync;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsRegistry;
import io.trino.plugin.warp.metrics.ScheduledMetricsHandler;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeLogger;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;
import io.trino.spi.catalog.CatalogName;

import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class WarpSharedInstancesModule
        implements WarpBaseModule
{
    // fake catalog name in order to use ShapingLogger
    private static final String WARP_SHARED = "warp-shared";

    private final Optional<Module> storageEngineModule;
    private final boolean isCoordinator;
    private final Map<String, String> config;

    public WarpSharedInstancesModule(Module storageEngineModule, boolean isCoordinator, Map<String, String> config)
    {
        this.storageEngineModule = Optional.ofNullable(storageEngineModule);
        this.isCoordinator = isCoordinator;
        this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(SharedConfig.class);
        configBinder(binder).bindConfig(NativeConfig.class);
        configBinder(binder).bindConfig(MetricsConfig.class);

        binder.bind(CatalogName.class).toInstance(new CatalogName(WARP_SHARED));
        binder.bind(CatalogNameProvider.class).toInstance(new CatalogNameProvider(WARP_SHARED));
        binder.bind(ShapingLoggerFactory.class);

        binder.bind(FailureGeneratorInvocationHandler.class);

        binder.bind(MetricsManager.class);
        binder.bind(MetricsRegistry.class);
        binder.bind(ScheduledMetricsHandler.class).asEagerSingleton();

        binder.bind(NativeLogger.class);
        binder.install(storageEngineModule.orElseGet(() -> new WarpNativeStorageEngineModule(isCoordinator, config)));

        binder.bind(MatchCollectIdService.class);

        binder.bind(DemoterSync.class);
    }
}
