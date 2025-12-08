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

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.WarpConnectorContext;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupRuleProvider;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.metrics.ScheduledMetricsHandler;
import io.trino.plugin.warp.node.CoordinatorInitializedEventHandler;
import io.trino.plugin.warp.node.CoordinatorNodeManager;
import io.trino.plugin.warp.node.WorkerNodeManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.read.RangeFillerService;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.util.json.SliceSerializer;
import io.trino.plugin.warp.util.json.WarpColumnJsonKeyDeserializer;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.type.TypeDeserializer;

import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.warp.di.WarpBaseModule.isSingle;
import static java.util.Objects.requireNonNull;

public class WarpMainModule
        implements WarpBaseModule
{
    private final WarpConnectorContext context;
    private final String catalogName;
    private final Map<String, String> config;

    WarpMainModule(WarpConnectorContext context, String catalogName, Map<String, String> config)
    {
        this.context = requireNonNull(context);
        this.catalogName = requireNonNull(catalogName);
        this.config = requireNonNull(config);
    }

    @Override
    public void configure(Binder binder)
    {
        boolean isCoordinator = context.getCurrentNode().isCoordinator();
        if (isCoordinator) {
            configureCoordinator(binder);
        }
        boolean isWorker = isSingle(config) || !isCoordinator;
        if (isWorker) {
            configureWorker(binder);
        }
        configureCommon(binder);

        binder.bind(WarpInitializedServiceRegistry.class);

        bindMetricsServices(binder);
        bindConfigs(binder);

        binder.bind(ShapingLoggerFactory.class);
    }

    private void bindConfigs(Binder binder)
    {
        configBinder(binder).bindConfig(MetricsConfig.class);
        configBinder(binder).bindConfig(GlobalConfig.class);
        configBinder(binder).bindConfig(WarmupDemoterConfig.class);
        configBinder(binder).bindConfig(ProxiedConnectorConfig.class);
        configBinder(binder).bindConfig(DictionaryConfig.class);
        configBinder(binder).bindConfig(CloudVendorConfig.class, ForWarp.class);

        binder.bind(SharedConfig.class).toInstance(context.getWarpPluginSharedInstances().sharedConfig());
        binder.bind(NativeConfig.class).toInstance(context.getWarpPluginSharedInstances().nativeConfig());
    }

    private void configureCommon(Binder binder)
    {
        binder.bind(EventBus.class).asEagerSingleton();

        binder.bind(FlowsSequencer.class);
        binder.bind(MetricsManager.class);
        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
        binder.bind(StorageEngineTxService.class);
        binder.bind(WarmupRuleService.class);
        binder.bind(WarpSessionProperties.class);

        binder.bind(ExceptionThrower.class).toInstance(context.getWarpPluginSharedInstances().exceptionThrower());
        binder.bind(NativeStorageStateHandler.class);
        binder.bind(RangeFillerService.class).toInstance(context.getWarpPluginSharedInstances().rangeFillerService());
        binder.bind(StorageEngine.class).toInstance(context.getWarpPluginSharedInstances().storageEngine());
        binder.bind(StorageEngineConstants.class).toInstance(context.getWarpPluginSharedInstances().storageEngineConstants());
    }

    private void configureCoordinator(Binder binder)
    {
        binder.bind(CoordinatorNodeManager.class);
        binder.bind(CoordinatorInitializedEventHandler.class);
    }

    private void configureWorker(Binder binder)
    {
        binder.bind(BufferAllocator.class);

        binder.bind(WorkerNodeManager.class);
        binder.bind(WorkerCapacityManager.class);
        binder.bind(WorkerMemoryManager.class);
    }

    private void bindMetricsServices(Binder binder)
    {
        binder.bind(ScheduledMetricsHandler.class).toInstance(context.getWarpPluginSharedInstances().scheduledMetricsHandler());
        binder.bind(PrintMetricsTimerTask.class);
    }

    @SuppressWarnings("unused")
    @Provides
    @Singleton
    public CatalogNameProvider provideCatalogName()
    {
        return new CatalogNameProvider(catalogName + "_" + context.getCatalogVersion());
    }

    @Provides
    @Singleton
    public WarmupRuleProvider provideWarmupRuleProvider(WarmupRuleService warmupRuleService)
    {
        return new WarmupRuleProvider(Optional.of(warmupRuleService));
    }

    @Provides
    @Singleton
    public ObjectMapperProvider provideObjectMapperProvider(TypeManager typeManager)
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager)));
        provider.withKeyDeserializers(ImmutableMap.of(
                WarpColumn.class, new WarpColumnJsonKeyDeserializer()));
        return provider;
    }
}
