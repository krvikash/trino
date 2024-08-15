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

import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.json.ObjectMapperProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.hive.util.BlockJsonSerde;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.plugin.warp.CoordinatorNodeManager;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.WorkerNodeManager;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsTimerTask;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.metrics.ScheduledMetricsHandler;
import io.trino.plugin.warp.node.CoordinatorInitializedEventHandler;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

public class WarpMainModule
        implements WarpBaseModule
{
    private final ConnectorContext context;
    private final Map<String, String> config;

    WarpMainModule(ConnectorContext context, Map<String, String> config)
    {
        this.context = requireNonNull(context);
        this.config = requireNonNull(config);
    }

    @Override
    public void configure(Binder binder)
    {
        if (WarpBaseModule.isCoordinator(context)) {
            configureCoordinator(binder);
        }
        if (WarpBaseModule.isWorker(context, config)) {
            configureWorker(binder);
        }
        configureCommon(binder);

        binder.bind(WarpInitializedServiceRegistry.class);

        bindMetricsServices(binder);
        bindConfigs(binder);
    }

    private void bindConfigs(Binder binder)
    {
        configBinder(binder).bindConfig(MetricsConfig.class);
        configBinder(binder).bindConfig(GlobalConfig.class);
        configBinder(binder).bindConfig(NativeConfig.class);
        configBinder(binder).bindConfig(WarmupDemoterConfig.class);
        configBinder(binder).bindConfig(ProxiedConnectorConfig.class);
        configBinder(binder).bindConfig(DictionaryConfig.class);
        configBinder(binder).bindConfig(CloudVendorConfig.class, ForWarp.class);
    }

    private void configureCommon(Binder binder)
    {
        binder.bind(StorageEngineTxService.class);
        binder.bind(EventBus.class).asEagerSingleton();
        // bind block serializers for the purpose of TupleDomain serde
        binder.bind(HiveBlockEncodingSerde.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
        binder.bind(ObjectMapperProvider.class);
        binder.bind(WarpSessionProperties.class);
        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());

        binder.bind(MetricsManager.class);

        binder.bind(FlowsSequencer.class);

        binder.bind(WarmupRuleService.class);
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
    }

    private void bindMetricsServices(Binder binder)
    {
        binder.bind(ScheduledMetricsHandler.class).asEagerSingleton();
        Multibinder<MetricsTimerTask> multibinder = Multibinder.newSetBinder(binder, MetricsTimerTask.class);
        multibinder.addBinding().to(PrintMetricsTimerTask.class);
        binder.bind(PrintMetricsTimerTask.class);
    }

    @SuppressWarnings("unused")
    @Provides
    @Singleton
    public CatalogNameProvider provideCatalogName()
    {
        return new CatalogNameProvider(context.getCatalogHandle().getCatalogName() + "_" + context.getCatalogHandle().getVersion());
    }
}
