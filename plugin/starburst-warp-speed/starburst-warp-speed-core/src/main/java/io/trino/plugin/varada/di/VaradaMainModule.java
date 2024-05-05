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
package io.trino.plugin.varada.di;

import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.json.ObjectMapperProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.hive.util.BlockJsonSerde;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.WorkerNodeManager;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.config.DictionaryConfig;
import io.trino.plugin.varada.config.GlobalConfig;
import io.trino.plugin.varada.config.MetricsConfig;
import io.trino.plugin.varada.config.NativeConfig;
import io.trino.plugin.varada.config.ProxiedConnectorConfig;
import io.trino.plugin.varada.config.WarmupDemoterConfig;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.metrics.MetricsTimerTask;
import io.trino.plugin.varada.metrics.PrintMetricsTimerTask;
import io.trino.plugin.varada.metrics.ScheduledMetricsHandler;
import io.trino.plugin.varada.node.CoordinatorInitializedEventHandler;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.varada.type.VaradaTypeDeserializer;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorContext;
import io.varada.cloudvendors.config.CloudVendorConfig;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

public class VaradaMainModule
        implements VaradaBaseModule
{
    private final ConnectorContext context;
    private final Map<String, String> config;

    VaradaMainModule(ConnectorContext context, Map<String, String> config)
    {
        this.context = requireNonNull(context);
        this.config = requireNonNull(config);
    }

    @Override
    public void configure(Binder binder)
    {
        if (VaradaBaseModule.isCoordinator(context)) {
            configureCoordinator(binder);
        }
        if (VaradaBaseModule.isWorker(context, config)) {
            configureWorker(binder);
        }
        configureCommon(binder);

        binder.bind(VaradaInitializedServiceRegistry.class);

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
        binder.bind(VaradaSessionProperties.class);
        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());

        binder.bind(MetricsManager.class);

        binder.bind(VaradaTypeDeserializer.class).asEagerSingleton();

        binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
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
}
