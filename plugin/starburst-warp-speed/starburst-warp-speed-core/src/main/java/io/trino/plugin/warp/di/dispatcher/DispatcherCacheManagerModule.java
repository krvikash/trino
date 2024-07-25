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
package io.trino.plugin.warp.di.dispatcher;

import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.hive.util.BlockJsonSerde;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.plugin.warp.WorkerNodeManager;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.CloudVendorModule;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.di.EmptyConnectorContext;
import io.trino.plugin.warp.di.ExtraModule;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.di.WarpNativeStorageEngineModule;
import io.trino.plugin.warp.dictionary.AttachDictionaryService;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.ReadErrorHandler;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.query.classifier.ClassifierFactory;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateContextFactory;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmExecutionTaskFactory;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.warp.dispatcher.warmup.warmers.EmptyRowGroupWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarpProxiedWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WeGroupWarmer;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.DomainToMapBlockConvertor;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsModule;
import io.trino.plugin.warp.metrics.MetricsTimerTask;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.metrics.ScheduledMetricsHandler;
import io.trino.plugin.warp.proxiedconnector.EmptyProxiedConnectorTransformer;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.storage.read.ChunksQueueService;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.LazyCollectTxService;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.plugin.warp.storage.write.StorageWriterService;
import io.trino.plugin.warp.storage.write.WarmupElementStatsService;
import io.trino.plugin.warp.storage.write.WarpPageSinkFactory;
import io.trino.plugin.warp.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.warp.storage.write.dictionary.DictionaryWriterFactory;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;
import io.trino.spi.NodeManager;
import io.trino.spi.block.Block;

import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;

/**
 * this module will install dependencies which are required in the cache manager
 */
public class DispatcherCacheManagerModule
        implements ExtraModule
{
    private final String cacheManagerName;
    private final boolean isCoordinator;
    private Map<String, String> config;
    private final Optional<Module> storageEngineModule;
    private final Optional<Module> cloudVendorModule;

    public DispatcherCacheManagerModule(String cacheManagerName,
            Map<String, String> config,
            Module storageEngineModule,
            Module cloudVendorModule,
            boolean isCoordinator)
    {
        this.cacheManagerName = cacheManagerName;
        this.isCoordinator = isCoordinator;
        withConfig(config);
        this.storageEngineModule = Optional.ofNullable(storageEngineModule);
        this.cloudVendorModule = Optional.ofNullable(cloudVendorModule);
    }

    @Override
    public void configure(Binder binder)
    {
        binder.install(new MetricsModule(cacheManagerName));
        binder.bind(MetricsManager.class);
        configBinder(binder).bindConfig(MetricsConfig.class);
        binder.bind(WarpInitializedServiceRegistry.class);
        if (isCoordinator) {
            return;
        }
        configBinder(binder).bindConfig(CloudVendorConfig.class, ForWarp.class);
        configBinder(binder).bindConfig(DictionaryConfig.class);
        configBinder(binder).bindConfig(GlobalConfig.class);
        configBinder(binder).bindConfig(NativeConfig.class);
        configBinder(binder).bindConfig(WarmupDemoterConfig.class);

        EmptyConnectorContext context = new EmptyConnectorContext();

        binder.bind(NodeManager.class).toInstance(context.getNodeManager());

        binder.bind(EventBus.class).asEagerSingleton();
        // bind block serializers for the purpose of TupleDomain serde
        binder.bind(HiveBlockEncodingSerde.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
        binder.bind(ObjectMapperProvider.class);
        binder.install(storageEngineModule.orElseGet(() -> new WarpNativeStorageEngineModule(context, config)));
        binder.install(cloudVendorModule.orElse(CloudVendorModule.getModule(context, ForWarp.class, cacheManagerName, config)));

        binder.bind(AttachDictionaryService.class);
        binder.bind(BlockAppenderFactory.class);
        binder.bind(BlockFillersFactory.class);
        binder.bind(BlockTransformerFactory.class);
        binder.bind(BufferAllocator.class);
        binder.bind(ChunksQueueService.class);
        binder.bind(ClassifierFactory.class);
        binder.bind(CollectTxService.class);
        binder.bind(LazyCollectTxService.class);
        binder.bind(DictionaryCacheService.class);
        binder.bind(DictionaryWriterFactory.class);
        binder.bind(DispatcherPageSourceFactory.class);
        binder.bind(DispatcherProxiedConnectorTransformer.class).to(EmptyProxiedConnectorTransformer.class);
        binder.bind(DomainToMapBlockConvertor.class);
        binder.bind(EmptyRowGroupWarmer.class);
        binder.bind(FailureGeneratorInvocationHandler.class);
        binder.bind(FlowsSequencer.class);
        binder.bind(MatchCollectIdService.class);
        binder.bind(PredicateContextFactory.class);
        binder.bind(PredicatesCacheService.class);
        binder.bind(QueryClassifier.class);
        binder.bind(ReadErrorHandler.class);
        binder.bind(RowGroupDataDao.class);
        binder.bind(RowGroupDataService.class);
        binder.bind(StorageCollectorService.class);
        binder.bind(StorageEngineTxService.class);
        binder.bind(StorageWarmerService.class);
        binder.bind(StorageWriterService.class);
        binder.bind(WarmExecutionTaskFactory.class);
        binder.bind(WarmingManager.class);
        binder.bind(WarmupDemoterService.class);
        binder.bind(WarmupElementStatsService.class);
        binder.bind(WarmupElementsCreator.class);
        binder.bind(WarpPageSinkFactory.class);
        binder.bind(WarpProxiedWarmer.class);
        binder.bind(WeGroupWarmer.class);
        binder.bind(WorkerCapacityManager.class);
        binder.bind(WorkerNodeManager.class);
        binder.bind(WorkerTaskExecutorService.class);
        binder.bind(WorkerWarmingService.class);
        binder.bind(WorkerWarmupRuleService.class);

        bindMetricsServices(binder);
    }

    private void bindMetricsServices(Binder binder)
    {
        binder.bind(ScheduledMetricsHandler.class).asEagerSingleton();
        Multibinder<MetricsTimerTask> multibinder = Multibinder.newSetBinder(binder, MetricsTimerTask.class);
        multibinder.addBinding().to(PrintMetricsTimerTask.class);
        binder.bind(PrintMetricsTimerTask.class);
    }

    @Override
    public DispatcherCacheManagerModule withConfig(Map<String, String> config)
    {
        this.config = config;
        return this;
    }
}
