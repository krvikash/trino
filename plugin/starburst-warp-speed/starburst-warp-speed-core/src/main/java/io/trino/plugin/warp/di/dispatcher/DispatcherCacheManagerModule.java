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

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.util.BlockJsonSerde;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.CloudVendorModule;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.di.ExtraModule;
import io.trino.plugin.warp.di.WarpBaseModule;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.dictionary.AttachDictionaryService;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandleBuilderProvider;
import io.trino.plugin.warp.dispatcher.ReadErrorHandler;
import io.trino.plugin.warp.dispatcher.WarpCacheMgrConnectorContext;
import io.trino.plugin.warp.dispatcher.cache.CacheMgrWarmupRuleService;
import io.trino.plugin.warp.dispatcher.cache.DispatcherCacheTransformer;
import io.trino.plugin.warp.dispatcher.cache.PredicateHashCalculator;
import io.trino.plugin.warp.dispatcher.cache.WarpCachePageSourceFactory;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.query.classifier.ClassifierFactory;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateContextFactory;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.DemoterSync;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupRuleProvider;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarpCacheManagerDeleteService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarpDeleteService;
import io.trino.plugin.warp.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.warp.dispatcher.warmup.warmers.EmptyRowGroupWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarpProxiedWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WeGroupWarmer;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.DomainToMapBlockConvertor;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsModule;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.metrics.ScheduledMetricsHandler;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterruptInterceptor;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.MatchService;
import io.trino.plugin.warp.storage.read.RangeFillerService;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.plugin.warp.storage.write.StorageWriterService;
import io.trino.plugin.warp.storage.write.WarmupElementStatsService;
import io.trino.plugin.warp.storage.write.WarpPageSinkFactory;
import io.trino.plugin.warp.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.warp.storage.write.dictionary.DictionaryWriterFactory;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.warp.util.json.SliceSerializer;
import io.trino.plugin.warp.util.json.WarpColumnJsonKeyDeserializer;
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
    private final WarpCacheMgrConnectorContext warpCacheMgrConnectorContext;
    private Map<String, String> config;

    public DispatcherCacheManagerModule(String cacheManagerName,
            Map<String, String> config,
            WarpCacheMgrConnectorContext warpCacheMgrConnectorContext)
    {
        this.cacheManagerName = cacheManagerName;
        this.warpCacheMgrConnectorContext = warpCacheMgrConnectorContext;
        withConfig(config);
    }

    @Override
    public void configure(Binder binder)
    {
        binder.install(new MetricsModule());
        binder.bind(MetricsManager.class);
        configBinder(binder).bindConfig(MetricsConfig.class);
        binder.bind(WarpInitializedServiceRegistry.class);

        binder.bind(NodeManager.class).toInstance(warpCacheMgrConnectorContext.getNodeManager());
        binder.bind(EventBus.class).asEagerSingleton();
        configBinder(binder).bindConfig(GlobalConfig.class);
        configBinder(binder).bindConfig(CacheManagerConfig.class);
        binder.bind(CacheMgrWarmupRuleService.class);

        if (!WarpBaseModule.isWorker(warpCacheMgrConnectorContext.getNodeManager(), config)) {
            return;
        }
        configBinder(binder).bindConfig(CloudVendorConfig.class, ForWarp.class);
        configBinder(binder).bindConfig(DictionaryConfig.class);
        configBinder(binder).bindConfig(WarmupDemoterConfig.class);

        binder.bind(SharedConfig.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().sharedConfig());
        binder.bind(NativeConfig.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().nativeConfig());

        // bind block serializers for the purpose of TupleDomain serde
        binder.bind(HiveBlockEncodingSerde.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

        binder.bind(ExceptionThrower.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().exceptionThrower());
        binder.bind(NativeStorageStateHandler.class);
        binder.bind(RangeFillerService.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().rangeFillerService());
        binder.bind(StorageEngine.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().storageEngine());
        binder.bind(StorageEngineConstants.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().storageEngineConstants());

        binder.install(CloudVendorModule.getModule(warpCacheMgrConnectorContext, ForWarp.class, cacheManagerName, config));

        binder.bind(ShapingLoggerFactory.class);
        binder.bind(AttachDictionaryService.class);
        binder.bind(BlockAppenderFactory.class);
        binder.bind(BlockFillersFactory.class);
        binder.bind(BlockTransformerFactory.class);
        binder.bind(BufferAllocator.class);
        binder.bind(ClassifierFactory.class);
        binder.bind(CollectTxService.class);
        binder.bind(DemoterSync.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().demoterSync());
        binder.bind(DictionaryCacheService.class);
        binder.bind(DictionaryWriterFactory.class);
        binder.bind(DispatcherProxiedConnectorTransformer.class).to(DispatcherCacheTransformer.class);
        binder.bind(DispatcherTableHandleBuilderProvider.class);
        binder.bind(DomainToMapBlockConvertor.class);
        binder.bind(EmptyRowGroupWarmer.class);
        binder.bind(FailureGeneratorInvocationHandler.class);
        binder.bind(FlowsSequencer.class);
        binder.bind(MatchCollectIdService.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().matchCollectIdService());
        binder.bind(MatchService.class);
        binder.bind(PredicateContextFactory.class);
        binder.bind(PredicateHashCalculator.class);
        binder.bind(PredicatesCacheService.class);
        binder.bind(QueryClassifier.class);
        binder.bind(ReadErrorHandler.class);
        binder.bind(RowGroupDataDao.class);
        binder.bind(RowGroupDataService.class);
        binder.bind(StorageCollectorService.class);
        binder.bind(StorageEngineTxService.class);
        binder.bind(StorageWarmerService.class);
        binder.bind(StorageWriterService.class);
        binder.bind(WarmupDemoterService.class);
        binder.bind(WarmupElementStatsService.class);
        binder.bind(WarmupElementsCreator.class);
        binder.bind(WarpCachePageSourceFactory.class);
        binder.bind(WarpDeleteService.class).to(WarpCacheManagerDeleteService.class);
        binder.bind(WarpPageSinkFactory.class);
        binder.bind(WarpProxiedWarmer.class);
        binder.bind(WeGroupWarmer.class);
        binder.bind(WorkerTaskExecutorService.class);

        bindMetricsServices(binder);
        binder.bindInterceptor(Matchers.any(), Matchers.annotatedWith(NativeInterrupt.class), new NativeInterruptInterceptor());
    }

    private void bindMetricsServices(Binder binder)
    {
        binder.bind(ScheduledMetricsHandler.class).toInstance(warpCacheMgrConnectorContext.getWarpPluginSharedInstances().scheduledMetricsHandler());
        binder.bind(PrintMetricsTimerTask.class);
    }

    @Override
    public DispatcherCacheManagerModule withConfig(Map<String, String> config)
    {
        this.config = config;
        return this;
    }

    @SuppressWarnings("unused")
    @Provides
    @Singleton
    public CatalogNameProvider provideCatalogName()
    {
        return new CatalogNameProvider(cacheManagerName);
    }

    @Provides
    @Singleton
    public WarmupRuleProvider provideWarmupRuleProvider()
    {
        return new WarmupRuleProvider(Optional.empty());
    }

    @Provides
    @Singleton
    public ObjectMapperProvider provideObjectMapperProvider()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer()));
        provider.withKeyDeserializers(ImmutableMap.of(
                WarpColumn.class, new WarpColumnJsonKeyDeserializer()));
        return provider;
    }
}
