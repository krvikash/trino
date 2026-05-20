/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonBinder;
import io.airlift.tracing.SpanSerialization;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.disk.LocalDiskAllocator;
import io.starburst.stargate.buffer.data.disk.LocalDiskTier;
import io.starburst.stargate.buffer.data.disk.LocalDiskTierConfig;
import io.starburst.stargate.buffer.data.disk.LocalDiskTierFeatureConfig;
import io.starburst.stargate.buffer.data.execution.ChunkDataFactory;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManager.ForChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.memory.FullHeapMemoryConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.memory.MemoryConfig;
import io.starburst.stargate.buffer.data.memory.StaticMemoryConfig;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorageDriver;
import io.starburst.stargate.buffer.status.StatusProvider;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.server.HttpServerConfig.ProcessForwardedMode.ACCEPT;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.lang.Runtime.getRuntime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DataServerMainModule
        extends AbstractConfigurationAwareModule
{
    private final long bufferNodeId;
    private final boolean discoveryBroadcastEnabled;
    private final Ticker ticker;
    private final Optional<String> configPrefix;
    private final boolean useStaticMemoryConfig;

    private DataServerMainModule(long bufferNodeId, boolean discoveryBroadcastEnabled, Ticker ticker, boolean useStaticMemoryConfig, Optional<String> configPrefix)
    {
        this.bufferNodeId = bufferNodeId;
        this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.configPrefix = requireNonNull(configPrefix, "configPrefix is null");
        this.useStaticMemoryConfig = useStaticMemoryConfig;
    }

    @Override
    protected void setup(Binder binder)
    {
        JsonBinder.jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanSerialization.SpanDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(Span.class);

        configBinder(binder).bindConfig(ChunkManagerConfig.class, configPrefix.orElse(null));
        if (useStaticMemoryConfig) {
            configBinder(binder).bindConfig(StaticMemoryConfig.class, configPrefix.orElse(null));
            binder.bind(MemoryConfig.class).to(StaticMemoryConfig.class).in(SINGLETON);
        }
        else {
            configBinder(binder).bindConfig(FullHeapMemoryConfig.class, configPrefix.orElse(null));
            binder.bind(MemoryConfig.class).to(FullHeapMemoryConfig.class).in(SINGLETON);
        }
        configBinder(binder).bindConfig(MemoryAllocatorConfig.class, configPrefix.orElse(null));
        configBinder(binder).bindConfig(DataServerConfig.class, configPrefix.orElse(null));
        jaxrsBinder(binder).bind(DataResource.class);
        jaxrsBinder(binder).bind(LifecycleResource.class);

        jaxrsBinder(binder, VirtualThreadsDataServer.class).bind(BlockingDataResource.class);
        jaxrsBinder(binder, VirtualThreadsDataServer.class).bind(LifecycleResource.class);
        newOptionalBinder(binder, Key.get(HttpServerInfo.class, VirtualThreadsDataServer.class));

        binder.bind(MemoryAllocator.class).in(SINGLETON);
        binder.bind(BufferNodeId.class).toInstance(new BufferNodeId(bufferNodeId));
        binder.bind(BufferNodeInfoService.class).in(SINGLETON);
        binder.bind(Ticker.class).annotatedWith(ForChunkManager.class).toInstance(ticker);
        binder.bind(ChunkDataFactory.class).in(SINGLETON);
        binder.bind(ChunkManager.class).in(SINGLETON);
        binder.bind(MergedFileNameGenerator.class).in(SINGLETON);
        binder.bind(SpooledChunksByExchange.class).in(SINGLETON);
        binder.bind(DataServerStats.class).in(SINGLETON);
        newExporter(binder).export(DataServerStats.class).withGeneratedName();
        binder.bind(AddDataPagesThrottlingCalculator.class).in(SINGLETON);
        binder.bind(AddDataPagesInProgressTracker.class).in(SINGLETON);
        binder.bind(BufferNodeStateManager.class).in(SINGLETON);
        binder.bind(DataServerStatusProvider.class).in(SINGLETON);
        binder.bind(DrainService.class).in(SINGLETON);
        newSetBinder(binder, StatusProvider.class).addBinding().to(DataServerStatusProvider.class);
        binder.bind(ExecutorService.class).toInstance(newCachedThreadPool(daemonThreadsNamed("buffer-node-execution-%s")));
        binder.bind(ScheduledExecutorService.class).toInstance(newScheduledThreadPool(getRuntime().availableProcessors(), daemonThreadsNamed("buffer-node-execution-%s")));
        newOptionalBinder(binder, DiscoveryBroadcast.class);
        if (discoveryBroadcastEnabled) {
            binder.bind(DiscoveryBroadcast.class).in(SINGLETON);
        }

        configBinder(binder).bindConfig(LocalDiskTierFeatureConfig.class, configPrefix.orElse(null));
        newOptionalBinder(binder, LocalDiskTier.class);
        newOptionalBinder(binder, LocalDiskAllocator.class);
        LocalDiskTierFeatureConfig diskTierFeatureConfig = buildConfigObject(LocalDiskTierFeatureConfig.class, configPrefix.orElse(null));
        if (diskTierFeatureConfig.isEnabled()) {
            SpoolingDirectoryConfig spoolingDirectoryConfig = buildConfigObject(SpoolingDirectoryConfig.class, configPrefix.orElse(null));
            if (spoolingDirectoryConfig.getStorageDriver() != SpoolingStorageDriver.TRINO_FS) {
                binder.addError(
                        "Local disk tier requires spooling.storage-driver=TRINO_FS (got %s)",
                        spoolingDirectoryConfig.getStorageDriver());
                return;
            }
            configBinder(binder).bindConfig(LocalDiskTierConfig.class, configPrefix.orElse(null));
            binder.bind(LocalDiskAllocator.class).in(SINGLETON);
            binder.bind(LocalDiskTier.class).asEagerSingleton();
        }

        if (buildConfigObject(DataServerConfig.class, configPrefix.orElse(null)).isTestingEnableStatsLogging()) {
            binder.bind(DataServerStatsLogger.class).in(SINGLETON);
        }

        configBinder(binder).bindConfigDefaults(HttpServerConfig.class, config -> {
            config.setProcessForwarded(ACCEPT);
        });
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static BoundedExecutor createAsyncHttpResponseExecutor(DataServerConfig config)
    {
        return new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("async-http-response-%s")), config.getHttpResponseThreads());
    }

    public static DataServerMainModule.Builder builder()
    {
        return new DataServerMainModule.Builder();
    }

    public static class Builder
    {
        private OptionalLong bufferNodeId = OptionalLong.empty();
        private boolean discoveryBroadcastEnabled = true;
        private Ticker ticker = Ticker.systemTicker();
        private Optional<String> configPrefix = Optional.empty();
        private boolean useStaticMemoryConfig;

        @VisibleForTesting
        public Builder withDiscoveryBroadcast(boolean discoveryBroadcastEnabled)
        {
            this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
            return this;
        }

        @VisibleForTesting
        public Builder withBufferNodeId(long bufferNodeId)
        {
            this.bufferNodeId = OptionalLong.of(bufferNodeId);
            return this;
        }

        public Builder withTicker(Ticker ticker)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            return this;
        }

        public Builder withUseStaticMemoryConfig(boolean useStaticMemoryConfig)
        {
            this.useStaticMemoryConfig = useStaticMemoryConfig;
            return this;
        }

        public Builder withConfigPrefix(String configPrefix)
        {
            requireNonNull(configPrefix, "configPrefix is null");
            this.configPrefix = Optional.of(configPrefix);
            return this;
        }

        public DataServerMainModule build()
        {
            return new DataServerMainModule(
                    bufferNodeId.orElse(new SecureRandom().nextLong()),
                    discoveryBroadcastEnabled,
                    ticker,
                    useStaticMemoryConfig,
                    configPrefix);
        }
    }
}
