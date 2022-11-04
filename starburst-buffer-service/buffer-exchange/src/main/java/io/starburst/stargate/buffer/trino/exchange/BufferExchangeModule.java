/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class BufferExchangeModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<ApiFactory> apiFactory;

    public BufferExchangeModule(Optional<ApiFactory> apiFactory)
    {
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(BufferExchangeConfig.class);

        binder.bind(BufferNodeDiscoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferCoordinatorExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferWorkerExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduledExecutorService.class).toInstance(Executors.newScheduledThreadPool(8)); // todo - configurable?
        binder.bind(ExecutorService.class).toInstance(newCachedThreadPool(daemonThreadsNamed("buffer-exchange-%s"))); // todo - make thread count bounded?
        binder.bind(DataApiFacade.class).in(Scopes.SINGLETON);

        bindPartitionNodeMapper(PartitionNodeMappingMode.PINNING, PinningPartitionNodeMapperFactory.class);
        bindPartitionNodeMapper(PartitionNodeMappingMode.RANDOM, RandomPartitionNodeMapperFactory.class);

        if (apiFactory.isEmpty()) {
            install(new RealBufferingServiceApiFactoryModule());
        }
        else {
            binder.bind(ApiFactory.class).toInstance(apiFactory.get());
        }
    }

    private void bindPartitionNodeMapper(PartitionNodeMappingMode mode, Class<? extends PartitionNodeMapperFactory> implementation)
    {
        super.install(conditionalModule(
                BufferExchangeConfig.class,
                bufferExchangeConfig -> bufferExchangeConfig.getPartitionNodeMappingMode() == mode,
                localBinder -> localBinder.bind(PartitionNodeMapperFactory.class).to(implementation).in(Scopes.SINGLETON)));
    }

    private static class RealBufferingServiceApiFactoryModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            httpClientBinder(binder).bindHttpClient("exchange.buffer-discovery.http", ForBufferDiscoveryClient.class);
            httpClientBinder(binder)
                    .bindHttpClient("exchange.buffer-data.http", ForBufferDataClient.class)
                    .withConfigDefaults(config -> config.setMaxContentLength(DataSize.of(32, MEGABYTE)));
            binder.bind(ApiFactory.class).to(RealBufferingServiceApiFactory.class);
        }

        @Provides
        public DiscoveryApi getDiscoveryApi(BufferExchangeConfig config, @ForBufferDiscoveryClient HttpClient httpClient)
        {
            requireNonNull(config, "config is null");
            requireNonNull(httpClient, "httpClient is null");
            return new HttpDiscoveryClient(config.getDiscoveryServiceUri(), httpClient);
        }
    }
}
