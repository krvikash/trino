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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonBinder;
import io.airlift.node.NodeInfo;
import io.airlift.tracing.SpanSerialization;
import io.airlift.units.DataSize;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.client.ForBufferDataClient;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.ForBufferDiscoveryClient;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManagerFactory.InternalCommunicationDependencies;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.CoordinatorLocator;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.DataApiBinder.dataApiBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class BufferExchangeModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<ApiFactory> apiFactory;
    private final Optional<InternalCommunicationDependencies> internalCommunicationDependencies;

    public BufferExchangeModule(Optional<ApiFactory> apiFactory, Optional<InternalCommunicationDependencies> internalCommunicationDependencies)
    {
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
        this.internalCommunicationDependencies = requireNonNull(internalCommunicationDependencies, "internalCommunicationDependencies is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(BufferExchangeConfig.class);

        JsonBinder.jsonBinder(binder).addSerializerBinding(Span.class).to(SpanSerialization.SpanSerializer.class);
        jsonCodecBinder(binder).bindJsonCodec(Span.class);

        binder.bind(BufferNodeDiscoveryManager.class).to(ApiBasedBufferNodeDiscoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferCoordinatorExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferWorkerExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(ListeningScheduledExecutorService.class).toInstance(MoreExecutors.listeningDecorator(newScheduledThreadPool(8, daemonThreadsNamed("buffer-exchange-scheduled-%s")))); // todo - configurable?
        binder.bind(ScheduledExecutorService.class).to(ListeningScheduledExecutorService.class);
        binder.bind(ExecutorService.class).toInstance(newCachedThreadPool(daemonThreadsNamed("buffer-exchange-%s"))); // todo - make thread count bounded?
        binder.bind(DataApiFacade.class).in(Scopes.SINGLETON);
        binder.bind(DataApiFacadeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DataApiFacadeStats.class).withGeneratedName();
        binder.bind(PartitionNodeMapperFactory.class).to(switch (buildConfigObject(BufferExchangeConfig.class).getPartitionNodeMappingMode()) {
            case PINNING_SINGLE -> PinningPartitionNodeMapperFactory.class;
            case PINNING_MULTI -> SmartPinningPartitionNodeMapperFactory.class;
            case RANDOM -> RandomPartitionNodeMapperFactory.class;
            case LOCAL_PRIORITY -> LocalPriorityPartitionNodeMapperFactory.class;
        }).in(Scopes.SINGLETON);

        BufferExchangeConfig bufferExchangeConfig = buildConfigObject(BufferExchangeConfig.class);
        if (bufferExchangeConfig.isUseEmbeddedBufferService()) {
            verify(internalCommunicationDependencies.isPresent(), "internalCommunicationDependencies must not be empty if embedded buffer service is in use");
            internalCommunicationDependencies.ifPresent(internalCommunicationDependencies -> {
                // internalCommunicationDependencies.getInternalCommunicationConfig() is exposed as separate properties as
                // we need to access those during bootstrapping.
                binder.bind(SecurityConfig.class).toInstance(internalCommunicationDependencies.getSecurityConfig());
                binder.bind(NodeInfo.class).toInstance(internalCommunicationDependencies.getNodeInfo());
            });
            install(new RealBufferingServiceApiFactoryModule(true));
        }
        else {
            verify(internalCommunicationDependencies.isEmpty(), "internalCommunicationDependencies must be empty if embedded buffer service is not in use");
            if (apiFactory.isEmpty()) {
                install(new RealBufferingServiceApiFactoryModule(false));
            }
            else {
                binder.bind(ApiFactory.class).toInstance(apiFactory.get());
            }
        }
    }

    private static class RealBufferingServiceApiFactoryModule
            extends AbstractConfigurationAwareModule
    {
        private final boolean useInternalCommunication;

        public RealBufferingServiceApiFactoryModule(boolean useInternalCommunication)
        {
            this.useInternalCommunication = useInternalCommunication;
        }

        @Override
        protected void setup(Binder binder)
        {
            // discovery http client
            if (useInternalCommunication) {
                install(internalHttpClientModule("exchange.buffer-discovery", ForBufferDiscoveryClient.class).build());
            }
            else {
                httpClientBinder(binder).bindHttpClient("exchange.buffer-discovery", ForBufferDiscoveryClient.class);
            }

            // data http client
            if (useInternalCommunication) {
                install(internalHttpClientModule("exchange.buffer-data", ForBufferDataClient.class)
                        .withConfigDefaults(config -> config
                                .setMaxResponseContentLength(DataSize.of(64, MEGABYTE)) // should equal to chunk.max-size
                                .setIdleTimeout(succinctDuration(30, SECONDS)))
                        .build());
            }
            else {
                httpClientBinder(binder).bindHttpClient("exchange.buffer-data", ForBufferDataClient.class)
                        .withConfigDefaults(config -> config
                                .setMaxResponseContentLength(DataSize.of(64, MEGABYTE)) // should equal to chunk.max-size
                                .setIdleTimeout(succinctDuration(30, SECONDS)));
            }

            dataApiBinder(binder, super::install).bindHttpDataApi("exchange.buffer-data");
            binder.bind(ApiFactory.class).to(RealBufferingServiceApiFactory.class);
        }

        @Provides
        public DiscoveryApi getDiscoveryApi(BufferExchangeConfig config, CoordinatorLocator coordinatorLocator, @ForBufferDiscoveryClient HttpClient httpClient)
        {
            requireNonNull(config, "config is null");
            requireNonNull(httpClient, "httpClient is null");

            Supplier<URI> uriSupplier;
            if (config.isUseEmbeddedBufferService()) {
                uriSupplier = () -> {
                    Set<URI> coordinatorUris = coordinatorLocator.getCoordinatorUris();
                    if (coordinatorUris.isEmpty()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "No coordinator nodes available");
                    }
                    if (coordinatorUris.size() > 1) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Multiple coordinator nodes available");
                    }
                    return Iterables.getOnlyElement(coordinatorUris);
                };
            }
            else {
                uriSupplier = config::getDiscoveryServiceUri;
            }
            return new HttpDiscoveryClient(uriSupplier, httpClient);
        }
    }
}
