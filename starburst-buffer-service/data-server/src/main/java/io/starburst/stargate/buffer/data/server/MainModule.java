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

import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManager.ForChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;

import java.security.SecureRandom;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.Objects.requireNonNull;

public class MainModule
        implements Module
{
    private final long bufferNodeId;
    private final boolean discoveryBroadcastEnabled;
    private final Ticker ticker;

    public MainModule()
    {
        this(new SecureRandom().nextLong());
    }

    public MainModule(long bufferNodeId)
    {
        this(bufferNodeId, true, Ticker.systemTicker());
    }

    public MainModule(long bufferNodeId, boolean discoveryBroadcastEnabled, Ticker ticker)
    {
        this.bufferNodeId = bufferNodeId;
        this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("buffer-discovery.http", ForBufferDiscoveryClient.class);

        configBinder(binder).bindConfig(ChunkManagerConfig.class);
        configBinder(binder).bindConfig(MemoryAllocatorConfig.class);
        configBinder(binder).bindConfig(DataServerConfig.class);
        jaxrsBinder(binder).bind(DataResource.class);
        jaxrsBinder(binder).bind(ChunkDataResponseWriter.class);
        binder.bind(MemoryAllocator.class).in(SINGLETON);
        binder.bind(Long.class).annotatedWith(BufferNodeId.class).toInstance(bufferNodeId);
        binder.bind(Ticker.class).annotatedWith(ForChunkManager.class).toInstance(ticker);
        binder.bind(ChunkManager.class).in(SINGLETON);
        binder.bind(DataServerStats.class).in(SINGLETON);
        if (discoveryBroadcastEnabled) {
            binder.bind(DiscoveryBroadcast.class).in(SINGLETON);
        }
    }

    @Inject
    @Provides
    public DiscoveryApi getDiscoveryApi(DataServerConfig config, @ForBufferDiscoveryClient HttpClient httpClient)
    {
        return new HttpDiscoveryClient(config.getDiscoveryServiceUri(), httpClient);
    }
}
