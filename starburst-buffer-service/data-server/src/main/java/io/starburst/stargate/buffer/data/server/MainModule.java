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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;

import java.util.concurrent.ThreadLocalRandom;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class MainModule
        implements Module
{
    private final long bufferNodeId;
    private final boolean discoveryBroadcastEnabled;

    public MainModule()
    {
        this(ThreadLocalRandom.current().nextLong());
    }

    public MainModule(long bufferNodeId)
    {
        this(bufferNodeId, true);
    }

    public MainModule(long bufferNodeId, boolean discoveryBroadcastEnabled)
    {
        this.bufferNodeId = bufferNodeId;
        this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
    }

    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("buffer-discovery.http", ForBufferDiscoveryClient.class);

        configBinder(binder).bindConfig(ChunkManagerConfig.class);
        configBinder(binder).bindConfig(MemoryAllocatorConfig.class);
        configBinder(binder).bindConfig(DataServerConfig.class);
        jaxrsBinder(binder).bind(DataResource.class);
        jaxrsBinder(binder).bind(PagesResponseWriter.class);
        binder.bind(MemoryAllocator.class).in(SINGLETON);
        binder.bind(Long.class).annotatedWith(BufferNodeId.class).toInstance(bufferNodeId);
        binder.bind(ChunkManager.class).in(SINGLETON);
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
