/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server.testing;

import com.google.common.base.Ticker;
import com.google.common.io.Closer;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.starburst.stargate.buffer.data.server.DataServerStatusProvider;
import io.starburst.stargate.buffer.data.server.DiscoveryApiModule;
import io.starburst.stargate.buffer.data.server.MainModule;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.status.StatusModule;
import io.starburst.stargate.buffer.status.StatusProvider;
import org.weakref.jmx.guice.MBeanModule;

import javax.ws.rs.core.UriBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.starburst.stargate.buffer.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.BufferNodeState.STARTED;
import static java.util.Objects.requireNonNull;

public class TestingDataServer
        implements Closeable
{
    private final URI baseUri;
    private final DataServerStatusProvider statusProvider;
    private final Closer closer = Closer.create();
    private final Optional<DiscoveryApi> discovery;

    private TestingDataServer(long nodeId, Optional<Module> discoveryApiModule, Map<String, String> configProperties)
    {
        Map<String, String> finalConfigProperties = new HashMap<>(configProperties);
        List<Module> modules = new ArrayList<>(Arrays.asList(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new TestingJmxModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new EventModule(),
                new StatusModule(),
                new MainModule(nodeId, discoveryApiModule.isPresent(), Ticker.systemTicker())));
        discoveryApiModule.ifPresent(modules::add);

        Bootstrap app = new Bootstrap(modules);

        Injector injector = app
                .quiet()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(finalConfigProperties)
                .initialize();

        BufferNodeStateManager stateManager = injector.getInstance(BufferNodeStateManager.class);
        stateManager.transitionState(STARTED);
        if (!discoveryApiModule.isPresent()) {
            stateManager.transitionState(ACTIVE);
        }
        this.statusProvider = injector.getInstance(DataServerStatusProvider.class);

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        closer.register(lifeCycleManager::stop);

        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        baseUri = UriBuilder.fromUri(httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri())
                .host("localhost")
                .build();
        if (discoveryApiModule.isPresent()) {
            discovery = Optional.of(injector.getInstance(DiscoveryApi.class));
        }
        else {
            discovery = Optional.empty();
        }
    }

    public StatusProvider getStatusProvider()
    {
        return this.statusProvider;
    }

    public Optional<DiscoveryApi> getDiscovery()
    {
        return discovery;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<Module> discoveryApiModule = Optional.empty();
        private Map<String, String> configProperties = new HashMap<>();

        public Builder withDefaultDiscoveryApiModule()
        {
            discoveryApiModule = Optional.of(new DiscoveryApiModule());
            return this;
        }

        public Builder withDiscoveryApiModule(Module module)
        {
            discoveryApiModule = Optional.of(requireNonNull(module, "module is not null"));
            return this;
        }

        public Builder setConfigProperties(Map<String, String> configProperties)
        {
            requireNonNull(configProperties, "configProperties is null");
            this.configProperties = new HashMap<>(configProperties);
            return this;
        }

        public Builder setConfigProperty(String key, String value)
        {
            this.configProperties.put(key, value);
            return this;
        }

        public TestingDataServer build()
        {
            return build(0);
        }

        public TestingDataServer build(int nodeId)
        {
            return new TestingDataServer(nodeId, discoveryApiModule, configProperties);
        }
    }

    public URI getBaseUri()
    {
        return baseUri;
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
