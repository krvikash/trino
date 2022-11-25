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
import io.starburst.stargate.buffer.data.server.MainModule;
import io.starburst.stargate.buffer.status.StatusModule;
import io.starburst.stargate.buffer.status.StatusProvider;
import org.weakref.jmx.guice.MBeanModule;

import javax.ws.rs.core.UriBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.ACTIVE;
import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.STARTED;
import static java.util.Objects.requireNonNull;

public class TestingDataServer
        implements Closeable
{
    private final URI baseUri;
    private final DataServerStatusProvider statusProvider;
    private final Closer closer = Closer.create();

    private TestingDataServer(long nodeId, boolean discoveryBroadcastEnabled, Map<String, String> configProperties)
    {
        Map<String, String> finalConfigProperties = new HashMap<>(configProperties);
        if (!discoveryBroadcastEnabled) {
            finalConfigProperties.put("discovery-service.uri", "http://dummy"); // this is still needed in config
        }

        Bootstrap app = new Bootstrap(
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
                new MainModule(nodeId, discoveryBroadcastEnabled, Ticker.systemTicker()));

        Injector injector = app
                .quiet()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(finalConfigProperties)
                .initialize();

        BufferNodeStateManager stateManager = injector.getInstance(BufferNodeStateManager.class);
        stateManager.transitionState(STARTED);
        if (!discoveryBroadcastEnabled) {
            stateManager.transitionState(ACTIVE);
        }
        this.statusProvider = injector.getInstance(DataServerStatusProvider.class);

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        closer.register(lifeCycleManager::stop);

        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        baseUri = UriBuilder.fromUri(httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri())
                .host("localhost")
                .build();
    }

    public StatusProvider getStatusProvider()
    {
        return this.statusProvider;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean discoveryBroadcastEnabled;
        private Map<String, String> configProperties = new HashMap<>();

        public Builder setDiscoveryBroadcastEnabled(boolean discoveryBroadcastEnabled)
        {
            this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
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
            return new TestingDataServer(nodeId, discoveryBroadcastEnabled, configProperties);
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
